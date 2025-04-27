/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.exchange.filesystem.oss;

import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClientBuilder;
import com.aliyun.oss.model.ListObjectsRequest;
import com.aliyun.oss.model.OSSObject;
import com.aliyun.oss.model.ObjectListing;
import com.aliyun.oss.model.PutObjectRequest;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.errorprone.annotations.ThreadSafe;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.google.inject.Inject;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.slice.Slices;
import io.trino.annotation.NotThreadSafe;
import io.trino.plugin.exchange.filesystem.ExchangeSourceFile;
import io.trino.plugin.exchange.filesystem.ExchangeStorageReader;
import io.trino.plugin.exchange.filesystem.ExchangeStorageWriter;
import io.trino.plugin.exchange.filesystem.FileStatus;
import io.trino.plugin.exchange.filesystem.FileSystemExchangeStorage;
import io.trino.plugin.exchange.filesystem.MetricsBuilder;
import io.trino.plugin.exchange.filesystem.MetricsBuilder.CounterMetricBuilder;
import jakarta.annotation.PreDestroy;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.toListenableFuture;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.trino.plugin.exchange.filesystem.FileSystemExchangeFutures.translateFailures;
import static io.trino.plugin.exchange.filesystem.FileSystemExchangeManager.PATH_SEPARATOR;
import static io.trino.plugin.exchange.filesystem.MetricsBuilder.SOURCE_FILES_PROCESSED;
import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;

public class OssFileSystemExchangeStorage
        implements FileSystemExchangeStorage
{
    private static final int BLOCK_SIZE = 4 * 1024 * 1024; // 4MB
    private final OSS ossClient;
    private final String bucketName;

    @Inject
    public OssFileSystemExchangeStorage(ExchangeOssConfig config)
    {
        this.ossClient = new OSSClientBuilder().build(
                config.getEndpoint(),
                config.getAccessKey(),
                config.getAccessSecretKey());
        this.bucketName = config.getBucketName();
    }

    private static String getPath(URI uri)
    {
        String path = uri.getPath();
        if (path.startsWith(PATH_SEPARATOR)) {
            path = path.substring(1);
        }
        return path;
    }

    @Override
    public void createDirectories(URI dir)
            throws IOException
    {
        // OSS doesn't require explicit directory creation
    }

    @Override
    public ExchangeStorageReader createExchangeStorageReader(List<ExchangeSourceFile> sourceFiles, int maxPageStorageSize, MetricsBuilder metricsBuilder)
    {
        return new OssExchangeStorageReader(ossClient, bucketName, sourceFiles, metricsBuilder, BLOCK_SIZE, maxPageStorageSize);
    }

    @Override
    public ExchangeStorageWriter createExchangeStorageWriter(URI file)
    {
        String key = getPath(file);
        return new OssExchangeStorageWriter(ossClient, bucketName, key, BLOCK_SIZE);
    }

    @Override
    public ListenableFuture<Void> createEmptyFile(URI file)
    {
        String key = getPath(file);
        return translateFailures(toListenableFuture(CompletableFuture.runAsync(() -> {
            InputStream in = new ByteArrayInputStream(new byte[0], 0, 0);
            PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName, key, in);
            ossClient.putObject(putObjectRequest);
        }, directExecutor())));
    }

    @Override
    public ListenableFuture<Void> deleteRecursively(List<URI> directories)
    {
        return translateFailures(toListenableFuture(CompletableFuture.runAsync(() -> {
            for (URI directory : directories) {
                String prefix = getPath(directory);
                ossClient.listObjects(bucketName, prefix).getObjectSummaries()
                        .forEach(object -> ossClient.deleteObject(bucketName, object.getKey()));
            }
        }, directExecutor())));
    }

    @Override
    public ListenableFuture<List<FileStatus>> listFilesRecursively(URI dir)
    {
        String prefix = getPath(dir);
        ImmutableList.Builder<FileStatus> fileStatuses = ImmutableList.builder();

        String nextMarker = null;
        boolean isTruncated;

        do {
            ListObjectsRequest request = new ListObjectsRequest(bucketName)
                    .withPrefix(prefix)
                    .withMarker(nextMarker)
                    .withMaxKeys(1000); // Default is 100, 1000 is max

            ObjectListing listing = ossClient.listObjects(request);

            listing.getObjectSummaries().forEach(object -> {
                URI fileUri = URI.create("oss://" + bucketName + "/" + object.getKey());
                fileStatuses.add(new FileStatus(fileUri.getPath(), object.getSize()));
            });

            nextMarker = listing.getNextMarker();
            isTruncated = listing.isTruncated();
        }
        while (isTruncated);
        return immediateFuture(fileStatuses.build());
    }

    @Override
    public int getWriteBufferSize()
    {
        return BLOCK_SIZE;
    }

    @PreDestroy
    @Override
    public void close()
            throws IOException
    {
        ossClient.shutdown();
    }

    @ThreadSafe
    private static class OssExchangeStorageReader
            implements ExchangeStorageReader
    {
        private static final int INSTANCE_SIZE = instanceSize(OssExchangeStorageReader.class);

        private final OSS ossClient;
        private final String bucketName;
        @GuardedBy("this")
        private final Queue<ExchangeSourceFile> sourceFiles;
        private final int blockSize;
        private final int bufferSize;
        private final CounterMetricBuilder sourceFilesProcessedMetric;
        @GuardedBy("this")
        private final int sliceSize = -1;
        @GuardedBy("this")
        private ExchangeSourceFile currentFile;
        @GuardedBy("this")
        private long fileOffset;
        @GuardedBy("this")
        private SliceInput sliceInput;
        private volatile boolean closed;
        private volatile long bufferRetainedSize;
        private volatile ListenableFuture<Void> inProgressReadFuture = immediateVoidFuture();

        public OssExchangeStorageReader(
                OSS ossClient,
                String bucketName,
                List<ExchangeSourceFile> sourceFiles,
                MetricsBuilder metricsBuilder,
                int blockSize,
                int maxPageStorageSize)
        {
            this.ossClient = requireNonNull(ossClient, "ossClient is null");
            this.bucketName = requireNonNull(bucketName, "bucketName is null");
            this.sourceFiles = new ArrayDeque<>(requireNonNull(sourceFiles, "sourceFiles is null"));
            this.blockSize = blockSize;
            this.bufferSize = min(blockSize, maxPageStorageSize);
            this.sourceFilesProcessedMetric = metricsBuilder.getCounterMetric(SOURCE_FILES_PROCESSED);
        }

        @Override
        public synchronized Slice read()
                throws IOException
        {
            if (closed) {
                return null;
            }

            if (sliceInput == null || !sliceInput.isReadable()) {
                if (currentFile == null) {
                    currentFile = sourceFiles.poll();
                    if (currentFile == null) {
                        return null;
                    }
                    sourceFilesProcessedMetric.increment();
                }

                fillBuffer();
                if (sliceInput == null || !sliceInput.isReadable()) {
                    currentFile = null;
                    return read();
                }
            }

            int size = min(sliceInput.available(), bufferSize);
            Slice slice = Slices.allocate(size);
            sliceInput.readBytes(slice, 0, size);
            return slice;
        }

        @Override
        public ListenableFuture<Void> isBlocked()
        {
            return inProgressReadFuture;
        }

        @Override
        public synchronized long getRetainedSize()
        {
            return INSTANCE_SIZE + bufferRetainedSize;
        }

        @Override
        public boolean isFinished()
        {
            return closed || (currentFile == null && sourceFiles.isEmpty());
        }

        @Override
        public synchronized void close()
        {
            if (closed) {
                return;
            }
            closed = true;
            inProgressReadFuture = immediateVoidFuture();
            sliceInput = null;
            bufferRetainedSize = 0;
        }

        @GuardedBy("this")
        private void fillBuffer()
        {
            if (inProgressReadFuture.isDone()) {
                String key = getPath(currentFile.getFileUri());
                inProgressReadFuture = translateFailures(toListenableFuture(CompletableFuture.supplyAsync(() -> {
                    OSSObject object = ossClient.getObject(bucketName, key);
                    try (InputStream inputStream = object.getObjectContent()) {
                        byte[] buffer = new byte[blockSize];
                        int bytesRead = inputStream.read(buffer);
                        if (bytesRead == -1) {
                            return null;
                        }
                        return Slices.wrappedBuffer(buffer, 0, bytesRead);
                    }
                    catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }, directExecutor()).thenAccept(slice -> {
                    synchronized (OssExchangeStorageReader.this) {
                        if (slice != null) {
                            sliceInput = slice.getInput();
                            bufferRetainedSize = slice.getRetainedSize();
                        }
                    }
                })));
            }
        }
    }

    @NotThreadSafe
    private static class OssExchangeStorageWriter
            implements ExchangeStorageWriter
    {
        private static final int INSTANCE_SIZE = instanceSize(OssExchangeStorageWriter.class);

        private final OSS ossClient;
        private final String bucketName;
        private final String key;
        private final int blockSize;
        private final List<ListenableFuture<Void>> uploadFutures = new ArrayList<>();
        private volatile boolean closed;

        public OssExchangeStorageWriter(
                OSS ossClient,
                String bucketName,
                String key,
                int blockSize)
        {
            this.ossClient = requireNonNull(ossClient, "ossClient is null");
            this.bucketName = requireNonNull(bucketName, "bucketName is null");
            this.key = requireNonNull(key, "key is null");
            this.blockSize = blockSize;
        }

        @Override
        public ListenableFuture<Void> write(Slice slice)
        {
            checkState(!closed, "already closed");
            byte[] data = new byte[slice.length()];
            slice.getBytes(0, data);
            return translateFailures(toListenableFuture(CompletableFuture.runAsync(() -> {
                InputStream in = new ByteArrayInputStream(data, 0, data.length);
                PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName, key, in);
                ossClient.putObject(putObjectRequest);
            }, directExecutor())));
        }

        @Override
        public ListenableFuture<Void> finish()
        {
            checkState(!closed, "already closed");
            closed = true;
            return translateFailures(Futures.allAsList(uploadFutures));
        }

        @Override
        public ListenableFuture<Void> abort()
        {
            checkState(!closed, "already closed");
            closed = true;
            return immediateVoidFuture();
        }

        @Override
        public long getRetainedSize()
        {
            return INSTANCE_SIZE;
        }
    }
}
