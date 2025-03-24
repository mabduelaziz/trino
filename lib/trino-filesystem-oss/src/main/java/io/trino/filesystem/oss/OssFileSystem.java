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
package io.trino.filesystem.oss;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobListOption;
import com.google.cloud.storage.StorageBatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import io.airlift.units.Duration;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemException;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoOutputFile;
import io.trino.filesystem.UriLocation;
import io.trino.filesystem.encryption.EncryptionKey;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

import static com.google.cloud.storage.Storage.BlobListOption.currentDirectory;
import static com.google.cloud.storage.Storage.BlobListOption.matchGlob;
import static com.google.cloud.storage.Storage.BlobListOption.pageSize;
import static com.google.cloud.storage.Storage.SignUrlOption.withExtHeaders;
import static com.google.cloud.storage.Storage.SignUrlOption.withV4Signature;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.Iterables.partition;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.trino.filesystem.oss.OssUtils.encodedKey;
import static io.trino.filesystem.oss.OssUtils.getBlob;
import static io.trino.filesystem.oss.OssUtils.handleOssException;
import static io.trino.filesystem.oss.OssUtils.keySha256Checksum;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class OssFileSystem
        implements TrinoFileSystem
{
    private final ListeningExecutorService executorService;
    private final Storage storage;
    private final int readBlockSizeBytes;
    private final long writeBlockSizeBytes;
    private final int pageSize;
    private final int batchSize;

    public OssFileSystem(ListeningExecutorService executorService, Storage storage, int readBlockSizeBytes, long writeBlockSizeBytes, int pageSize, int batchSize)
    {
        this.executorService = requireNonNull(executorService, "executorService is null");
        this.storage = requireNonNull(storage, "storage is null");
        this.readBlockSizeBytes = readBlockSizeBytes;
        this.writeBlockSizeBytes = writeBlockSizeBytes;
        this.pageSize = pageSize;
        this.batchSize = batchSize;
    }

    @Override
    public TrinoInputFile newInputFile(Location location)
    {
        OssLocation ossLocation = new OssLocation(location);
        checkIsValidFile(ossLocation);
        return new OssInputFile(ossLocation, storage, readBlockSizeBytes, OptionalLong.empty(), Optional.empty(), Optional.empty());
    }

    @Override
    public TrinoInputFile newEncryptedInputFile(Location location, EncryptionKey key)
    {
        OssLocation ossLocation = new OssLocation(location);
        checkIsValidFile(ossLocation);
        return new OssInputFile(ossLocation, storage, readBlockSizeBytes, OptionalLong.empty(), Optional.empty(), Optional.of(key));
    }

    @Override
    public TrinoInputFile newInputFile(Location location, long length)
    {
        OssLocation ossLocation = new OssLocation(location);
        checkIsValidFile(ossLocation);
        return new OssInputFile(ossLocation, storage, readBlockSizeBytes, OptionalLong.of(length), Optional.empty(), Optional.empty());
    }

    @Override
    public TrinoInputFile newEncryptedInputFile(Location location, long length, EncryptionKey key)
    {
        OssLocation ossLocation = new OssLocation(location);
        checkIsValidFile(ossLocation);
        return new OssInputFile(ossLocation, storage, readBlockSizeBytes, OptionalLong.of(length), Optional.empty(), Optional.of(key));
    }

    @Override
    public TrinoInputFile newInputFile(Location location, long length, Instant lastModified)
    {
        OssLocation ossLocation = new OssLocation(location);
        checkIsValidFile(ossLocation);
        return new OssInputFile(ossLocation, storage, readBlockSizeBytes, OptionalLong.of(length), Optional.of(lastModified), Optional.empty());
    }

    @Override
    public TrinoInputFile newEncryptedInputFile(Location location, long length, Instant lastModified, EncryptionKey key)
    {
        OssLocation ossLocation = new OssLocation(location);
        checkIsValidFile(ossLocation);
        return new OssInputFile(ossLocation, storage, readBlockSizeBytes, OptionalLong.of(length), Optional.of(lastModified), Optional.of(key));
    }

    @Override
    public TrinoOutputFile newOutputFile(Location location)
    {
        OssLocation ossLocation = new OssLocation(location);
        checkIsValidFile(ossLocation);
        return new OssOutputFile(ossLocation, storage, writeBlockSizeBytes, Optional.empty());
    }

    @Override
    public TrinoOutputFile newEncryptedOutputFile(Location location, EncryptionKey key)
    {
        OssLocation ossLocation = new OssLocation(location);
        checkIsValidFile(ossLocation);
        return new OssOutputFile(ossLocation, storage, writeBlockSizeBytes, Optional.of(key));
    }

    @Override
    public void deleteFile(Location location)
            throws IOException
    {
        OssLocation ossLocation = new OssLocation(location);
        checkIsValidFile(ossLocation);
        getBlob(storage, ossLocation).ifPresent(Blob::delete);
    }

    @Override
    public void deleteFiles(Collection<Location> locations)
            throws IOException
    {
        List<ListenableFuture<?>> batchFutures = new ArrayList<>();
        try {
            for (List<Location> locationBatch : partition(locations, batchSize)) {
                StorageBatch batch = storage.batch();
                for (Location location : locationBatch) {
                    OssLocation ossLocation = new OssLocation(location);
                    batch.delete(BlobId.of(ossLocation.bucket(), ossLocation.path()));
                }
                batchFutures.add(executorService.submit(batch::submit));
            }
            getFutureValue(Futures.allAsList(batchFutures));
        }
        catch (RuntimeException e) {
            throw handleOssException(e, "delete files", locations);
        }
    }

    @Override
    public void deleteDirectory(Location location)
            throws IOException
    {
        OssLocation ossLocation = new OssLocation(normalizeToDirectory(location));
        try {
            List<ListenableFuture<?>> batchFutures = new ArrayList<>();

            for (List<Blob> blobBatch : partition(getPage(ossLocation).iterateAll(), batchSize)) {
                StorageBatch batch = storage.batch();
                for (Blob blob : blobBatch) {
                    batch.delete(blob.getBlobId());
                }
                batchFutures.add(executorService.submit(batch::submit));
            }
            getFutureValue(Futures.allAsList(batchFutures));
        }
        catch (RuntimeException e) {
            throw handleOssException(e, "deleting directory", ossLocation);
        }
    }

    @Override
    public void renameFile(Location source, Location target)
            throws IOException
    {
        throw new TrinoFileSystemException("Oss does not support renames");
    }

    @Override
    public FileIterator listFiles(Location location)
            throws IOException
    {
        OssLocation ossLocation = new OssLocation(normalizeToDirectory(location));
        try {
            return new OssFileIterator(ossLocation, getPage(ossLocation));
        }
        catch (RuntimeException e) {
            throw handleOssException(e, "listing files", ossLocation);
        }
    }

    private static Location normalizeToDirectory(Location location)
    {
        String path = location.path();
        if (!path.isEmpty() && !path.endsWith("/")) {
            return location.appendSuffix("/");
        }
        return location;
    }

    private static void checkIsValidFile(OssLocation ossLocation)
    {
        checkState(!ossLocation.path().isEmpty(), "Location path is empty: %s", ossLocation);
        checkState(!ossLocation.path().endsWith("/"), "Location path ends with a slash: %s", ossLocation);
    }

    private Page<Blob> getPage(OssLocation location, BlobListOption... blobListOptions)
    {
        List<BlobListOption> optionsBuilder = new ArrayList<>();

        if (!location.path().isEmpty()) {
            optionsBuilder.add(BlobListOption.prefix(location.path()));
        }
        optionsBuilder.addAll(Arrays.asList(blobListOptions));
        optionsBuilder.add(pageSize(this.pageSize));
        return storage.list(location.bucket(), optionsBuilder.toArray(BlobListOption[]::new));
    }

    @Override
    public Optional<Boolean> directoryExists(Location location)
            throws IOException
    {
        // Notes:
        // Oss is not hierarchical, there is no way to determine the difference
        // between an empty blob and a directory: for this case Optional.empty() will be returned per the super
        // method spec.
        //
        // Note on blob.isDirectory: The isDirectory() method returns false unless invoked via storage.list()
        // with currentDirectory() enabled for empty blobs intended to be used as directories.
        // The only time blob.isDirectory() is true is when an object was created that introduced the path:
        //
        // Example 1: createBlob("bucket", "dir") creates an empty blob intended to be used as a "directory"
        // you can then create a file "bucket", "dir/file")
        // Invoking  blob.isDirectory() on "dir" returns false even after the "dir/file" object is created.
        //
        // Example 2: createBlob("bucket", "dir2/file") when "dir2" does not exist will return true for isDirectory()
        // when invoked on the "dir2/" path. Also note that the blob name has a trailing slash.
        // This behavior is only enabled with BlobListOption.currentDirectory() and isDirectory() is only true when the blob
        // is returned from a storage.list operation.

        OssLocation ossLocation = new OssLocation(location);
        if (ossLocation.path().isEmpty()) {
            return Optional.of(bucketExists(ossLocation.bucket()));
        }
        if (listFiles(location).hasNext()) {
            return Optional.of(true);
        }
        return Optional.empty();
    }

    private boolean bucketExists(String bucket)
    {
        return storage.get(bucket) != null;
    }

    @Override
    public void createDirectory(Location location)
            throws IOException
    {
        validateOssLocation(location);
    }

    @Override
    public void renameDirectory(Location source, Location target)
            throws IOException
    {
        throw new TrinoFileSystemException("OSS does not support directory renames");
    }

    @Override
    public Set<Location> listDirectories(Location location)
            throws IOException
    {
        OssLocation ossLocation = new OssLocation(normalizeToDirectory(location));
        try {
            Page<Blob> page = getPage(ossLocation, currentDirectory(), matchGlob(ossLocation.path() + "*/"));
            Iterator<Blob> blobIterator = Iterators.filter(page.iterateAll().iterator(), BlobInfo::isDirectory);
            ImmutableSet.Builder<Location> locationBuilder = ImmutableSet.builder();
            while (blobIterator.hasNext()) {
                locationBuilder.add(Location.of(ossLocation.getBase() + blobIterator.next().getName()));
            }
            return locationBuilder.build();
        }
        catch (RuntimeException e) {
            throw handleOssException(e, "listing directories", ossLocation);
        }
    }

    @Override
    public Optional<Location> createTemporaryDirectory(Location targetPath, String temporaryPrefix, String relativePrefix)
    {
        validateOssLocation(targetPath);
        // OSS does not have directories
        return Optional.empty();
    }

    @Override
    public Optional<UriLocation> preSignedUri(Location location, Duration ttl)
            throws IOException
    {
        return preSignedUri(location, ttl, Optional.empty());
    }

    @Override
    public Optional<UriLocation> encryptedPreSignedUri(Location location, Duration ttl, EncryptionKey key)
            throws IOException
    {
        return preSignedUri(location, ttl, Optional.of(key));
    }

    private Optional<UriLocation> preSignedUri(Location location, Duration ttl, Optional<EncryptionKey> key)
            throws IOException
    {
        OssLocation ossLocation = new OssLocation(location);
        BlobInfo blobInfo = BlobInfo
                .newBuilder(BlobId.of(ossLocation.bucket(), ossLocation.path()))
                .build();

        Map<String, String> extHeaders = preSignedHeaders(key);
        URL url = storage.signUrl(blobInfo, ttl.toMillis(), MILLISECONDS, withV4Signature(), withExtHeaders(extHeaders));
        try {
            return Optional.of(new UriLocation(url.toURI(), toMultiMap(extHeaders)));
        }
        catch (URISyntaxException e) {
            throw new IOException("Error creating URI for location: " + location, e);
        }
    }

    private static Map<String, String> preSignedHeaders(Optional<EncryptionKey> key)
    {
        if (key.isEmpty()) {
            return ImmutableMap.of();
        }

        EncryptionKey encryption = key.get();
        ImmutableMap.Builder<String, String> headers = ImmutableMap.builderWithExpectedSize(3);
        headers.put("x-goog-encryption-algorithm", encryption.algorithm());
        headers.put("x-goog-encryption-key", encodedKey(encryption));
        headers.put("x-goog-encryption-key-sha256", keySha256Checksum(encryption));
        return headers.buildOrThrow();
    }

    private Map<String, List<String>> toMultiMap(Map<String, String> extHeaders)
    {
        return extHeaders.entrySet().stream()
                .map(entry -> Map.entry(entry.getKey(), ImmutableList.of(entry.getValue())))
                .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    @SuppressWarnings("ResultOfObjectAllocationIgnored")
    private static void validateOssLocation(Location location)
    {
        new OssLocation(location);
    }
}
