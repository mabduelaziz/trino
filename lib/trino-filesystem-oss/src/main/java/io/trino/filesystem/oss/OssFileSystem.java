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

import com.aliyun.oss.OSS;
import com.aliyun.oss.model.DeleteObjectsRequest;
import com.aliyun.oss.model.DeleteObjectsResult;
import com.aliyun.oss.model.ListObjectsRequest;
import com.aliyun.oss.model.OSSObjectSummary;
import com.aliyun.oss.model.ObjectListing;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import io.airlift.units.Duration;
import io.trino.filesystem.FileEntry;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemException;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoOutputFile;
import io.trino.filesystem.UriLocation;
import io.trino.filesystem.encryption.EncryptionKey;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.stream.Stream;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.partition;
import static com.google.common.collect.Multimaps.toMultimap;
import static java.util.Objects.requireNonNull;

public class OssFileSystem
        implements TrinoFileSystem
{
    private final OSS client;

    public OssFileSystem(OSS client)
    {
        this.client = requireNonNull(client, "client is null");
    }

    private static void validateOssLocation(Location location)
    {
        verify(location.path().contains("/"), "Invalid OSS location: %s", location);
    }

    @Override
    public TrinoInputFile newInputFile(Location location)
    {
        String bucket = getBucketFromLocation(location);
        String key = getKeyFromLocation(location);
        return new OssInputFile(location, client, bucket, key, OptionalLong.empty(), Optional.empty());
    }

    @Override
    public TrinoInputFile newInputFile(Location location, long length)
    {
        String bucket = getBucketFromLocation(location);
        String key = getKeyFromLocation(location);
        return new OssInputFile(location, client, bucket, key, OptionalLong.of(length), Optional.empty());
    }

    @Override
    public TrinoInputFile newInputFile(Location location, long length, Instant lastModified)
    {
        String bucket = getBucketFromLocation(location);
        String key = getKeyFromLocation(location);
        return new OssInputFile(location, client, bucket, key, OptionalLong.of(length), Optional.of(lastModified));
    }

    @Override
    public TrinoInputFile newEncryptedInputFile(Location location, EncryptionKey key)
    {
        // OSS doesn't support client-side encryption
        return newInputFile(location);
    }

    @Override
    public TrinoInputFile newEncryptedInputFile(Location location, long length, EncryptionKey key)
    {
        // OSS doesn't support client-side encryption
        return newInputFile(location, length);
    }

    @Override
    public TrinoInputFile newEncryptedInputFile(Location location, long length, Instant lastModified, EncryptionKey key)
    {
        // OSS doesn't support client-side encryption
        return newInputFile(location, length, lastModified);
    }

    @Override
    public TrinoOutputFile newOutputFile(Location location)
    {
        String bucket = getBucketFromLocation(location);
        String key = getKeyFromLocation(location);
        return new OssOutputFile(location, client, bucket, key);
    }

    @Override
    public TrinoOutputFile newEncryptedOutputFile(Location location, EncryptionKey key)
    {
        // OSS doesn't support client-side encryption
        return newOutputFile(location);
    }

    @Override
    public void deleteFile(Location location)
            throws IOException
    {
        location.verifyValidFileLocation();
        String bucket = getBucketFromLocation(location);
        String key = getKeyFromLocation(location);

        try {
            client.deleteObject(bucket, key);
        }
        catch (Exception e) {
            throw new TrinoFileSystemException("Failed to delete file: " + location, e);
        }
    }

    @Override
    public void deleteDirectory(Location location)
            throws IOException
    {
        FileIterator iterator = listObjects(location, true);
        while (iterator.hasNext()) {
            List<Location> files = new ArrayList<>();
            while ((files.size() < 1000) && iterator.hasNext()) {
                files.add(iterator.next().location());
            }
            deleteObjects(files);
        }
    }

    @Override
    public void deleteFiles(Collection<Location> locations)
            throws IOException
    {
        locations.forEach(Location::verifyValidFileLocation);
        deleteObjects(locations);
    }

    private void deleteObjects(Collection<Location> locations)
            throws IOException
    {
        SetMultimap<String, String> bucketToKeys = locations.stream()
                .collect(toMultimap(
                        this::getBucketFromLocation,
                        this::getKeyFromLocation,
                        HashMultimap::create));

        Map<String, String> failures = new HashMap<>();

        for (Entry<String, Collection<String>> entry : bucketToKeys.asMap().entrySet()) {
            String bucket = entry.getKey();
            Collection<String> allKeys = entry.getValue();

            for (List<String> keys : partition(allKeys, 250)) {
                DeleteObjectsRequest request = new DeleteObjectsRequest(bucket);
                request.setKeys(keys);
                request.setQuiet(true);

                try {
                    DeleteObjectsResult result = client.deleteObjects(request);
                    for (String key : result.getDeletedObjects()) {
                        failures.put("oss://%s/%s".formatted(bucket, key), "Failed to delete");
                    }
                }
                catch (Exception e) {
                    throw new TrinoFileSystemException("Error while batch deleting files", e);
                }
            }
        }

        if (!failures.isEmpty()) {
            throw new IOException("Failed to delete one or more files: " + failures);
        }
    }

    @Override
    public void renameFile(Location source, Location target)
            throws IOException
    {
        throw new IOException("OSS does not support renames");
    }

    @Override
    public FileIterator listFiles(Location location)
            throws IOException
    {
        return listObjects(location, false);
    }

    private FileIterator listObjects(Location location, boolean includeDirectoryObjects)
            throws IOException
    {
        String bucket = getBucketFromLocation(location);
        String key = getKeyFromLocation(location);

        if (!key.isEmpty() && !key.endsWith("/")) {
            key += "/";
        }

        ListObjectsRequest request = new ListObjectsRequest(bucket);
        request.setPrefix(key);

        try {
            ObjectListing result = client.listObjects(request);
            Stream<OSSObjectSummary> objectStream = result.getObjectSummaries().stream();
            if (!includeDirectoryObjects) {
                objectStream = objectStream.filter(object -> !object.getKey().endsWith("/"));
            }
            return new OssFileIterator(location, objectStream.iterator());
        }
        catch (Exception e) {
            throw new TrinoFileSystemException("Failed to list location: " + location, e);
        }
    }

    @Override
    public Optional<Boolean> directoryExists(Location location)
            throws IOException
    {
        validateOssLocation(location);
        if (location.path().isEmpty() || listFiles(location).hasNext()) {
            return Optional.of(true);
        }
        return Optional.empty();
    }

    @Override
    public void createDirectory(Location location)
    {
        validateOssLocation(location);
        // OSS does not have directories
    }

    @Override
    public void renameDirectory(Location source, Location target)
            throws IOException
    {
        throw new IOException("OSS does not support directory renames");
    }

    @Override
    public Set<Location> listDirectories(Location location)
            throws IOException
    {
        String bucket = getBucketFromLocation(location);
        String key = getKeyFromLocation(location);

        if (!key.isEmpty() && !key.endsWith("/")) {
            key += "/";
        }

        ListObjectsRequest request = new ListObjectsRequest(bucket);
        request.setPrefix(key);
        request.setDelimiter("/");

        try {
            ObjectListing result = client.listObjects(request);
            return result.getCommonPrefixes().stream()
                    .map(location::appendPath)
                    .collect(toImmutableSet());
        }
        catch (Exception e) {
            throw new TrinoFileSystemException("Failed to list location: " + location, e);
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
        return encryptedPreSignedUri(location, ttl, Optional.empty());
    }

    @Override
    public Optional<UriLocation> encryptedPreSignedUri(Location location, Duration ttl, EncryptionKey key)
            throws IOException
    {
        return encryptedPreSignedUri(location, ttl, Optional.of(key));
    }

    private Optional<UriLocation> encryptedPreSignedUri(Location location, Duration ttl, Optional<EncryptionKey> encryptionKey)
            throws IOException
    {
        location.verifyValidFileLocation();
        String bucket = getBucketFromLocation(location);
        String objectKey = getKeyFromLocation(location);

        try {
            Date expiration = Date.from(Instant.now().plusMillis(ttl.toMillis()));
            URI uri = client.generatePresignedUrl(bucket, objectKey, expiration).toURI();
            return Optional.of(new UriLocation(uri, Map.of()));
        }
        catch (URISyntaxException e) {
            throw new TrinoFileSystemException("Failed to convert pre-signed URL to URI", e);
        }
        catch (Exception e) {
            throw new IOException("Failed to generate pre-signed URI", e);
        }
    }

    private String getBucketFromLocation(Location location)
    {
        String path = location.path();
        int firstSlash = path.indexOf('/');
        return firstSlash == -1 ? path : path.substring(0, firstSlash);
    }

    private String getKeyFromLocation(Location location)
    {
        String path = location.path();
        int firstSlash = path.indexOf('/');
        return firstSlash == -1 ? "" : path.substring(firstSlash + 1);
    }

    private record OssFileIterator(Location baseLocation, Iterator<OSSObjectSummary> iterator)
            implements FileIterator
    {
        private static String getBucketFromLocation(Location location)
        {
            String path = location.path();
            int firstSlash = path.indexOf('/');
            return firstSlash == -1 ? path : path.substring(0, firstSlash);
        }

        @Override
        public boolean hasNext()
        {
            return iterator.hasNext();
        }

        @Override
        public FileEntry next()
        {
            OSSObjectSummary summary = iterator.next();
            String objectKey = summary.getKey();
            return new FileEntry(
                    baseLocation.appendPath(objectKey),
                    summary.getSize(),
                    Instant.ofEpochMilli(summary.getLastModified().getTime()),
                    Optional.empty());
        }
    }
}
