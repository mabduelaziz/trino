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

import io.airlift.units.Duration;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoOutputFile;
import io.trino.filesystem.UriLocation;
import io.trino.filesystem.encryption.EncryptionKey;

import java.io.IOException;
import java.time.Instant;
import java.util.Collection;
import java.util.Optional;
import java.util.Set;

public class OssFileSystem
        implements TrinoFileSystem
{
    @Override
    public TrinoInputFile newInputFile(Location location)
    {
        throw new UnsupportedOperationException("newInputFile not supported");
    }

    @Override
    public TrinoInputFile newInputFile(Location location, long length)
    {
        throw new UnsupportedOperationException("newInputFile with length not supported");
    }

    @Override
    public TrinoInputFile newInputFile(Location location, long length, Instant lastModified)
    {
        throw new UnsupportedOperationException("newInputFile with length and lastModified not supported");
    }

    @Override
    public TrinoInputFile newEncryptedInputFile(Location location, EncryptionKey key)
    {
        return TrinoFileSystem.super.newEncryptedInputFile(location, key);
    }

    @Override
    public TrinoInputFile newEncryptedInputFile(Location location, long length, EncryptionKey key)
    {
        return TrinoFileSystem.super.newEncryptedInputFile(location, length, key);
    }

    @Override
    public TrinoInputFile newEncryptedInputFile(Location location, long length, Instant lastModified, EncryptionKey key)
    {
        return TrinoFileSystem.super.newEncryptedInputFile(location, length, lastModified, key);
    }

    @Override
    public TrinoOutputFile newOutputFile(Location location)
    {
        throw new UnsupportedOperationException("newOutputFile not supported");
    }

    @Override
    public TrinoOutputFile newEncryptedOutputFile(Location location, EncryptionKey key)
    {
        return TrinoFileSystem.super.newEncryptedOutputFile(location, key);
    }

    @Override
    public void deleteFile(Location location)
            throws IOException
    {
        throw new UnsupportedOperationException("deleteFile not supported");
    }

    @Override
    public void deleteFiles(Collection<Location> locations)
            throws IOException
    {
        TrinoFileSystem.super.deleteFiles(locations);
    }

    @Override
    public void deleteDirectory(Location location)
            throws IOException
    {
        throw new UnsupportedOperationException("deleteDirectory not supported");
    }

    @Override
    public void renameFile(Location source, Location target)
            throws IOException
    {
        throw new UnsupportedOperationException("renameFile not supported");
    }

    @Override
    public FileIterator listFiles(Location location)
            throws IOException
    {
        throw new UnsupportedOperationException("listFiles not supported");
    }

    @Override
    public Optional<Boolean> directoryExists(Location location)
            throws IOException
    {
        throw new UnsupportedOperationException("directoryExists not supported");
    }

    @Override
    public void createDirectory(Location location)
            throws IOException
    {
        throw new UnsupportedOperationException("createDirectory not supported");
    }

    @Override
    public void renameDirectory(Location source, Location target)
            throws IOException
    {
        throw new UnsupportedOperationException("renameDirectory not supported");
    }

    @Override
    public Set<Location> listDirectories(Location location)
            throws IOException
    {
        throw new UnsupportedOperationException("listDirectories not supported");
    }

    @Override
    public Optional<Location> createTemporaryDirectory(Location targetPath, String temporaryPrefix, String relativePrefix)
            throws IOException
    {
        throw new UnsupportedOperationException("createTemporaryDirectory not supported");
    }

    @Override
    public Optional<UriLocation> preSignedUri(Location location, Duration ttl)
            throws IOException
    {
        return TrinoFileSystem.super.preSignedUri(location, ttl);
    }

    @Override
    public Optional<UriLocation> encryptedPreSignedUri(Location location, Duration ttl, EncryptionKey key)
            throws IOException
    {
        return TrinoFileSystem.super.encryptedPreSignedUri(location, ttl, key);
    }
}
