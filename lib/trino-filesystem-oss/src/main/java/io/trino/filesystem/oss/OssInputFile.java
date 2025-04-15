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
import com.aliyun.oss.model.OSSObject;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoInput;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoInputStream;

import java.io.IOException;
import java.time.Instant;
import java.util.Optional;
import java.util.OptionalLong;

import static java.util.Objects.requireNonNull;

public class OssInputFile
        implements TrinoInputFile
{
    private final Location location;
    private final OSS client;
    private final String bucket;
    private final String key;
    private final OptionalLong length;
    private final Optional<Instant> lastModified;

    public OssInputFile(Location location, OSS client, String bucket, String key, OptionalLong length, Optional<Instant> lastModified)
    {
        this.location = requireNonNull(location, "location is null");
        this.client = requireNonNull(client, "client is null");
        this.bucket = requireNonNull(bucket, "bucket is null");
        this.key = requireNonNull(key, "key is null");
        this.length = requireNonNull(length, "length is null");
        this.lastModified = requireNonNull(lastModified, "lastModified is null");
    }

    @Override
    public TrinoInput newInput()
            throws IOException
    {
        return new OssInput(location, client, bucket, key);
    }

    @Override
    public TrinoInputStream newStream()
            throws IOException
    {
        return new OssInputStream(location, client, bucket, key);
    }

    @Override
    public long length()
            throws IOException
    {
        if (length.isPresent()) {
            return length.getAsLong();
        }
        try {
            OSSObject object = client.getObject(bucket, key);
            return object.getObjectMetadata().getContentLength();
        }
        catch (Exception e) {
            throw new IOException("Failed to get length for file: " + location, e);
        }
    }

    @Override
    public Instant lastModified()
            throws IOException
    {
        if (lastModified.isPresent()) {
            return lastModified.get();
        }
        try {
            OSSObject object = client.getObject(bucket, key);
            return object.getObjectMetadata().getLastModified().toInstant();
        }
        catch (Exception e) {
            throw new IOException("Failed to get last modified time for file: " + location, e);
        }
    }

    @Override
    public boolean exists()
            throws IOException
    {
        try {
            return client.doesObjectExist(bucket, key);
        }
        catch (Exception e) {
            throw new IOException("Failed to check existence for file: " + location, e);
        }
    }

    @Override
    public Location location()
    {
        return location;
    }

    @Override
    public String toString()
    {
        return location.toString();
    }
}
