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
import com.aliyun.oss.model.ObjectMetadata;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoOutputFile;
import io.trino.memory.context.AggregatedMemoryContext;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;

import static java.util.Objects.requireNonNull;

public class OssOutputFile
        implements TrinoOutputFile
{
    private final OssLocation location;
    private final OSS client;

    public OssOutputFile(OssLocation location, OSS client)
    {
        this.location = requireNonNull(location, "location is null");
        this.client = requireNonNull(client, "client is null");
    }

    @Override
    public OutputStream create()
            throws IOException
    {
        return new OssOutputStream(location, client);
    }

    @Override
    public void createOrOverwrite(byte[] data)
            throws IOException
    {
        try {
            ObjectMetadata metadata = new ObjectMetadata();
            metadata.setContentLength(data.length);
            client.putObject(location.bucket(), location.key(), new ByteArrayInputStream(data), metadata);
        }
        catch (Exception e) {
            throw new IOException("Failed to write file: " + location, e);
        }
    }

    @Override
    public void createExclusive(byte[] data)
            throws IOException
    {
        try {
            if (client.doesObjectExist(location.bucket(), location.key())) {
                throw new IOException("File already exists: " + location);
            }
            createOrOverwrite(data);
        }
        catch (Exception e) {
            throw new IOException("Failed to write file: " + location, e);
        }
    }

    @Override
    public OutputStream create(AggregatedMemoryContext memoryContext)
            throws IOException
    {
        // Memory context is not used in OSS implementation
        return create();
    }

    @Override
    public Location location()
    {
        return location.location();
    }

    @Override
    public String toString()
    {
        return location.toString();
    }
}
