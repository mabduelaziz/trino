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
import io.trino.filesystem.TrinoInputStream;

import java.io.IOException;
import java.io.InputStream;

import static java.util.Objects.requireNonNull;

public class OssInputStream
        extends TrinoInputStream
{
    private final OssLocation location;
    private final OSS client;
    private OSSObject object;
    private InputStream inputStream;
    private volatile boolean closed;

    public OssInputStream(OssLocation location, OSS client)
    {
        this.location = requireNonNull(location, "location is null");
        this.client = requireNonNull(client, "client is null");
    }

    @Override
    public long getPosition()
            throws IOException
    {
        ensureOpen();
        return 0; // OSS streams don't track position
    }

    @Override
    public void seek(long position)
            throws IOException
    {
        ensureOpen();
        if (position < 0) {
            throw new IOException("Invalid position: " + position);
        }
        if (position > 0) {
            throw new IOException("Seeking not supported for OSS streams");
        }
    }

    @Override
    public int read()
            throws IOException
    {
        ensureOpen();
        try {
            if (inputStream == null) {
                object = client.getObject(location.bucket(), location.key());
                inputStream = object.getObjectContent();
            }
            return inputStream.read();
        }
        catch (Exception e) {
            throw new IOException("Failed to read from file: " + location, e);
        }
    }

    @Override
    public int read(byte[] buffer, int offset, int length)
            throws IOException
    {
        ensureOpen();
        try {
            if (inputStream == null) {
                object = client.getObject(location.bucket(), location.key());
                inputStream = object.getObjectContent();
            }
            return inputStream.read(buffer, offset, length);
        }
        catch (Exception e) {
            throw new IOException("Failed to read from file: " + location, e);
        }
    }

    @Override
    public void close()
            throws IOException
    {
        if (closed) {
            return;
        }
        closed = true;
        try {
            if (inputStream != null) {
                inputStream.close();
            }
            if (object != null) {
                object.close();
            }
        }
        catch (Exception e) {
            throw new IOException("Failed to close stream for file: " + location, e);
        }
    }

    private void ensureOpen()
            throws IOException
    {
        if (closed) {
            throw new IOException("Stream closed: " + location);
        }
    }

    @Override
    public String toString()
    {
        return location.toString();
    }
}
