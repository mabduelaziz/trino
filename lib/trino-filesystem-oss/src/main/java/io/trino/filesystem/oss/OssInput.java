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
import io.trino.filesystem.TrinoInput;

import java.io.IOException;
import java.io.InputStream;

import static java.util.Objects.requireNonNull;

final class OssInput
        implements TrinoInput
{
    private final OssLocation location;
    private final OSS client;
    private volatile boolean closed;

    public OssInput(OssLocation location, OSS client)
    {
        this.location = requireNonNull(location, "location is null");
        this.client = requireNonNull(client, "client is null");
        location.location().verifyValidFileLocation();
    }

    @Override
    public void readFully(long position, byte[] buffer, int bufferOffset, int bufferLength)
            throws IOException
    {
        ensureOpen();
        try (OSSObject object = client.getObject(location.bucket(), location.key())) {
            try (InputStream inputStream = object.getObjectContent()) {
                inputStream.skip(position);
                int bytesRead = 0;
                while (bytesRead < bufferLength) {
                    int n = inputStream.read(buffer, bufferOffset + bytesRead, bufferLength - bytesRead);
                    if (n == -1) {
                        throw new IOException("End of stream reached before reading all bytes");
                    }
                    bytesRead += n;
                }
            }
        }
        catch (Exception e) {
            throw new IOException("Failed to read file: " + location, e);
        }
    }

    @Override
    public int readTail(byte[] buffer, int bufferOffset, int bufferLength)
            throws IOException
    {
        ensureOpen();
        try (OSSObject object = client.getObject(location.bucket(), location.key())) {
            long fileSize = object.getObjectMetadata().getContentLength();
            long startPosition = Math.max(0, fileSize - bufferLength);
            int readLength = (int) Math.min(bufferLength, fileSize);

            try (InputStream inputStream = object.getObjectContent()) {
                inputStream.skip(startPosition);
                int bytesRead = 0;
                while (bytesRead < readLength) {
                    int n = inputStream.read(buffer, bufferOffset + bytesRead, readLength - bytesRead);
                    if (n == -1) {
                        break;
                    }
                    bytesRead += n;
                }
                return bytesRead;
            }
        }
        catch (Exception e) {
            throw new IOException("Failed to read tail of file: " + location, e);
        }
    }

    @Override
    public void close()
            throws IOException
    {
        closed = true;
    }

    private void ensureOpen()
            throws IOException
    {
        if (closed) {
            throw new IOException("Input stream closed: " + location);
        }
    }

    @Override
    public String toString()
    {
        return location.toString();
    }
}
