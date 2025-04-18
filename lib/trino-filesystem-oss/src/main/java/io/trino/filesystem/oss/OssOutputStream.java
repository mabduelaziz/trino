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
import com.aliyun.oss.model.AbortMultipartUploadRequest;
import com.aliyun.oss.model.CompleteMultipartUploadRequest;
import com.aliyun.oss.model.InitiateMultipartUploadRequest;
import com.aliyun.oss.model.InitiateMultipartUploadResult;
import com.aliyun.oss.model.PartETag;
import com.aliyun.oss.model.UploadPartRequest;
import com.aliyun.oss.model.UploadPartResult;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class OssOutputStream
        extends OutputStream
{
    private final OssLocation location;
    private final OSS client;
    private final ByteArrayOutputStream buffer;
    private final List<PartETag> partETags;
    private String uploadId;
    private int partNumber;
    private boolean closed;

    public OssOutputStream(OssLocation location, OSS client)
    {
        this.location = requireNonNull(location, "location is null");
        this.client = requireNonNull(client, "client is null");
        this.buffer = new ByteArrayOutputStream();
        this.partETags = new ArrayList<>();
        this.partNumber = 1;
    }

    @Override
    public void write(int b)
            throws IOException
    {
        ensureOpen();
        buffer.write(b);
        if (buffer.size() >= 5 * 1024 * 1024) { // 5MB minimum part size
            uploadPart();
        }
    }

    @Override
    public void write(byte[] b, int off, int len)
            throws IOException
    {
        ensureOpen();
        buffer.write(b, off, len);
        if (buffer.size() >= 5 * 1024 * 1024) { // 5MB minimum part size
            uploadPart();
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
            if (buffer.size() > 0) {
                uploadPart();
            }
            if (uploadId != null) {
                CompleteMultipartUploadRequest request = new CompleteMultipartUploadRequest(location.bucket(), location.key(), uploadId, partETags);
                client.completeMultipartUpload(request);
            }
            else if (buffer.size() > 0) {
                // If the file is small enough, use simple upload
                client.putObject(location.bucket(), location.key(), new ByteArrayInputStream(buffer.toByteArray()));
            }
        }
        catch (Exception e) {
            if (uploadId != null) {
                try {
                    AbortMultipartUploadRequest request = new AbortMultipartUploadRequest(location.bucket(), location.key(), uploadId);
                    client.abortMultipartUpload(request);
                }
                catch (Exception ignored) {
                    // Ignore abort failure
                }
            }
            throw new IOException("Failed to close stream for file: " + location, e);
        }
    }

    private void uploadPart()
            throws IOException
    {
        try {
            if (uploadId == null) {
                InitiateMultipartUploadRequest request = new InitiateMultipartUploadRequest(location.bucket(), location.key());
                InitiateMultipartUploadResult result = client.initiateMultipartUpload(request);
                uploadId = result.getUploadId();
            }

            byte[] data = buffer.toByteArray();
            buffer.reset();

            UploadPartRequest request = new UploadPartRequest(location.bucket(), location.key(), uploadId, partNumber, new ByteArrayInputStream(data), data.length);
            UploadPartResult result = client.uploadPart(request);
            partETags.add(new PartETag(partNumber, result.getETag()));
            partNumber++;
        }
        catch (Exception e) {
            throw new IOException("Failed to upload part for file: " + location, e);
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
