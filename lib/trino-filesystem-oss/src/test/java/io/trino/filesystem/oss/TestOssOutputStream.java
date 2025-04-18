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
import com.aliyun.oss.model.CompleteMultipartUploadRequest;
import com.aliyun.oss.model.CompleteMultipartUploadResult;
import com.aliyun.oss.model.InitiateMultipartUploadRequest;
import com.aliyun.oss.model.InitiateMultipartUploadResult;
import com.aliyun.oss.model.UploadPartRequest;
import com.aliyun.oss.model.UploadPartResult;
import io.trino.filesystem.Location;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TestOssOutputStream
{
    private static final String TEST_UPLOAD_ID = "test-upload-id";
    private static final String TEST_BUCKET = "test-bucket";
    private static final String TEST_KEY = "test-key";

    @Mock
    private OSS ossClient;
    private OssLocation location;
    private OssOutputStream outputStream;

    @BeforeEach
    void setUp()
            throws Exception
    {
        MockitoAnnotations.openMocks(this);
        URI uri = new URI("oss://" + TEST_BUCKET + "/" + TEST_KEY);
        location = new OssLocation(Location.of(uri.toString()));

        // Mock multipart upload initiation
        InitiateMultipartUploadResult initResult = new InitiateMultipartUploadResult();
        initResult.setUploadId(TEST_UPLOAD_ID);
        when(ossClient.initiateMultipartUpload(any(InitiateMultipartUploadRequest.class)))
                .thenReturn(initResult);
    }

    @Test
    void testWrite()
            throws IOException
    {
        // Mock upload part
        UploadPartResult uploadResult = new UploadPartResult();
        uploadResult.setETag("test-etag");
        when(ossClient.uploadPart(any(UploadPartRequest.class))).thenReturn(uploadResult);

        // Mock complete multipart upload
        CompleteMultipartUploadResult completeResult = new CompleteMultipartUploadResult();
        when(ossClient.completeMultipartUpload(any(CompleteMultipartUploadRequest.class)))
                .thenReturn(completeResult);

        outputStream = new OssOutputStream(location, ossClient);
        byte[] data = "test data".getBytes(StandardCharsets.UTF_8);
        outputStream.write(data);
        outputStream.close();

        // Verify that the upload was initiated and completed
        verify(ossClient).initiateMultipartUpload(any(InitiateMultipartUploadRequest.class));
        verify(ossClient).completeMultipartUpload(any(CompleteMultipartUploadRequest.class));
    }

    @Test
    void testWriteWithOffset()
            throws IOException
    {
        // Mock upload part
        UploadPartResult uploadResult = new UploadPartResult();
        uploadResult.setETag("test-etag");
        when(ossClient.uploadPart(any(UploadPartRequest.class))).thenReturn(uploadResult);

        // Mock complete multipart upload
        CompleteMultipartUploadResult completeResult = new CompleteMultipartUploadResult();
        when(ossClient.completeMultipartUpload(any(CompleteMultipartUploadRequest.class)))
                .thenReturn(completeResult);

        outputStream = new OssOutputStream(location, ossClient);
        byte[] data = "test data".getBytes(StandardCharsets.UTF_8);
        outputStream.write(data, 2, 4);
        outputStream.close();

        // Verify that the upload was initiated and completed
        verify(ossClient).initiateMultipartUpload(any(InitiateMultipartUploadRequest.class));
        verify(ossClient).completeMultipartUpload(any(CompleteMultipartUploadRequest.class));
    }

    @Test
    void testWriteError()
    {
        when(ossClient.uploadPart(any(UploadPartRequest.class)))
                .thenThrow(new RuntimeException("Test error"));

        outputStream = new OssOutputStream(location, ossClient);
        byte[] data = "test data".getBytes(StandardCharsets.UTF_8);

        assertThatThrownBy(() -> {
            outputStream.write(data);
            outputStream.close();
        }).isInstanceOf(IOException.class)
                .hasMessageContaining("Failed to close stream for file")
                .hasCauseInstanceOf(IOException.class)
                .hasRootCauseMessage("Test error");
    }

    @Test
    void testClose()
            throws IOException
    {
        // Mock upload part
        UploadPartResult uploadResult = new UploadPartResult();
        uploadResult.setETag("test-etag");
        when(ossClient.uploadPart(any(UploadPartRequest.class))).thenReturn(uploadResult);

        // Mock complete multipart upload
        CompleteMultipartUploadResult completeResult = new CompleteMultipartUploadResult();
        when(ossClient.completeMultipartUpload(any(CompleteMultipartUploadRequest.class)))
                .thenReturn(completeResult);

        // Mock initiate multipart upload
        InitiateMultipartUploadResult initResult = new InitiateMultipartUploadResult();
        initResult.setUploadId(TEST_UPLOAD_ID);
        when(ossClient.initiateMultipartUpload(any(InitiateMultipartUploadRequest.class)))
                .thenReturn(initResult);

        outputStream = new OssOutputStream(location, ossClient);
        byte[] data = new byte[5 * 1024 * 1024]; // 5MB to trigger multipart upload
        outputStream.write(data);
        outputStream.close();

        verify(ossClient).completeMultipartUpload(any(CompleteMultipartUploadRequest.class));
    }

    @Test
    void testCloseError()
    {
        when(ossClient.completeMultipartUpload(any(CompleteMultipartUploadRequest.class)))
                .thenThrow(new RuntimeException("Test error"));

        // Mock initiate multipart upload
        InitiateMultipartUploadResult initResult = new InitiateMultipartUploadResult();
        initResult.setUploadId(TEST_UPLOAD_ID);
        when(ossClient.initiateMultipartUpload(any(InitiateMultipartUploadRequest.class)))
                .thenReturn(initResult);

        // Mock upload part
        UploadPartResult uploadResult = new UploadPartResult();
        uploadResult.setETag("test-etag");
        when(ossClient.uploadPart(any(UploadPartRequest.class))).thenReturn(uploadResult);

        outputStream = new OssOutputStream(location, ossClient);
        byte[] data = new byte[5 * 1024 * 1024]; // 5MB to trigger multipart upload

        assertThatThrownBy(() -> {
            outputStream.write(data);
            outputStream.close();
        }).isInstanceOf(IOException.class)
                .hasMessageContaining("Failed to close stream for file")
                .hasCauseInstanceOf(RuntimeException.class)
                .hasRootCauseMessage("Test error");
    }

    @Test
    void testFlush()
            throws IOException
    {
        // Mock initiate multipart upload
        InitiateMultipartUploadResult initResult = new InitiateMultipartUploadResult();
        initResult.setUploadId(TEST_UPLOAD_ID);
        when(ossClient.initiateMultipartUpload(any(InitiateMultipartUploadRequest.class)))
                .thenReturn(initResult);

        // Mock upload part
        UploadPartResult uploadResult = new UploadPartResult();
        uploadResult.setETag("test-etag");
        when(ossClient.uploadPart(any(UploadPartRequest.class))).thenReturn(uploadResult);

        outputStream = new OssOutputStream(location, ossClient);
        byte[] data = new byte[5 * 1024 * 1024]; // 5MB to trigger multipart upload
        outputStream.write(data);
        outputStream.flush();

        verify(ossClient).uploadPart(any(UploadPartRequest.class));
    }
}
