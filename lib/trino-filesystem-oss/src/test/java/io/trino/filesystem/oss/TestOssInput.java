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
import com.aliyun.oss.model.ObjectMetadata;
import io.trino.filesystem.Location;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;

class TestOssInput
{
    private static final String TEST_BUCKET = "test-bucket";
    private static final String TEST_KEY = "test-key";

    @Mock
    private OSS ossClient;
    @Mock
    private OSSObject ossObject;
    private Location location;
    private String bucket;
    private String key;
    private OssInput input;

    @BeforeEach
    void setUp()
            throws Exception
    {
        MockitoAnnotations.openMocks(this);
        URI uri = new URI("oss://" + TEST_BUCKET + "/" + TEST_KEY);
        location = Location.of(uri.toString());
        bucket = "test-bucket";
        key = "test-key";
    }

    @Test
    void testReadFully()
            throws IOException
    {
        byte[] testData = "test data".getBytes(StandardCharsets.UTF_8);
        ByteArrayInputStream byteStream = new ByteArrayInputStream(testData);

        when(ossClient.getObject(bucket, key)).thenReturn(ossObject);
        when(ossObject.getObjectContent()).thenReturn(byteStream);

        input = new OssInput(location, ossClient, bucket, key);
        byte[] buffer = new byte[testData.length];
        input.readFully(0, buffer, 0, buffer.length);
    }

    @Test
    void testReadTail()
            throws IOException
    {
        byte[] testData = "test data".getBytes(StandardCharsets.UTF_8);
        ByteArrayInputStream byteStream = new ByteArrayInputStream(testData);
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(testData.length);

        when(ossClient.getObject(bucket, key)).thenReturn(ossObject);
        when(ossObject.getObjectContent()).thenReturn(byteStream);
        when(ossObject.getObjectMetadata()).thenReturn(metadata);

        input = new OssInput(location, ossClient, bucket, key);
        byte[] buffer = new byte[4];
        input.readTail(buffer, 0, buffer.length);
    }

    @Test
    void testReadFullyError()
    {
        when(ossClient.getObject(bucket, key)).thenThrow(new RuntimeException("Test error"));

        input = new OssInput(location, ossClient, bucket, key);
        byte[] buffer = new byte[10];
        assertThatThrownBy(() -> input.readFully(0, buffer, 0, buffer.length))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Failed to read file");
    }

    @Test
    void testReadTailError()
    {
        when(ossClient.getObject(bucket, key)).thenThrow(new RuntimeException("Test error"));

        input = new OssInput(location, ossClient, bucket, key);
        byte[] buffer = new byte[10];
        assertThatThrownBy(() -> input.readTail(buffer, 0, buffer.length))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Failed to read tail of file");
    }

    @Test
    void testReadSuccessfully()
            throws IOException
    {
        byte[] testData = "test data".getBytes(StandardCharsets.UTF_8);
        ByteArrayInputStream byteStream = new ByteArrayInputStream(testData);

        when(ossClient.getObject(bucket, key)).thenReturn(ossObject);
        when(ossObject.getObjectContent()).thenReturn(byteStream);

        input = new OssInput(location, ossClient, bucket, key);
        byte[] buffer = new byte[testData.length];
        input.readFully(0, buffer, 0, buffer.length);
        assertThat(buffer).isEqualTo(testData);
    }

    @Test
    void testReadWithOffset()
            throws IOException
    {
        byte[] testData = "test data".getBytes(StandardCharsets.UTF_8);
        ByteArrayInputStream byteStream = new ByteArrayInputStream(testData);

        when(ossClient.getObject(bucket, key)).thenReturn(ossObject);
        when(ossObject.getObjectContent()).thenReturn(byteStream);

        input = new OssInput(location, ossClient, bucket, key);
        byte[] buffer = new byte[4];
        input.readFully(0, buffer, 1, 3);
        assertThat(buffer[1]).isEqualTo(testData[0]);
        assertThat(buffer[2]).isEqualTo(testData[1]);
        assertThat(buffer[3]).isEqualTo(testData[2]);
    }
}
