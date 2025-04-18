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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TestOssOutputFile
{
    private static final String TEST_BUCKET = "test-bucket";
    private static final String TEST_KEY = "test-key";

    @Mock
    private OSS ossClient;
    private OssLocation location;
    private OssOutputFile outputFile;

    @BeforeEach
    void setUp()
            throws Exception
    {
        MockitoAnnotations.openMocks(this);
        URI uri = new URI("oss://" + TEST_BUCKET + "/" + TEST_KEY);
        location = new OssLocation(Location.of(uri.toString()));
    }

    @Test
    void testCreate()
            throws IOException
    {
        outputFile = new OssOutputFile(location, ossClient);
        OutputStream stream = outputFile.create();
        assertThat(stream).isInstanceOf(OssOutputStream.class);
    }

    @Test
    void testCreateOrOverwrite()
            throws IOException
    {
        byte[] content = "test content".getBytes(StandardCharsets.UTF_8);
        outputFile = new OssOutputFile(location, ossClient);
        outputFile.createOrOverwrite(content);

        // Verify that putObject was called with correct parameters
        verify(ossClient).putObject(eq(TEST_BUCKET), eq(TEST_KEY), any(ByteArrayInputStream.class), any(ObjectMetadata.class));
    }

    @Test
    void testCreateExclusive()
            throws IOException
    {
        byte[] content = "test content".getBytes(StandardCharsets.UTF_8);
        when(ossClient.doesObjectExist(TEST_BUCKET, TEST_KEY)).thenReturn(false);

        outputFile = new OssOutputFile(location, ossClient);
        outputFile.createExclusive(content);

        // Verify that putObject was called with correct parameters
        verify(ossClient).putObject(eq(TEST_BUCKET), eq(TEST_KEY), any(ByteArrayInputStream.class), any(ObjectMetadata.class));
    }

    @Test
    void testCreateExclusiveFileExists()
    {
        when(ossClient.doesObjectExist(TEST_BUCKET, TEST_KEY)).thenReturn(true);

        outputFile = new OssOutputFile(location, ossClient);
        byte[] content = "test content".getBytes(StandardCharsets.UTF_8);
        assertThatThrownBy(() -> outputFile.createExclusive(content))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Failed to write file")
                .hasCauseInstanceOf(IOException.class)
                .hasRootCauseMessage("File already exists: " + location);
    }
}
