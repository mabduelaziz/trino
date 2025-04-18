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
import io.trino.filesystem.TrinoInput;
import io.trino.filesystem.TrinoInputStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.net.URI;
import java.time.Instant;
import java.util.Optional;
import java.util.OptionalLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;

class TestOssInputFile
{
    private static final long TEST_LENGTH = 1234L;
    private static final Instant TEST_LAST_MODIFIED = Instant.parse("2025-04-16T06:04:25.114Z");
    private static final String TEST_BUCKET = "test-bucket";
    private static final String TEST_KEY = "test-key";

    @Mock
    private OSS ossClient;
    @Mock
    private OSSObject ossObject;
    private OssLocation location;
    private OssLocation ossLocation;
    private OssInputFile inputFile;

    @BeforeEach
    void setUp()
            throws Exception
    {
        MockitoAnnotations.openMocks(this);
        URI uri = new URI("oss://" + TEST_BUCKET + "/" + TEST_KEY);
        location = new OssLocation(Location.of(uri.toString()));
    }

    @Test
    void testExists()
            throws IOException
    {
        when(ossClient.doesObjectExist(location.bucket(), location.key())).thenReturn(true);
        inputFile = new OssInputFile(location, ossClient, OptionalLong.empty(), Optional.empty());
        assertThat(inputFile.exists()).isTrue();
    }

    @Test
    void testNotExists()
            throws IOException
    {
        when(ossClient.doesObjectExist(location.bucket(), location.key())).thenReturn(false);

        inputFile = new OssInputFile(location, ossClient, OptionalLong.empty(), Optional.empty());
        assertThat(inputFile.exists()).isFalse();
    }

    @Test
    void testExistsError()
    {
        when(ossClient.doesObjectExist(location.bucket(), location.key())).thenThrow(new RuntimeException("Test error"));

        inputFile = new OssInputFile(location, ossClient, OptionalLong.empty(), Optional.empty());
        assertThatThrownBy(inputFile::exists)
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Failed to check existence for file");
    }

    @Test
    void testLength()
            throws IOException
    {
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(TEST_LENGTH);
        when(ossClient.getObject(location.bucket(), location.key())).thenReturn(ossObject);
        when(ossObject.getObjectMetadata()).thenReturn(metadata);

        inputFile = new OssInputFile(location, ossClient, OptionalLong.empty(), Optional.empty());
        assertThat(inputFile.length()).isEqualTo(TEST_LENGTH);
    }

    @Test
    void testLengthError()
    {
        when(ossClient.getObject(location.bucket(), location.key())).thenThrow(new RuntimeException("Test error"));

        inputFile = new OssInputFile(location, ossClient, OptionalLong.empty(), Optional.empty());
        assertThatThrownBy(inputFile::length)
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Failed to get length for file");
    }

    @Test
    void testLastModified()
            throws IOException
    {
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setLastModified(java.util.Date.from(TEST_LAST_MODIFIED));
        when(ossClient.getObject(location.bucket(), location.key())).thenReturn(ossObject);
        when(ossObject.getObjectMetadata()).thenReturn(metadata);

        inputFile = new OssInputFile(location, ossClient, OptionalLong.empty(), Optional.empty());
        assertThat(inputFile.lastModified()).isEqualTo(TEST_LAST_MODIFIED);
    }

    @Test
    void testLastModifiedError()
    {
        when(ossClient.getObject(location.bucket(), location.key())).thenThrow(new RuntimeException("Test error"));

        inputFile = new OssInputFile(location, ossClient, OptionalLong.empty(), Optional.empty());
        assertThatThrownBy(inputFile::lastModified)
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Failed to get last modified time for file");
    }

    @Test
    void testNewInput()
            throws IOException
    {
        inputFile = new OssInputFile(location, ossClient, OptionalLong.empty(), Optional.empty());
        TrinoInput input = inputFile.newInput();
        assertThat(input).isInstanceOf(OssInput.class);
    }

    @Test
    void testNewStream()
            throws IOException
    {
        inputFile = new OssInputFile(location, ossClient, OptionalLong.empty(), Optional.empty());
        TrinoInputStream stream = inputFile.newStream();
        assertThat(stream).isInstanceOf(OssInputStream.class);
    }
}
