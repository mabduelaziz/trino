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
import com.aliyun.oss.OSSClientBuilder;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.spi.security.ConnectorIdentity;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestOssFileSystemFactory
{
    @Test
    public void testClientCreatedWithCorrectConfig()
    {
        // Mock config
        OssFileSystemConfig config = mock(OssFileSystemConfig.class);
        when(config.getEndpoint()).thenReturn("oss-me-central-1.aliyuncs.com");
        when(config.getAccessKeyId()).thenReturn("mock-id");
        when(config.getAccessKeySecret()).thenReturn("mock-secret");

        // Mock builder + OSS client
        OSSClientBuilder builder = mock(OSSClientBuilder.class);
        OSS mockClient = mock(OSS.class);
        when(builder.build("oss-me-central-1.aliyuncs.com", "mock-id", "mock-secret")).thenReturn(mockClient);

        // Create factory
        OssFileSystemFactory factory = new OssFileSystemFactory(config, builder);
        TrinoFileSystem trinoFileSystem = factory.create(ConnectorIdentity.ofUser("mock-user"));
        // Verify the builder was called with correct values
        assertThat(trinoFileSystem).isInstanceOf(OssFileSystem.class);
        OssFileSystem ossFileSystem = (OssFileSystem) trinoFileSystem;
        assertThat(ossFileSystem.getClient()).isSameAs(mockClient);
    }

    @Test
    public void testClientShutdownOnDestroy()
    {
        // Mock config
        OssFileSystemConfig config = mock(OssFileSystemConfig.class);
        when(config.getEndpoint()).thenReturn("oss-me-central-1.aliyuncs.com");
        when(config.getAccessKeyId()).thenReturn("mock-id");
        when(config.getAccessKeySecret()).thenReturn("mock-secret");

        // Mock builder + OSS client
        OSSClientBuilder builder = mock(OSSClientBuilder.class);
        OSS mockClient = mock(OSS.class);
        when(builder.build("oss-me-central-1.aliyuncs.com", "mock-id", "mock-secret")).thenReturn(mockClient);

        // Create factory
        OssFileSystemFactory factory = new OssFileSystemFactory(config, builder);

        // Call destroy
        factory.destroy();

        // Verify client was shut down
        verify(mockClient).shutdown();
    }

    @Test
    public void testCreateFileSystem()
    {
        // Mock config
        OssFileSystemConfig config = mock(OssFileSystemConfig.class);
        when(config.getEndpoint()).thenReturn("oss-me-central-1.aliyuncs.com");
        when(config.getAccessKeyId()).thenReturn("mock-id");
        when(config.getAccessKeySecret()).thenReturn("mock-secret");

        // Mock builder + OSS client
        OSSClientBuilder builder = mock(OSSClientBuilder.class);
        OSS mockClient = mock(OSS.class);
        when(builder.build("oss-me-central-1.aliyuncs.com", "mock-id", "mock-secret")).thenReturn(mockClient);

        // Create factory
        OssFileSystemFactory factory = new OssFileSystemFactory(config, builder);

        // Create file system with identity
        ConnectorIdentity identity = ConnectorIdentity.ofUser("test-user");
        TrinoFileSystem fileSystem = factory.create(identity);

        // Verify file system was created
        assertThat(fileSystem).isNotNull();
    }

    @Test
    public void testNullConfigThrows()
    {
        assertThatThrownBy(() -> new OssFileSystemFactory(null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    public void testNullBuilderThrows()
    {
        // Mock config
        OssFileSystemConfig config = mock(OssFileSystemConfig.class);
        when(config.getEndpoint()).thenReturn("oss-me-central-1.aliyuncs.com");
        when(config.getAccessKeyId()).thenReturn("mock-id");
        when(config.getAccessKeySecret()).thenReturn("mock-secret");

        assertThatThrownBy(() -> new OssFileSystemFactory(config, null))
                .isInstanceOf(NullPointerException.class);
    }
}
