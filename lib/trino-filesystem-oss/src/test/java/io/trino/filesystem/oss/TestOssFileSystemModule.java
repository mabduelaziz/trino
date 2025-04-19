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

import com.google.inject.Guice;
import com.google.inject.Injector;
import io.airlift.configuration.ConfigurationFactory;
import io.airlift.configuration.ConfigurationModule;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class TestOssFileSystemModule
{
    @Test
    public void testModuleBindings()
    {
        Map<String, String> config = Map.of(
                "oss.endpoint", "oss-test-endpoint",
                "oss.access-key-id", "test-access-key-id",
                "oss.access-key-secret", "test-access-key-secret");

        Injector injector = Guice.createInjector(
                new ConfigurationModule(new ConfigurationFactory(config)),
                new OssFileSystemModule());

        // Verify OssFileSystemConfig is bound
        OssFileSystemConfig configInstance = injector.getInstance(OssFileSystemConfig.class);
        assertThat(configInstance).isNotNull();
        assertThat(configInstance.getEndpoint()).isEqualTo("oss-test-endpoint");
        assertThat(configInstance.getAccessKeyId()).isEqualTo("test-access-key-id");
        assertThat(configInstance.getAccessKeySecret()).isEqualTo("test-access-key-secret");

        // Verify OssFileSystemFactory is bound as singleton
        OssFileSystemFactory factory1 = injector.getInstance(OssFileSystemFactory.class);
        OssFileSystemFactory factory2 = injector.getInstance(OssFileSystemFactory.class);
        assertThat(factory1).isNotNull();
        assertThat(factory2).isSameAs(factory1);
    }
}
