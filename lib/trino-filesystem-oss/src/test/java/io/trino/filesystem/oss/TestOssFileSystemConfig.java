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

import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import jakarta.validation.constraints.AssertTrue;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.airlift.testing.ValidationAssertions.assertFailsValidation;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestOssFileSystemConfig
{
    @Test
    void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(OssFileSystemConfig.class)
                .setReadBlockSize(DataSize.of(2, MEGABYTE))
                .setWriteBlockSize(DataSize.of(16, MEGABYTE))
                .setPageSize(100)
                .setBatchSize(100)
                .setProjectId(null)
                .setEndpoint(Optional.empty())
                .setUseOssAccessToken(false)
                .setJsonKey(null)
                .setJsonKeyFilePath(null)
                .setMaxRetries(20)
                .setBackoffScaleFactor(3.0)
                .setMaxRetryTime(new Duration(25, SECONDS))
                .setMinBackoffDelay(new Duration(10, MILLISECONDS))
                .setMaxBackoffDelay(new Duration(2000, MILLISECONDS))
                .setApplicationId("Trino"));
    }

    @Test
    void testExplicitPropertyMappings()
            throws IOException
    {
        Path jsonKeyFile = Files.createTempFile(null, null);

        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("oss.read-block-size", "51MB")
                .put("oss.write-block-size", "52MB")
                .put("oss.page-size", "10")
                .put("oss.batch-size", "11")
                .put("oss.project-id", "project")
                .put("oss.endpoint", "http://custom.dns.org:8000")
                .put("oss.use-access-token", "true")
                .put("oss.json-key", "{}")
                .put("oss.json-key-file-path", jsonKeyFile.toString())
                .put("oss.client.max-retries", "10")
                .put("oss.client.backoff-scale-factor", "4.0")
                .put("oss.client.max-retry-time", "10s")
                .put("oss.client.min-backoff-delay", "20ms")
                .put("oss.client.max-backoff-delay", "20ms")
                .put("oss.application-id", "application id")
                .buildOrThrow();

        OssFileSystemConfig expected = new OssFileSystemConfig()
                .setReadBlockSize(DataSize.of(51, MEGABYTE))
                .setWriteBlockSize(DataSize.of(52, MEGABYTE))
                .setPageSize(10)
                .setBatchSize(11)
                .setProjectId("project")
                .setEndpoint(Optional.of("http://custom.dns.org:8000"))
                .setUseOssAccessToken(true)
                .setJsonKey("{}")
                .setJsonKeyFilePath(jsonKeyFile.toString())
                .setMaxRetries(10)
                .setBackoffScaleFactor(4.0)
                .setMaxRetryTime(new Duration(10, SECONDS))
                .setMinBackoffDelay(new Duration(20, MILLISECONDS))
                .setMaxBackoffDelay(new Duration(20, MILLISECONDS))
                .setApplicationId("application id");
        assertFullMapping(properties, expected);
    }

    @Test
    public void testValidation()
    {
        assertThatThrownBy(
                new OssFileSystemConfig()
                        .setUseOssAccessToken(true)
                        .setJsonKey("{}}")::validate)
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Cannot specify 'oss.json-key' when 'oss.use-access-token' is set");

        assertThatThrownBy(
                new OssFileSystemConfig()
                        .setUseOssAccessToken(true)
                        .setJsonKeyFilePath("/dev/null")::validate)
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Cannot specify 'oss.json-key-file-path' when 'oss.use-access-token' is set");

        assertThatThrownBy(
                new OssFileSystemConfig()
                        .setJsonKey("{}")
                        .setJsonKeyFilePath("/dev/null")::validate)
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("'oss.json-key' and 'oss.json-key-file-path' cannot be both set");

        assertFailsValidation(
                new OssFileSystemConfig()
                        .setJsonKey("{}")
                        .setMinBackoffDelay(new Duration(20, MILLISECONDS))
                        .setMaxBackoffDelay(new Duration(19, MILLISECONDS)),
                "retryDelayValid",
                "oss.client.min-backoff-delay must be less than or equal to oss.client.max-backoff-delay",
                AssertTrue.class);
    }
}
