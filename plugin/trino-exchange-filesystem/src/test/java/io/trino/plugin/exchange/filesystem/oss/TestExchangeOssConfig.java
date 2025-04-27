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
package io.trino.plugin.exchange.filesystem.oss;

import io.airlift.configuration.testing.ConfigAssertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;

public class TestExchangeOssConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(ConfigAssertions.recordDefaults(ExchangeOssConfig.class)
                .setAccessKey(null)
                .setAccessSecretKey(null)
                .setEndpoint(null)
                .setBucketName(null));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = Map.of(
                "exchange.oss.access-key", "test-access-key",
                "exchange.oss.access-secret-key", "test-secret-key",
                "exchange.oss.endpoint", "test-endpoint");

        ExchangeOssConfig expected = new ExchangeOssConfig()
                .setAccessKey("test-access-key")
                .setAccessSecretKey("test-secret-key")
                .setEndpoint("test-endpoint");

        assertFullMapping(properties, expected);
    }
}
