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

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;
import jakarta.validation.constraints.NotNull;

public class ExchangeOssConfig
{
    private String endpoint;
    private String accessKey;
    private String accessSecretKey;
    private String bucketName;

    @NotNull
    public String getEndpoint()
    {
        return endpoint;
    }

    @Config("exchange.oss.endpoint")
    @ConfigDescription("OSS endpoint")
    public ExchangeOssConfig setEndpoint(String endpoint)
    {
        this.endpoint = endpoint;
        return this;
    }

    @NotNull
    public String getAccessKey()
    {
        return accessKey;
    }

    @Config("exchange.oss.access-key")
    @ConfigDescription("OSS access key")
    public ExchangeOssConfig setAccessKey(String accessKey)
    {
        this.accessKey = accessKey;
        return this;
    }

    @NotNull
    public String getAccessSecretKey()
    {
        return accessSecretKey;
    }

    @Config("exchange.oss.access-secret-key")
    @ConfigDescription("OSS access secret key")
    @ConfigSecuritySensitive
    public ExchangeOssConfig setAccessSecretKey(String accessSecretKey)
    {
        this.accessSecretKey = accessSecretKey;
        return this;
    }

    @NotNull
    public String getBucketName()
    {
        return bucketName;
    }

    @Config("exchange.oss.bucket-name")
    @ConfigDescription("OSS bucket name")
    public ExchangeOssConfig setBucketName(String bucketName)
    {
        this.bucketName = bucketName;
        return this;
    }
}
