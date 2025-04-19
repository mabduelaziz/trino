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

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import jakarta.validation.constraints.NotNull;

public class OssFileSystemConfig
{
    private String endpoint;
    private String accessKeyId;
    private String accessKeySecret;
    private String region;

    @NotNull
    public String getEndpoint()
    {
        return endpoint;
    }

    @Config("oss.endpoint")
    @ConfigDescription("OSS service endpoint")
    public OssFileSystemConfig setEndpoint(String endpoint)
    {
        this.endpoint = endpoint;
        return this;
    }

    @NotNull
    public String getAccessKeyId()
    {
        return accessKeyId;
    }

    @Config("oss.access-key-id")
    @ConfigDescription("OSS access key ID")
    public OssFileSystemConfig setAccessKeyId(String accessKeyId)
    {
        this.accessKeyId = accessKeyId;
        return this;
    }

    @NotNull
    public String getAccessKeySecret()
    {
        return accessKeySecret;
    }

    @Config("oss.access-key-secret")
    @ConfigDescription("OSS access key secret")
    public OssFileSystemConfig setAccessKeySecret(String accessKeySecret)
    {
        this.accessKeySecret = accessKeySecret;
        return this;
    }

    public String getRegion()
    {
        return region;
    }

    @Config("oss.region")
    @ConfigDescription("OSS region")
    public OssFileSystemConfig setRegion(String region)
    {
        this.region = region;
        return this;
    }
}
