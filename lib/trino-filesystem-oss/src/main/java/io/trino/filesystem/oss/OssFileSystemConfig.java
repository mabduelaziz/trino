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
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.concurrent.TimeUnit.SECONDS;

public class OssFileSystemConfig
{
    private String endpoint;
    private String accessKeyId;
    private String accessKeySecret;
    private String region;
    private Duration connectTimeout = new Duration(10, SECONDS);
    private Duration socketTimeout = new Duration(10, SECONDS);
    private Duration maxErrorRetries = new Duration(3, SECONDS);
    private DataSize multipartUploadMinFileSize = DataSize.of(16, MEGABYTE);
    private DataSize multipartUploadPartSize = DataSize.of(16, MEGABYTE);
    private int maxConnections = 500;

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

    @NotNull
    public Duration getConnectTimeout()
    {
        return connectTimeout;
    }

    @Config("oss.connect-timeout")
    @ConfigDescription("OSS client connection timeout")
    public OssFileSystemConfig setConnectTimeout(Duration connectTimeout)
    {
        this.connectTimeout = connectTimeout;
        return this;
    }

    @NotNull
    public Duration getSocketTimeout()
    {
        return socketTimeout;
    }

    @Config("oss.socket-timeout")
    @ConfigDescription("OSS client socket timeout")
    public OssFileSystemConfig setSocketTimeout(Duration socketTimeout)
    {
        this.socketTimeout = socketTimeout;
        return this;
    }

    @NotNull
    public Duration getMaxErrorRetries()
    {
        return maxErrorRetries;
    }

    @Config("oss.max-error-retries")
    @ConfigDescription("Maximum number of error retries")
    public OssFileSystemConfig setMaxErrorRetries(Duration maxErrorRetries)
    {
        this.maxErrorRetries = maxErrorRetries;
        return this;
    }

    @NotNull
    public DataSize getMultipartUploadMinFileSize()
    {
        return multipartUploadMinFileSize;
    }

    @Config("oss.multipart-upload-min-file-size")
    @ConfigDescription("Minimum file size for multipart upload")
    public OssFileSystemConfig setMultipartUploadMinFileSize(DataSize multipartUploadMinFileSize)
    {
        this.multipartUploadMinFileSize = multipartUploadMinFileSize;
        return this;
    }

    @NotNull
    public DataSize getMultipartUploadPartSize()
    {
        return multipartUploadPartSize;
    }

    @Config("oss.multipart-upload-part-size")
    @ConfigDescription("Part size for multipart upload")
    public OssFileSystemConfig setMultipartUploadPartSize(DataSize multipartUploadPartSize)
    {
        this.multipartUploadPartSize = multipartUploadPartSize;
        return this;
    }

    @Min(1)
    public int getMaxConnections()
    {
        return maxConnections;
    }

    @Config("oss.max-connections")
    @ConfigDescription("Maximum number of concurrent connections to OSS")
    public OssFileSystemConfig setMaxConnections(int maxConnections)
    {
        this.maxConnections = maxConnections;
        return this;
    }
}
