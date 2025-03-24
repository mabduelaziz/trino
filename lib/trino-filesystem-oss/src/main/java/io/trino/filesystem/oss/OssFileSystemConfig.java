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
import io.airlift.configuration.ConfigSecuritySensitive;
import io.airlift.configuration.validation.FileExists;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;
import jakarta.annotation.Nullable;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkState;
import static io.airlift.units.DataSize.Unit.MEGABYTE;

public class OssFileSystemConfig
{
    private DataSize readBlockSize = DataSize.of(2, MEGABYTE);
    private DataSize writeBlockSize = DataSize.of(16, MEGABYTE);
    private int pageSize = 100;
    private int batchSize = 100;

    private String projectId;
    private Optional<String> endpoint = Optional.empty();

    private boolean useOssAccessToken;
    private String jsonKey;
    private String jsonKeyFilePath;
    private int maxRetries = 20;
    private double backoffScaleFactor = 3.0;
    private Duration maxRetryTime = new Duration(25, TimeUnit.SECONDS);
    private Duration minBackoffDelay = new Duration(10, TimeUnit.MILLISECONDS);
    // Note: there is no benefit to setting this much higher as the rpc quota is 1x per second: https://cloud.google.com/storage/docs/retry-strategy#java
    private Duration maxBackoffDelay = new Duration(2000, TimeUnit.MILLISECONDS);
    private String applicationId = "Trino";

    @NotNull
    public DataSize getReadBlockSize()
    {
        return readBlockSize;
    }

    @Config("oss.read-block-size")
    @ConfigDescription("Minimum size that will be read in one RPC. The default size is 2MiB, see com.google.cloud.BaseStorageReadChannel.")
    public OssFileSystemConfig setReadBlockSize(DataSize readBlockSize)
    {
        this.readBlockSize = readBlockSize;
        return this;
    }

    @NotNull
    public DataSize getWriteBlockSize()
    {
        return writeBlockSize;
    }

    @Config("oss.write-block-size")
    @ConfigDescription("Minimum size that will be written in one RPC. The default size is 16MiB, see com.google.cloud.BaseStorageWriteChannel.")
    public OssFileSystemConfig setWriteBlockSize(DataSize writeBlockSize)
    {
        this.writeBlockSize = writeBlockSize;
        return this;
    }

    @Min(1)
    public int getPageSize()
    {
        return pageSize;
    }

    @Config("oss.page-size")
    @ConfigDescription("The maximum number of blobs to return per page.")
    public OssFileSystemConfig setPageSize(int pageSize)
    {
        this.pageSize = pageSize;
        return this;
    }

    @Min(1)
    public int getBatchSize()
    {
        return batchSize;
    }

    @Config("oss.batch-size")
    @ConfigDescription("Number of blobs to delete per batch. Recommended batch size is 100: https://cloud.google.com/storage/docs/batch")
    public OssFileSystemConfig setBatchSize(int batchSize)
    {
        this.batchSize = batchSize;
        return this;
    }

    @Nullable
    public String getProjectId()
    {
        return projectId;
    }

    @Config("oss.project-id")
    public OssFileSystemConfig setProjectId(String projectId)
    {
        this.projectId = projectId;
        return this;
    }

    public Optional<String> getEndpoint()
    {
        return endpoint;
    }

    @ConfigDescription("Endpoint to use for OSS requests")
    @Config("oss.endpoint")
    public OssFileSystemConfig setEndpoint(Optional<String> endpoint)
    {
        this.endpoint = endpoint;
        return this;
    }

    public boolean isUseOssAccessToken()
    {
        return useOssAccessToken;
    }

    @Config("oss.use-access-token")
    public OssFileSystemConfig setUseOssAccessToken(boolean useOssAccessToken)
    {
        this.useOssAccessToken = useOssAccessToken;
        return this;
    }

    @Nullable
    public String getJsonKey()
    {
        return jsonKey;
    }

    @Config("oss.json-key")
    @ConfigSecuritySensitive
    public OssFileSystemConfig setJsonKey(String jsonKey)
    {
        this.jsonKey = jsonKey;
        return this;
    }

    @Nullable
    @FileExists
    public String getJsonKeyFilePath()
    {
        return jsonKeyFilePath;
    }

    @Config("oss.json-key-file-path")
    @ConfigDescription("JSON key file used to access Google Cloud Storage")
    public OssFileSystemConfig setJsonKeyFilePath(String jsonKeyFilePath)
    {
        this.jsonKeyFilePath = jsonKeyFilePath;
        return this;
    }

    @Min(0)
    public int getMaxRetries()
    {
        return maxRetries;
    }

    @Config("oss.client.max-retries")
    @ConfigDescription("Maximum number of RPC attempts")
    public OssFileSystemConfig setMaxRetries(int maxRetries)
    {
        this.maxRetries = maxRetries;
        return this;
    }

    @Min(1)
    public double getBackoffScaleFactor()
    {
        return backoffScaleFactor;
    }

    @Config("oss.client.backoff-scale-factor")
    @ConfigDescription("Scale factor for RPC retry delay")
    public OssFileSystemConfig setBackoffScaleFactor(double backoffScaleFactor)
    {
        this.backoffScaleFactor = backoffScaleFactor;
        return this;
    }

    @NotNull
    public Duration getMaxRetryTime()
    {
        return maxRetryTime;
    }

    @Config("oss.client.max-retry-time")
    @ConfigDescription("Total time limit for an RPC to be retried")
    public OssFileSystemConfig setMaxRetryTime(Duration maxRetryTime)
    {
        this.maxRetryTime = maxRetryTime;
        return this;
    }

    @NotNull
    @MinDuration("0ms")
    public Duration getMinBackoffDelay()
    {
        return minBackoffDelay;
    }

    @Config("oss.client.min-backoff-delay")
    @ConfigDescription("Minimum delay between RPC retries")
    public OssFileSystemConfig setMinBackoffDelay(Duration minBackoffDelay)
    {
        this.minBackoffDelay = minBackoffDelay;
        return this;
    }

    @NotNull
    @MinDuration("0ms")
    public Duration getMaxBackoffDelay()
    {
        return maxBackoffDelay;
    }

    @Config("oss.client.max-backoff-delay")
    @ConfigDescription("Maximum delay between RPC retries.")
    public OssFileSystemConfig setMaxBackoffDelay(Duration maxBackoffDelay)
    {
        this.maxBackoffDelay = maxBackoffDelay;
        return this;
    }

    @Size(max = 50)
    @NotNull
    public String getApplicationId()
    {
        return applicationId;
    }

    @Config("oss.application-id")
    @ConfigDescription("Suffix that will be added to HTTP User-Agent header to identify the application")
    public OssFileSystemConfig setApplicationId(String applicationId)
    {
        this.applicationId = applicationId;
        return this;
    }

    @AssertTrue(message = "oss.client.min-backoff-delay must be less than or equal to oss.client.max-backoff-delay")
    public boolean isRetryDelayValid()
    {
        return minBackoffDelay.compareTo(maxBackoffDelay) <= 0;
    }

    public void validate()
    {
        // This cannot be normal validation, as it would make it impossible to write TestOssFileSystemConfig.testExplicitPropertyMappings

        if (useOssAccessToken) {
            checkState(jsonKey == null, "Cannot specify 'oss.json-key' when 'oss.use-access-token' is set");
            checkState(jsonKeyFilePath == null, "Cannot specify 'oss.json-key-file-path' when 'oss.use-access-token' is set");
        }
        checkState(jsonKey == null || jsonKeyFilePath == null, "'oss.json-key' and 'oss.json-key-file-path' cannot be both set");
    }
}
