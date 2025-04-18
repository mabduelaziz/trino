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
import com.aliyun.oss.model.DeleteObjectsRequest;
import com.aliyun.oss.model.DeleteObjectsResult;
import com.aliyun.oss.model.ListObjectsRequest;
import com.aliyun.oss.model.OSSObjectSummary;
import com.aliyun.oss.model.ObjectListing;
import com.google.common.collect.ImmutableList;
import io.trino.filesystem.FileEntry;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestOssFileSystem
{
    private static final String TEST_BUCKET = "test-bucket";
    private static final String TEST_KEY = "test/path/file.txt";
    private static final Location TEST_LOCATION = Location.of("oss://" + TEST_BUCKET + "/" + TEST_KEY);

    private OSS ossClient;
    private TrinoFileSystem fileSystem;

    @BeforeEach
    public void setup()
    {
        ossClient = mock(OSS.class);
        fileSystem = new OssFileSystem(ossClient);
    }

    @Test
    public void testDeleteFile()
            throws IOException
    {
        fileSystem.deleteFile(TEST_LOCATION);
        verify(ossClient).deleteObject(TEST_BUCKET, TEST_KEY);
    }

    @Test
    public void testDeleteFileError()
    {
        when(ossClient.deleteObject(anyString(), anyString()))
                .thenThrow(new RuntimeException("Delete failed"));

        assertThatThrownBy(() -> fileSystem.deleteFile(TEST_LOCATION))
                .isInstanceOf(TrinoFileSystemException.class)
                .hasMessageContaining("Failed to delete file");
    }

    @Test
    public void testListFiles()
            throws IOException
    {
        ObjectListing listing = mock(ObjectListing.class);
        OSSObjectSummary summary = new OSSObjectSummary();
        summary.setBucketName(TEST_BUCKET);
        summary.setKey(TEST_KEY);
        summary.setSize(1234L);
        summary.setLastModified(new Date());

        when(listing.getObjectSummaries()).thenReturn(ImmutableList.of(summary));
        when(ossClient.listObjects(any(ListObjectsRequest.class))).thenReturn(listing);

        FileIterator iterator = fileSystem.listFiles(Location.of("oss://" + TEST_BUCKET + "/test/"));
        assertThat(iterator.hasNext()).isTrue();
        FileEntry entry = iterator.next();
        assertThat(entry.location().toString()).isEqualTo("oss://" + TEST_BUCKET + "/" + TEST_KEY);
        assertThat(entry.length()).isEqualTo(1234L);
    }

    @Test
    public void testDeleteDirectory()
            throws IOException
    {
        // Setup mock listing response
        ObjectListing listing = mock(ObjectListing.class);
        List<OSSObjectSummary> summaries = new ArrayList<>();
        for (int i = 0; i < 1500; i++) { // Test batch deletion (>1000 files)
            OSSObjectSummary summary = new OSSObjectSummary();
            summary.setBucketName(TEST_BUCKET);
            summary.setKey("test/dir/file" + i + ".txt");
            summary.setLastModified(new Date());
            summaries.add(summary);
        }

        when(listing.getObjectSummaries()).thenReturn(summaries);
        when(ossClient.listObjects(any(ListObjectsRequest.class))).thenReturn(listing);

        // Setup mock deletion response
        DeleteObjectsResult deleteResult = mock(DeleteObjectsResult.class);
        when(deleteResult.getDeletedObjects()).thenReturn(ImmutableList.of());
        when(ossClient.deleteObjects(any(DeleteObjectsRequest.class))).thenReturn(deleteResult);

        // Execute
        fileSystem.deleteDirectory(Location.of("oss://" + TEST_BUCKET + "/test/dir/"));

        // Verify batch deletions were performed
        ArgumentCaptor<DeleteObjectsRequest> requestCaptor = ArgumentCaptor.forClass(DeleteObjectsRequest.class);
        verify(ossClient, times(6)).deleteObjects(requestCaptor.capture());

        // Verify each batch
        List<DeleteObjectsRequest> requests = requestCaptor.getAllValues();
        assertThat(requests).hasSize(6); // 1500 files / 250 per batch = 6 batches

        // Verify each batch size
        for (int i = 0; i < 6; i++) {
            assertThat(requests.get(i).getKeys()).hasSize(250); // First 5 batches should be full
        }
        // Verify all files are included across all batches
        Set<String> allDeletedKeys = requests.stream()
                .flatMap(req -> req.getKeys().stream())
                .collect(Collectors.toSet());
        assertThat(allDeletedKeys).hasSize(1500);
    }

    @Test
    public void testDirectoryExists()
            throws IOException
    {
        ObjectListing listing = mock(ObjectListing.class);
        OSSObjectSummary summary = new OSSObjectSummary();
        summary.setBucketName(TEST_BUCKET);
        summary.setKey("test/dir/file.txt");

        when(listing.getObjectSummaries()).thenReturn(ImmutableList.of(summary));
        when(ossClient.listObjects(any(ListObjectsRequest.class))).thenReturn(listing);

        Optional<Boolean> exists = fileSystem.directoryExists(Location.of("oss://" + TEST_BUCKET + "/test/dir/"));
        assertThat(exists).contains(true);
    }

    @Test
    public void testRenameNotSupported()
    {
        Location source = Location.of("oss://" + TEST_BUCKET + "/source.txt");
        Location target = Location.of("oss://" + TEST_BUCKET + "/target.txt");

        assertThatThrownBy(() -> fileSystem.renameFile(source, target))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("OSS does not support renames");
    }
}
