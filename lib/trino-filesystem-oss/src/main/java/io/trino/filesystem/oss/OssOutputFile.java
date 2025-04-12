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

import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoOutputFile;
import io.trino.memory.context.AggregatedMemoryContext;

import java.io.IOException;
import java.io.OutputStream;

public class OssOutputFile
        implements TrinoOutputFile
{
    @Override
    public OutputStream create()
            throws IOException
    {
        throw new UnsupportedOperationException("create not supported");
    }

    @Override
    public void createOrOverwrite(byte[] data)
            throws IOException
    {
        throw new UnsupportedOperationException("createOrOverwrite not supported");
    }

    @Override
    public void createExclusive(byte[] data)
            throws IOException
    {
        throw new UnsupportedOperationException("createExclusive not supported");
    }

    @Override
    public OutputStream create(AggregatedMemoryContext memoryContext)
            throws IOException
    {
        throw new UnsupportedOperationException("create with memoryContext not supported");
    }

    @Override
    public Location location()
    {
        throw new UnsupportedOperationException("location not supported");
    }
}
