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
package io.trino.spooling.filesystem.encryption;

import io.trino.filesystem.encryption.EncryptionKey;

import java.util.List;
import java.util.Map;

public class OssEncryptionHeadersTranslator
        implements EncryptionHeadersTranslator
{
    @Override
    public EncryptionKey extractKey(Map<String, List<String>> headers)
    {
        throw new UnsupportedOperationException("extractKey: not implemented");
    }

    @Override
    public Map<String, List<String>> createHeaders(EncryptionKey encryption)
    {
        throw new UnsupportedOperationException("createHeaders: not implemented");
    }
}
