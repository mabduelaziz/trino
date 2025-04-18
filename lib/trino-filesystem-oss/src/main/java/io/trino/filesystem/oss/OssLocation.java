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

import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

record OssLocation(Location location)
{
    OssLocation
    {
        requireNonNull(location, "location is null");
        checkArgument(location.scheme().isPresent(), "No scheme for OSS location: %s", location);
        checkArgument(Objects.equals("oss", location.scheme().get()), "Wrong scheme for OSS location: %s", location);
        checkArgument(location.host().isPresent(), "No bucket for OSS location: %s", location);
        checkArgument(location.userInfo().isEmpty(), "OSS location contains user info: %s", location);
        checkArgument(location.port().isEmpty(), "OSS location contains port: %s", location);
    }

    public static OssLocation of(Location location)
    {
        return new OssLocation(location);
    }

    public String scheme()
    {
        return location.scheme().orElseThrow();
    }

    public String bucket()
    {
        return location.host().orElseThrow();
    }

    public String key()
    {
        return location.path();
    }

    @Override
    public String toString()
    {
        return location.toString();
    }

    public Location baseLocation()
    {
        return Location.of("%s://%s/".formatted(scheme(), bucket()));
    }
}
