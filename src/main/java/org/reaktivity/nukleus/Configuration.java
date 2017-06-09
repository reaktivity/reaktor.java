/**
 * Copyright 2016-2017 The Reaktivity Project
 *
 * The Reaktivity Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.reaktivity.nukleus;

import static java.util.Objects.requireNonNull;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.function.BiFunction;

public class Configuration
{
    public static final String DIRECTORY_PROPERTY_NAME = "nuklei.directory";

    public static final String MAXIMUM_STREAMS_COUNT_PROPERTY_NAME = "nuklei.maximum.streams.count";

    public static final String STREAMS_BUFFER_CAPACITY_PROPERTY_NAME = "nuklei.streams.buffer.capacity";

    public static final String THROTTLE_BUFFER_CAPACITY_PROPERTY_NAME = "nuklei.throttle.buffer.capacity";

    public static final String COMMAND_BUFFER_CAPACITY_PROPERTY_NAME = "nuklei.command.buffer.capacity";

    public static final String RESPONSE_BUFFER_CAPACITY_PROPERTY_NAME = "nuklei.response.buffer.capacity";

    public static final String COUNTERS_BUFFER_CAPACITY_PROPERTY_NAME = "nuklei.counters.buffer.capacity";

    public static final int MAXIMUM_STREAMS_COUNT_DEFAULT = 64;

    public static final int STREAMS_BUFFER_CAPACITY_DEFAULT = 1024 * 1024;

    public static final int THROTTLE_BUFFER_CAPACITY_DEFAULT = 64 * 1024;

    public static final int COMMAND_BUFFER_CAPACITY_DEFAULT = 1024 * 1024;

    public static final int RESPONSE_BUFFER_CAPACITY_DEFAULT = 1024 * 1024;

    public static final int COUNTERS_BUFFER_CAPACITY_DEFAULT = 1024 * 1024;

    private final BiFunction<String, String, String> getProperty;

    public Configuration()
    {
        this.getProperty = System::getProperty;
    }

    public Configuration(
        Properties properties)
    {
        requireNonNull(properties, "properties");
        this.getProperty = properties::getProperty;
    }

    public Path directory()
    {
        return Paths.get(getProperty(DIRECTORY_PROPERTY_NAME, "./"));
    }

    public int maximumStreamsCount()
    {
        return getInteger(MAXIMUM_STREAMS_COUNT_PROPERTY_NAME, MAXIMUM_STREAMS_COUNT_DEFAULT);
    }

    public int streamsBufferCapacity()
    {
        return getInteger(STREAMS_BUFFER_CAPACITY_PROPERTY_NAME, STREAMS_BUFFER_CAPACITY_DEFAULT);
    }

    public int throttleBufferCapacity()
    {
        return getInteger(THROTTLE_BUFFER_CAPACITY_PROPERTY_NAME, THROTTLE_BUFFER_CAPACITY_DEFAULT);
    }

    public int commandBufferCapacity()
    {
        return getInteger(COMMAND_BUFFER_CAPACITY_PROPERTY_NAME, COMMAND_BUFFER_CAPACITY_DEFAULT);
    }

    public int responseBufferCapacity()
    {
        return getInteger(RESPONSE_BUFFER_CAPACITY_PROPERTY_NAME, RESPONSE_BUFFER_CAPACITY_DEFAULT);
    }

    public int counterValuesBufferCapacity()
    {
        return getInteger(COUNTERS_BUFFER_CAPACITY_PROPERTY_NAME, COUNTERS_BUFFER_CAPACITY_DEFAULT);
    }

    public int counterLabelsBufferCapacity()
    {
        return getInteger(COUNTERS_BUFFER_CAPACITY_PROPERTY_NAME, COUNTERS_BUFFER_CAPACITY_DEFAULT) * 2;
    }

    private String getProperty(String key, String defaultValue)
    {
        return getProperty.apply(key, defaultValue);
    }

    private int getInteger(String key, int defaultValue)
    {
        String value = getProperty.apply(key, null);
        if (value == null)
        {
            return defaultValue;
        }
        return Integer.decode(value);
    }
}
