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
import java.util.function.BooleanSupplier;
import java.util.function.IntSupplier;
import java.util.function.Supplier;

public class Configuration
{
    @Deprecated
    public static final String DIRECTORY_PROPERTY_NAME = "reaktor.directory";

    @Deprecated
    public static final String MAXIMUM_STREAMS_COUNT_PROPERTY_NAME = "reaktor.maximum.streams.count";

    @Deprecated
    public static final String STREAMS_BUFFER_CAPACITY_PROPERTY_NAME = "reaktor.streams.buffer.capacity";

    @Deprecated
    public static final String THROTTLE_BUFFER_CAPACITY_PROPERTY_NAME = "reaktor.throttle.buffer.capacity";

    @Deprecated
    public static final String COMMAND_BUFFER_CAPACITY_PROPERTY_NAME = "reaktor.command.buffer.capacity";

    @Deprecated
    public static final String RESPONSE_BUFFER_CAPACITY_PROPERTY_NAME = "reaktor.response.buffer.capacity";

    @Deprecated
    public static final String COUNTERS_BUFFER_CAPACITY_PROPERTY_NAME = "reaktor.counters.buffer.capacity";

    @Deprecated
    public static final int MAXIMUM_STREAMS_COUNT_DEFAULT = 16 * 1024;

    @Deprecated
    public static final int STREAMS_BUFFER_CAPACITY_DEFAULT = 1024 * 1024;

    @Deprecated
    public static final int THROTTLE_BUFFER_CAPACITY_DEFAULT = 64 * 1024;

    @Deprecated
    public static final int COMMAND_BUFFER_CAPACITY_DEFAULT = 1024 * 1024;

    @Deprecated
    public static final int RESPONSE_BUFFER_CAPACITY_DEFAULT = 1024 * 1024;

    @Deprecated
    public static final int COUNTERS_BUFFER_CAPACITY_DEFAULT = 1024 * 1024;

    private final BiFunction<String, String, String> getProperty;
    private final BiFunction<String, String, String> getPropertyDefault;

    public Configuration()
    {
        this.getProperty = System::getProperty;
        this.getPropertyDefault = (p, d) -> d;
    }

    public Configuration(
        Properties properties)
    {
        requireNonNull(properties, "properties");
        this.getProperty = properties::getProperty;
        this.getPropertyDefault = (p, d) -> d;
    }

    protected Configuration(
        Configuration config)
    {
        this.getProperty = config.getProperty;
        this.getPropertyDefault = config.getPropertyDefault;
    }

    protected Configuration(
        Configuration config,
        Properties defaultOverrides)
    {
        this.getProperty = config.getProperty;
        this.getPropertyDefault = defaultOverrides::getProperty;
    }

    @Deprecated
    public Path directory()
    {
        return Paths.get(getProperty(DIRECTORY_PROPERTY_NAME, "."));
    }

    @Deprecated
    public int maximumStreamsCount()
    {
        return getInteger(MAXIMUM_STREAMS_COUNT_PROPERTY_NAME, MAXIMUM_STREAMS_COUNT_DEFAULT);
    }

    @Deprecated
    public int streamsBufferCapacity()
    {
        return getInteger(STREAMS_BUFFER_CAPACITY_PROPERTY_NAME, STREAMS_BUFFER_CAPACITY_DEFAULT);
    }

    @Deprecated
    public int throttleBufferCapacity()
    {
        return getInteger(THROTTLE_BUFFER_CAPACITY_PROPERTY_NAME, THROTTLE_BUFFER_CAPACITY_DEFAULT);
    }

    @Deprecated
    public int commandBufferCapacity()
    {
        return getInteger(COMMAND_BUFFER_CAPACITY_PROPERTY_NAME, COMMAND_BUFFER_CAPACITY_DEFAULT);
    }

    @Deprecated
    public int responseBufferCapacity()
    {
        return getInteger(RESPONSE_BUFFER_CAPACITY_PROPERTY_NAME, RESPONSE_BUFFER_CAPACITY_DEFAULT);
    }

    @Deprecated
    public int counterValuesBufferCapacity()
    {
        return getInteger(COUNTERS_BUFFER_CAPACITY_PROPERTY_NAME, COUNTERS_BUFFER_CAPACITY_DEFAULT);
    }

    @Deprecated
    public int counterLabelsBufferCapacity()
    {
        return getInteger(COUNTERS_BUFFER_CAPACITY_PROPERTY_NAME, COUNTERS_BUFFER_CAPACITY_DEFAULT) * 2;
    }

    protected final int getInteger(String key, int defaultValue)
    {
        String value = getProperty(key, (String) null);
        if (value == null)
        {
            return defaultValue;
        }
        return Integer.decode(value);
    }

    protected final boolean getBoolean(String key, boolean defaultValue)
    {
        String value = getProperty(key, (String) null);
        if (value == null)
        {
            return defaultValue;
        }
        return Boolean.valueOf(value);
    }

    protected final String getProperty(String key, Supplier<String> defaultValue)
    {
        String value = getProperty(key, (String) null);
        if (value == null)
        {
            return defaultValue.get();
        }
        return value;
    }

    protected final int getInteger(String key, IntSupplier defaultValue)
    {
        String value = getProperty(key, (String) null);
        if (value == null)
        {
            return defaultValue.getAsInt();
        }
        return Integer.decode(value);
    }

    protected final boolean getBoolean(String key, BooleanSupplier defaultValue)
    {
        String value = getProperty(key, (String) null);
        if (value == null)
        {
            return defaultValue.getAsBoolean();
        }
        return Boolean.valueOf(value);
    }

    protected String getProperty(String key, String defaultValue)
    {
        return getProperty.apply(key, getPropertyDefault.apply(key, defaultValue));
    }
}
