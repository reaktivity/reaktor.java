/**
 * Copyright 2016-2019 The Reaktivity Project
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
package org.reaktivity.k3po.nukleus.ext.internal;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

import org.reaktivity.nukleus.Configuration;

public final class NukleusExtConfiguration extends Configuration
{
    public static final PropertyDef<String> NUKLEUS_EXT_DIRECTORY;
    public static final IntPropertyDef NUKLEUS_EXT_STREAMS_BUFFER_CAPACITY;
    public static final IntPropertyDef NUKLEUS_EXT_COMMAND_BUFFER_CAPACITY;
    public static final IntPropertyDef NUKLEUS_EXT_RESPONSE_BUFFER_CAPACITY;
    public static final IntPropertyDef NUKLEUS_EXT_COUNTERS_BUFFER_CAPACITY;
    public static final IntPropertyDef NUKLEUS_EXT_MAXIMUM_MESSAGES_PER_READ;
    public static final LongPropertyDef NUKLEUS_EXT_BACKOFF_MAX_SPINS;
    public static final LongPropertyDef NUKLEUS_EXT_BACKOFF_MAX_YIELDS;
    public static final LongPropertyDef NUKLEUS_EXT_BACKOFF_MIN_PARK_NANOS;
    public static final LongPropertyDef NUKLEUS_EXT_BACKOFF_MAX_PARK_NANOS;

    private static final ConfigurationDef NUKLEUS_EXT_CONFIG;

    static
    {
        final ConfigurationDef config = new ConfigurationDef("k3po.nukleus.ext");
        NUKLEUS_EXT_DIRECTORY = config.property("directory", "target/nukleus-itests");
        NUKLEUS_EXT_STREAMS_BUFFER_CAPACITY = config.property("streams.buffer.capacity", 1024 * 1024);
        NUKLEUS_EXT_COMMAND_BUFFER_CAPACITY = config.property("command.buffer.capacity", 1024 * 1024);
        NUKLEUS_EXT_RESPONSE_BUFFER_CAPACITY = config.property("response.buffer.capacity", 1024 * 1024);
        NUKLEUS_EXT_COUNTERS_BUFFER_CAPACITY = config.property("counters.buffer.capacity", 1024 * 1024);
        NUKLEUS_EXT_MAXIMUM_MESSAGES_PER_READ = config.property("maximum.messages.per.read", Integer.MAX_VALUE);
        NUKLEUS_EXT_BACKOFF_MAX_SPINS = config.property("backoff.idle.strategy.max.spins", 64L);
        NUKLEUS_EXT_BACKOFF_MAX_YIELDS = config.property("backoff.idle.strategy.max.yields", 64L);
        // TODO: shorten property name string values to match constant naming
        NUKLEUS_EXT_BACKOFF_MIN_PARK_NANOS = config.property("backoff.idle.strategy.min.park.period", NANOSECONDS.toNanos(64L));
        NUKLEUS_EXT_BACKOFF_MAX_PARK_NANOS = config.property("backoff.idle.strategy.max.park.period", MILLISECONDS.toNanos(1L));
        NUKLEUS_EXT_CONFIG = config;
    }

    public NukleusExtConfiguration()
    {
        super();
    }

    public NukleusExtConfiguration(
        Configuration config)
    {
        super(NUKLEUS_EXT_CONFIG, config);
    }

    public NukleusExtConfiguration(
        Properties properties)
    {
        super(NUKLEUS_EXT_CONFIG, properties);
    }

    public NukleusExtConfiguration(
        Configuration config,
        Properties defaultOverrides)
    {
        super(NUKLEUS_EXT_CONFIG, config, defaultOverrides);
    }

    @Override
    public Path directory()
    {
        return Paths.get(NUKLEUS_EXT_DIRECTORY.get(this));
    }

    public int maximumMessagesPerRead()
    {
        return NUKLEUS_EXT_MAXIMUM_MESSAGES_PER_READ.getAsInt(this);
    }

    public int streamsBufferCapacity()
    {
        return NUKLEUS_EXT_STREAMS_BUFFER_CAPACITY.getAsInt(this);
    }

    public int commandBufferCapacity()
    {
        return NUKLEUS_EXT_COMMAND_BUFFER_CAPACITY.get(this);
    }

    public int responseBufferCapacity()
    {
        return NUKLEUS_EXT_RESPONSE_BUFFER_CAPACITY.getAsInt(this);
    }

    public int counterValuesBufferCapacity()
    {
        return NUKLEUS_EXT_COUNTERS_BUFFER_CAPACITY.getAsInt(this);
    }

    public int counterLabelsBufferCapacity()
    {
        return NUKLEUS_EXT_COUNTERS_BUFFER_CAPACITY.getAsInt(this) * 2;
    }

    public long maxSpins()
    {
        return NUKLEUS_EXT_BACKOFF_MAX_SPINS.getAsLong(this);
    }

    public long maxYields()
    {
        return NUKLEUS_EXT_BACKOFF_MAX_YIELDS.getAsLong(this);
    }

    public long minParkNanos()
    {
        return NUKLEUS_EXT_BACKOFF_MIN_PARK_NANOS.getAsLong(this);
    }

    public long maxParkNanos()
    {
        return NUKLEUS_EXT_BACKOFF_MAX_PARK_NANOS.getAsLong(this);
    }
}
