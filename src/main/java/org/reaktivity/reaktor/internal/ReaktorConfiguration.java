/**
 * Copyright 2016-2018 The Reaktivity Project
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
package org.reaktivity.reaktor.internal;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

import org.reaktivity.nukleus.Configuration;

public class ReaktorConfiguration extends Configuration
{
    public static final String DIRECTORY_PROPERTY_NAME = "reaktor.directory";

    public static final String STREAMS_BUFFER_CAPACITY_PROPERTY_NAME = "reaktor.streams.buffer.capacity";

    public static final String THROTTLE_BUFFER_CAPACITY_PROPERTY_NAME = "reaktor.throttle.buffer.capacity";

    public static final String COMMAND_BUFFER_CAPACITY_PROPERTY_NAME = "reaktor.command.buffer.capacity";

    public static final String RESPONSE_BUFFER_CAPACITY_PROPERTY_NAME = "reaktor.response.buffer.capacity";

    public static final String COUNTERS_BUFFER_CAPACITY_PROPERTY_NAME = "reaktor.counters.buffer.capacity";

    public static final String BUFFER_POOL_CAPACITY_PROPERTY = "reaktor.buffer.pool.capacity";

    public static final String BUFFER_SLOT_CAPACITY_PROPERTY = "reaktor.buffer.slot.capacity";

    public static final String ROUTES_BUFFER_CAPACITY_PROPERTY_NAME = "reaktor.routes.buffer.capacity";

    public static final String TIMESTAMPS_PROPERTY_NAME = "reaktor.timestamps";

    public static final String BACKOFF_IDLE_STRATEGY_MAX_SPINS = "reaktor.backoff.idle.strategy.max.spins";

    public static final String BACKOFF_IDLE_STRATEGY_MAX_YIELDS = "reaktor.backoff.idle.strategy.max.yields";

    public static final String BACKOFF_IDLE_STRATEGY_MIN_PARK_PERIOD_NANOS = "reaktor.backoff.idle.strategy.min.park.period";

    public static final String BACKOFF_IDLE_STRATEGY_MAX_PARK_PERIOD_NANOS = "reaktor.backoff.idle.strategy.max.park.period";

    public static final int BUFFER_SLOT_CAPACITY_DEFAULT = 65536;

    public static final int STREAMS_BUFFER_CAPACITY_DEFAULT = 1024 * 1024;

    public static final int THROTTLE_BUFFER_CAPACITY_DEFAULT = 64 * 1024;

    public static final int COMMAND_BUFFER_CAPACITY_DEFAULT = 1024 * 1024;

    public static final int RESPONSE_BUFFER_CAPACITY_DEFAULT = 1024 * 1024;

    public static final int COUNTERS_BUFFER_CAPACITY_DEFAULT = 1024 * 1024;

    public static final int ROUTES_BUFFER_CAPACITY_DEFAULT = 1024 * 1024;

    private static final long BACKOFF_IDLE_STRATEGY_MAX_SPINS_DEFAULT = 64L;

    private static final long BACKOFF_IDLE_STRATEGY_MAX_YIELDS_DEFAULT = 64L;

    private static final long BACKOFF_IDLE_STRATEGY_MIN_PARK_PERIOD_NANOS_DEFAULT = NANOSECONDS.toNanos(64L);

    private static final long BACKOFF_IDLE_STRATEGY_MAX_PARK_PERIOD_NANOS_DEFAULT = MILLISECONDS.toNanos(1L);

    private static final boolean TIMESTAMPS_DEFAULT = true;

    public ReaktorConfiguration(
        Configuration config)
    {
        super(config);
    }

    public ReaktorConfiguration(
        Properties properties)
    {
        super(properties);
    }

    public ReaktorConfiguration(
        Configuration config,
        Properties defaultOverrides)
    {
        super(config, defaultOverrides);
    }

    @Override
    public final Path directory()
    {
        return Paths.get(getProperty(DIRECTORY_PROPERTY_NAME, "."));
    }

    public int bufferPoolCapacity()
    {
        return getInteger(BUFFER_POOL_CAPACITY_PROPERTY, this::calculateBufferPoolCapacity);
    }

    public int bufferSlotCapacity()
    {
        return getInteger(BUFFER_SLOT_CAPACITY_PROPERTY, BUFFER_SLOT_CAPACITY_DEFAULT);
    }

    @Override
    public int maximumStreamsCount()
    {
        return bufferPoolCapacity() / bufferSlotCapacity();
    }

    @Override
    public int streamsBufferCapacity()
    {
        return getInteger(STREAMS_BUFFER_CAPACITY_PROPERTY_NAME, STREAMS_BUFFER_CAPACITY_DEFAULT);
    }

    @Override
    public int throttleBufferCapacity()
    {
        return getInteger(THROTTLE_BUFFER_CAPACITY_PROPERTY_NAME, THROTTLE_BUFFER_CAPACITY_DEFAULT);
    }

    @Override
    public int commandBufferCapacity()
    {
        return getInteger(COMMAND_BUFFER_CAPACITY_PROPERTY_NAME, COMMAND_BUFFER_CAPACITY_DEFAULT);
    }

    @Override
    public int responseBufferCapacity()
    {
        return getInteger(RESPONSE_BUFFER_CAPACITY_PROPERTY_NAME, RESPONSE_BUFFER_CAPACITY_DEFAULT);
    }

    public int routesBufferCapacity()
    {
        return getInteger(ROUTES_BUFFER_CAPACITY_PROPERTY_NAME, ROUTES_BUFFER_CAPACITY_DEFAULT);
    }

    @Override
    public int counterValuesBufferCapacity()
    {
        return getInteger(COUNTERS_BUFFER_CAPACITY_PROPERTY_NAME, COUNTERS_BUFFER_CAPACITY_DEFAULT);
    }

    @Override
    public int counterLabelsBufferCapacity()
    {
        return getInteger(COUNTERS_BUFFER_CAPACITY_PROPERTY_NAME, COUNTERS_BUFFER_CAPACITY_DEFAULT) * 2;
    }

    public boolean timestamps()
    {
        return getBoolean(TIMESTAMPS_PROPERTY_NAME, TIMESTAMPS_DEFAULT);
    }

    private int calculateBufferPoolCapacity()
    {
        return bufferSlotCapacity() * 64;
    }

    public long maxSpins()
    {
        return getLong(BACKOFF_IDLE_STRATEGY_MAX_SPINS, BACKOFF_IDLE_STRATEGY_MAX_SPINS_DEFAULT);
    }

    public long maxYields()
    {
        return getLong(BACKOFF_IDLE_STRATEGY_MAX_YIELDS, BACKOFF_IDLE_STRATEGY_MAX_YIELDS_DEFAULT);
    }

    public long minParkPeriodNanos()
    {
        return getLong(BACKOFF_IDLE_STRATEGY_MIN_PARK_PERIOD_NANOS, BACKOFF_IDLE_STRATEGY_MIN_PARK_PERIOD_NANOS_DEFAULT);
    }

    public long maxParkPeriodNanos()
    {
        return getLong(BACKOFF_IDLE_STRATEGY_MAX_PARK_PERIOD_NANOS, BACKOFF_IDLE_STRATEGY_MAX_PARK_PERIOD_NANOS_DEFAULT);
    }
}
