/**
 * Copyright 2016-2021 The Reaktivity Project
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
package org.reaktivity.reaktor;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.function.Function;

import org.agrona.LangUtil;
import org.reaktivity.reaktor.nukleus.Configuration;

public class ReaktorConfiguration extends Configuration
{
    public static final boolean DEBUG_BUDGETS = Boolean.getBoolean("reaktor.debug.budgets");

    public static final PropertyDef<String> REAKTOR_NAME;
    public static final PropertyDef<String> REAKTOR_DIRECTORY;
    public static final PropertyDef<Path> REAKTOR_CACHE_DIRECTORY;
    public static final PropertyDef<HostResolver> REAKTOR_HOST_RESOLVER;
    public static final IntPropertyDef REAKTOR_BUDGETS_BUFFER_CAPACITY;
    public static final IntPropertyDef REAKTOR_STREAMS_BUFFER_CAPACITY;
    public static final IntPropertyDef REAKTOR_COMMAND_BUFFER_CAPACITY;
    public static final IntPropertyDef REAKTOR_RESPONSE_BUFFER_CAPACITY;
    public static final IntPropertyDef REAKTOR_COUNTERS_BUFFER_CAPACITY;
    public static final IntPropertyDef REAKTOR_BUFFER_POOL_CAPACITY;
    public static final IntPropertyDef REAKTOR_BUFFER_SLOT_CAPACITY;
    public static final IntPropertyDef REAKTOR_ROUTES_BUFFER_CAPACITY;
    public static final BooleanPropertyDef REAKTOR_TIMESTAMPS;
    public static final IntPropertyDef REAKTOR_MAXIMUM_MESSAGES_PER_READ;
    public static final IntPropertyDef REAKTOR_MAXIMUM_EXPIRATIONS_PER_POLL;
    public static final IntPropertyDef REAKTOR_TASK_PARALLELISM;
    public static final LongPropertyDef REAKTOR_BACKOFF_MAX_SPINS;
    public static final LongPropertyDef REAKTOR_BACKOFF_MAX_YIELDS;
    public static final LongPropertyDef REAKTOR_BACKOFF_MIN_PARK_NANOS;
    public static final LongPropertyDef REAKTOR_BACKOFF_MAX_PARK_NANOS;
    public static final BooleanPropertyDef REAKTOR_DRAIN_ON_CLOSE;
    public static final BooleanPropertyDef REAKTOR_SYNTHETIC_ABORT;
    public static final LongPropertyDef REAKTOR_ROUTED_DELAY_MILLIS;
    public static final LongPropertyDef REAKTOR_CREDITOR_CHILD_CLEANUP_LINGER_MILLIS;
    public static final BooleanPropertyDef REAKTOR_VERBOSE;

    private static final ConfigurationDef REAKTOR_CONFIG;


    static
    {
        final ConfigurationDef config = new ConfigurationDef("reaktor");
        REAKTOR_NAME = config.property("name", "reaktor");
        REAKTOR_DIRECTORY = config.property("directory", ".");
        REAKTOR_CACHE_DIRECTORY = config.property(Path.class, "cache.directory", ReaktorConfiguration::cacheDirectory, "cache");
        REAKTOR_HOST_RESOLVER = config.property(HostResolver.class, "host.resolver",
                ReaktorConfiguration::decodeHostResolver, ReaktorConfiguration::defaultHostResolver);
        REAKTOR_BUDGETS_BUFFER_CAPACITY = config.property("budgets.buffer.capacity", 1024 * 1024);
        REAKTOR_STREAMS_BUFFER_CAPACITY = config.property("streams.buffer.capacity", 1024 * 1024);
        REAKTOR_COMMAND_BUFFER_CAPACITY = config.property("command.buffer.capacity", 1024 * 1024);
        REAKTOR_RESPONSE_BUFFER_CAPACITY = config.property("response.buffer.capacity", 1024 * 1024);
        REAKTOR_COUNTERS_BUFFER_CAPACITY = config.property("counters.buffer.capacity", 1024 * 1024);
        REAKTOR_BUFFER_POOL_CAPACITY = config.property("buffer.pool.capacity", ReaktorConfiguration::defaultBufferPoolCapacity);
        REAKTOR_BUFFER_SLOT_CAPACITY = config.property("buffer.slot.capacity", 64 * 1024);
        REAKTOR_ROUTES_BUFFER_CAPACITY = config.property("routes.buffer.capacity", 1024 * 1024);
        REAKTOR_TIMESTAMPS = config.property("timestamps", true);
        REAKTOR_MAXIMUM_MESSAGES_PER_READ = config.property("maximum.messages.per.read", Integer.MAX_VALUE);
        REAKTOR_MAXIMUM_EXPIRATIONS_PER_POLL = config.property("maximum.expirations.per.poll", Integer.MAX_VALUE);
        REAKTOR_TASK_PARALLELISM = config.property("task.parallelism", 1);
        REAKTOR_BACKOFF_MAX_SPINS = config.property("backoff.idle.strategy.max.spins", 64L);
        REAKTOR_BACKOFF_MAX_YIELDS = config.property("backoff.idle.strategy.max.yields", 64L);
        // TODO: shorten property name string values to match constant naming
        REAKTOR_BACKOFF_MIN_PARK_NANOS = config.property("backoff.idle.strategy.min.park.period", NANOSECONDS.toNanos(64L));
        REAKTOR_BACKOFF_MAX_PARK_NANOS = config.property("backoff.idle.strategy.max.park.period", MILLISECONDS.toNanos(1L));
        REAKTOR_DRAIN_ON_CLOSE = config.property("drain.on.close", false);
        REAKTOR_SYNTHETIC_ABORT = config.property("synthetic.abort", false);
        REAKTOR_ROUTED_DELAY_MILLIS = config.property("routed.delay.millis", 0L);
        REAKTOR_CREDITOR_CHILD_CLEANUP_LINGER_MILLIS = config.property("child.cleanup.linger", SECONDS.toMillis(5L));
        REAKTOR_VERBOSE = config.property("verbose", false);
        REAKTOR_CONFIG = config;
    }

    public ReaktorConfiguration(
        Configuration config)
    {
        super(REAKTOR_CONFIG, config);
    }

    public ReaktorConfiguration(
        Properties properties)
    {
        super(REAKTOR_CONFIG, properties);
    }

    public ReaktorConfiguration(
        Configuration config,
        Properties defaultOverrides)
    {
        super(REAKTOR_CONFIG, config, defaultOverrides);
    }

    public ReaktorConfiguration()
    {
        super(REAKTOR_CONFIG, new Configuration());
    }

    public String name()
    {
        return REAKTOR_NAME.get(this);
    }

    @Override
    public final Path directory()
    {
        return Paths.get(REAKTOR_DIRECTORY.get(this));
    }

    public final Path cacheDirectory()
    {
        return REAKTOR_CACHE_DIRECTORY.get(this);
    }

    public int bufferPoolCapacity()
    {
        return REAKTOR_BUFFER_POOL_CAPACITY.getAsInt(this);
    }

    public int bufferSlotCapacity()
    {
        return REAKTOR_BUFFER_SLOT_CAPACITY.getAsInt(this);
    }

    public int maximumStreamsCount()
    {
        return bufferPoolCapacity() / bufferSlotCapacity();
    }

    public int maximumMessagesPerRead()
    {
        return REAKTOR_MAXIMUM_MESSAGES_PER_READ.getAsInt(this);
    }

    public int maximumExpirationsPerPoll()
    {
        return REAKTOR_MAXIMUM_EXPIRATIONS_PER_POLL.getAsInt(this);
    }

    public int taskParallelism()
    {
        return REAKTOR_TASK_PARALLELISM.getAsInt(this);
    }

    public int budgetsBufferCapacity()
    {
        return REAKTOR_BUDGETS_BUFFER_CAPACITY.getAsInt(this);
    }

    public int streamsBufferCapacity()
    {
        return REAKTOR_STREAMS_BUFFER_CAPACITY.getAsInt(this);
    }

    public int commandBufferCapacity()
    {
        return REAKTOR_COMMAND_BUFFER_CAPACITY.get(this);
    }

    public int responseBufferCapacity()
    {
        return REAKTOR_RESPONSE_BUFFER_CAPACITY.getAsInt(this);
    }

    public int routesBufferCapacity()
    {
        return REAKTOR_ROUTES_BUFFER_CAPACITY.get(this);
    }

    public int counterValuesBufferCapacity()
    {
        return REAKTOR_COUNTERS_BUFFER_CAPACITY.getAsInt(this);
    }

    public int counterLabelsBufferCapacity()
    {
        return REAKTOR_COUNTERS_BUFFER_CAPACITY.getAsInt(this) * 2;
    }

    public boolean timestamps()
    {
        return REAKTOR_TIMESTAMPS.getAsBoolean(this);
    }

    public long maxSpins()
    {
        return REAKTOR_BACKOFF_MAX_SPINS.getAsLong(this);
    }

    public long maxYields()
    {
        return REAKTOR_BACKOFF_MAX_YIELDS.getAsLong(this);
    }

    public long minParkNanos()
    {
        return REAKTOR_BACKOFF_MIN_PARK_NANOS.getAsLong(this);
    }

    public long maxParkNanos()
    {
        return REAKTOR_BACKOFF_MAX_PARK_NANOS.getAsLong(this);
    }

    public boolean drainOnClose()
    {
        return REAKTOR_DRAIN_ON_CLOSE.getAsBoolean(this);
    }

    public boolean syntheticAbort()
    {
        return REAKTOR_SYNTHETIC_ABORT.getAsBoolean(this);
    }

    public long routedDelayMillis()
    {
        return REAKTOR_ROUTED_DELAY_MILLIS.getAsLong(this);
    }

    public long childCleanupLingerMillis()
    {
        return REAKTOR_CREDITOR_CHILD_CLEANUP_LINGER_MILLIS.getAsLong(this);
    }

    public boolean verbose()
    {
        return REAKTOR_VERBOSE.getAsBoolean(this);
    }

    public Function<String, InetAddress[]> hostResolver()
    {
        return REAKTOR_HOST_RESOLVER.get(this)::resolve;
    }

    private static int defaultBufferPoolCapacity(
        Configuration config)
    {
        return REAKTOR_BUFFER_SLOT_CAPACITY.get(config) * 64;
    }

    private static Path cacheDirectory(
        Configuration config,
        String cacheDirectory)
    {
        return Paths.get(REAKTOR_DIRECTORY.get(config)).resolve(cacheDirectory);
    }

    @FunctionalInterface
    private interface HostResolver
    {
        InetAddress[] resolve(
            String name);
    }

    private static HostResolver decodeHostResolver(
        Configuration config,
        String value)
    {
        HostResolver resolver = null;

        try
        {
            MethodType signature = MethodType.methodType(InetAddress[].class, String.class);
            String[] parts = value.split("::");
            Class<?> ownerClass = Class.forName(parts[0]);
            String methodName = parts[1];
            MethodHandle method = MethodHandles.publicLookup().findStatic(ownerClass, methodName, signature);
            resolver = name ->
            {
                InetAddress[] addresses = null;

                try
                {
                    addresses = (InetAddress[]) method.invoke(name);
                }
                catch (Throwable ex)
                {
                    LangUtil.rethrowUnchecked(ex);
                }

                return addresses;
            };
        }
        catch (Throwable ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }

        return resolver;
    }

    private static HostResolver defaultHostResolver(
        Configuration config)
    {
        return name ->
        {
            InetAddress[] addresses = null;

            try
            {
                addresses = InetAddress.getAllByName(name);
            }
            catch (UnknownHostException ex)
            {
                LangUtil.rethrowUnchecked(ex);
            }

            return addresses;
        };
    }
}
