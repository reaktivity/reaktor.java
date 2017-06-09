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
package org.reaktivity.reaktor.internal.acceptable;

import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.LongSupplier;

import org.agrona.concurrent.AtomicBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.AtomicCounter;
import org.reaktivity.nukleus.Nukleus;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.function.MessageFunction;
import org.reaktivity.nukleus.function.MessagePredicate;
import org.reaktivity.nukleus.route.RouteHandler;
import org.reaktivity.nukleus.route.RouteKind;
import org.reaktivity.nukleus.stream.StreamFactory;
import org.reaktivity.nukleus.stream.StreamFactoryBuilder;
import org.reaktivity.reaktor.internal.Context;
import org.reaktivity.reaktor.internal.layouts.StreamsLayout;
import org.reaktivity.reaktor.internal.router.ReferenceKind;
import org.reaktivity.reaktor.internal.router.Router;

public final class Acceptable extends Nukleus.Composite implements RouteHandler
{
    private final Context context;
    private final Router router;
    private final String sourceName;
    private final AtomicBuffer writeBuffer;
    private final Map<String, Source> sourcesByPartitionName;
    private final Map<String, Target> targetsByName;
    private final Function<RouteKind, StreamFactory> supplyStreamFactory;

    public Acceptable(
        Context context,
        Router router,
        String sourceName,
        Function<RouteKind, StreamFactoryBuilder> supplyStreamFactoryBuilder)
    {
        this.context = context;
        this.router = router;
        this.sourceName = sourceName;
        this.writeBuffer = new UnsafeBuffer(new byte[context.maxMessageLength()]);
        this.sourcesByPartitionName = new HashMap<>();
        this.targetsByName = new HashMap<>();

        final Map<RouteKind, StreamFactory> streamFactories = new EnumMap<>(RouteKind.class);
        final AtomicCounter streams = context.counters().streams();
        final LongSupplier supplyStreamId = streams::increment;
        final AtomicLong correlations = new AtomicLong();
        for (RouteKind kind : EnumSet.allOf(RouteKind.class))
        {
            final ReferenceKind refKind = ReferenceKind.valueOf(kind);
            final LongSupplier supplyCorrelationId = () -> refKind.nextRef(correlations);
            final StreamFactoryBuilder streamFactoryBuilder = supplyStreamFactoryBuilder.apply(kind);
            if (streamFactoryBuilder != null)
            {
                StreamFactory streamFactory = streamFactoryBuilder
                        .setRouteHandler(this)
                        .setWriteBuffer(writeBuffer)
                        .setStreamIdSupplier(supplyStreamId)
                        .setCorrelationIdSupplier(supplyCorrelationId)
                        .build();
                streamFactories.put(kind, streamFactory);
            }
        }
        this.supplyStreamFactory = streamFactories::get;
    }

    @Override
    public String name()
    {
        return sourceName;
    }

    public void onReadable(
        String partitionName)
    {
        sourcesByPartitionName.computeIfAbsent(partitionName, this::newSource);
    }

    public void onWritable(
        String targetName)
    {
        supplyTargetInternal(targetName);
    }

    @Override
    public <R> R resolve(
        MessagePredicate filter,
        MessageFunction<R> mapper)
    {
        return router.resolve(filter, mapper);
    }

    @Override
    public MessageConsumer supplyTarget(
        String targetName)
    {
        return supplyTargetInternal(targetName).writeHandler();
    }

    @Override
    public void setThrottle(
        String targetName,
        long streamId,
        MessageConsumer throttle)
    {
        supplyTargetInternal(targetName).setThrottle(streamId, throttle);
    }

    private Source newSource(
        String partitionName)
    {
        StreamsLayout layout = new StreamsLayout.Builder()
            .path(context.sourceStreamsPath().apply(partitionName))
            .streamsCapacity(context.streamsBufferCapacity())
            .throttleCapacity(context.throttleBufferCapacity())
            .readonly(true)
            .build();

        return include(new Source(sourceName, partitionName, layout, writeBuffer,
                                  this::supplyTargetInternal, supplyStreamFactory));
    }

    private Target supplyTargetInternal(
        String targetName)
    {
        return targetsByName.computeIfAbsent(targetName, this::newTarget);
    }

    private Target newTarget(
        String targetName)
    {
        String targetPartition = String.format("%s#%s", targetName, sourceName);

        StreamsLayout layout = new StreamsLayout.Builder()
                .path(context.targetStreamsPath().apply(targetPartition))
                .streamsCapacity(context.streamsBufferCapacity())
                .throttleCapacity(context.throttleBufferCapacity())
                .readonly(false)
                .build();

        return include(new Target(targetName, layout));
    }
}
