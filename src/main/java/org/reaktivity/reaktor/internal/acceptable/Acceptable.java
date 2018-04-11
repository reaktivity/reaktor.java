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
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.IntUnaryOperator;
import java.util.function.LongConsumer;
import java.util.function.LongFunction;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.concurrent.status.AtomicCounter;
import org.reaktivity.nukleus.Nukleus;
import org.reaktivity.nukleus.buffer.BufferPool;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.route.RouteKind;
import org.reaktivity.nukleus.route.RouteManager;
import org.reaktivity.nukleus.stream.StreamFactory;
import org.reaktivity.nukleus.stream.StreamFactoryBuilder;
import org.reaktivity.reaktor.internal.Context;
import org.reaktivity.reaktor.internal.State;
import org.reaktivity.reaktor.internal.buffer.CountingBufferPool;
import org.reaktivity.reaktor.internal.layouts.StreamsLayout;
import org.reaktivity.reaktor.internal.router.ReferenceKind;
import org.reaktivity.reaktor.internal.types.stream.AbortFW;

public final class Acceptable extends Nukleus.Composite
{
    private final AbortFW.Builder abortRW = new AbortFW.Builder();

    private final Context context;
    private final String sourceName;
    private final MutableDirectBuffer writeBuffer;
    private final Long2ObjectHashMap<MessageConsumer> streams;
    private final Function<RouteKind, StreamFactory> supplyStreamFactory;
    private final boolean timestamps;
    private final Source source;

    public Acceptable(
        Context context,
        MutableDirectBuffer writeBuffer,
        RouteManager router,
        String sourceName,
        State state,
        LongFunction<IntUnaryOperator> groupBudgetClaimer,
        LongFunction<IntUnaryOperator> groupBudgetReleaser,
        Function<RouteKind, StreamFactoryBuilder> supplyStreamFactoryBuilder,
        boolean timestamps,
        AtomicLong correlations)
    {
        this.context = context;
        this.sourceName = sourceName;
        this.writeBuffer = writeBuffer;
        this.streams = new Long2ObjectHashMap<>();

        final Map<RouteKind, StreamFactory> streamFactories = new EnumMap<>(RouteKind.class);
        final Function<String, LongSupplier> supplyCounter = name -> () -> context.counters().counter(name).increment() + 1;
        final Function<String, LongConsumer> supplyAccumulator = name -> (i) -> context.counters().counter(name).add(i);
        final AtomicCounter acquires = context.counters().acquires();
        final AtomicCounter releases = context.counters().releases();
        final BufferPool bufferPool = new CountingBufferPool(state.bufferPool(), acquires::increment, releases::increment);
        final Supplier<BufferPool> supplyCountingBufferPool = () -> bufferPool;
        for (RouteKind kind : EnumSet.allOf(RouteKind.class))
        {
            final ReferenceKind refKind = ReferenceKind.valueOf(kind);
            final LongSupplier supplyCorrelationId = () -> refKind.nextRef(correlations);
            final StreamFactoryBuilder streamFactoryBuilder = supplyStreamFactoryBuilder.apply(kind);
            if (streamFactoryBuilder != null)
            {
                StreamFactory streamFactory = streamFactoryBuilder
                        .setRouteManager(router)
                        .setWriteBuffer(writeBuffer)
                        .setStreamIdSupplier(state::supplyStreamId)
                        .setTraceSupplier(state::supplyTrace)
                        .setGroupIdSupplier(state::supplyGroupId)
                        .setGroupBudgetClaimer(groupBudgetClaimer)
                        .setGroupBudgetReleaser(groupBudgetReleaser)
                        .setCorrelationIdSupplier(supplyCorrelationId)
                        .setCounterSupplier(supplyCounter)
                        .setAccumulatorSupplier(supplyAccumulator)
                        .setBufferPoolSupplier(supplyCountingBufferPool)
                        .build();
                streamFactories.put(kind, streamFactory);
            }
        }
        this.supplyStreamFactory = streamFactories::get;
        this.timestamps = timestamps;
        this.source = newSource(sourceName);
    }

    @Override
    public String name()
    {
        return sourceName;
    }

    @Override
    public void close() throws Exception
    {
        doReset(sourceName, source);
        streams.forEach(this::doAbort);

        super.close();
    }

    private Source newSource(
        String sourceName)
    {
        StreamsLayout layout = new StreamsLayout.Builder()
            .path(context.sourceStreamsPath().apply(sourceName))
            .streamsCapacity(context.streamsBufferCapacity())
            .throttleCapacity(context.throttleBufferCapacity())
            .readonly(false)
            .build();

        return include(new Source(context.name(), sourceName, layout, writeBuffer, streams, supplyStreamFactory,
                                  timestamps));
    }

    private void doReset(
        String sourceName,
        Source source)
    {
        source.reset();
    }

    private void doAbort(
        long streamId,
        MessageConsumer stream)
    {
        final AbortFW abort = abortRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                                     .streamId(streamId)
                                     .build();

        stream.accept(abort.typeId(), abort.buffer(), abort.offset(), abort.sizeof());
    }
}
