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
package org.reaktivity.reaktor.internal.router;

import static org.agrona.LangUtil.rethrowUnchecked;

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
import java.util.function.ToIntFunction;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.concurrent.MessageHandler;
import org.agrona.concurrent.ringbuffer.RingBuffer;
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
import org.reaktivity.reaktor.internal.types.stream.AbortFW;
import org.reaktivity.reaktor.internal.types.stream.BeginFW;
import org.reaktivity.reaktor.internal.types.stream.DataFW;
import org.reaktivity.reaktor.internal.types.stream.EndFW;
import org.reaktivity.reaktor.internal.types.stream.FrameFW;
import org.reaktivity.reaktor.internal.types.stream.ResetFW;
import org.reaktivity.reaktor.internal.types.stream.WindowFW;

final class Source implements Nukleus
{
    private final FrameFW frameRO = new FrameFW();
    private final BeginFW beginRO = new BeginFW();
    private final AbortFW.Builder abortRW = new AbortFW.Builder();

    private final ResetFW.Builder resetRW = new ResetFW.Builder();

    private final String nukleusName;
    private final String name;
    private final StreamsLayout layout;
    private final MutableDirectBuffer writeBuffer;
    private final ToIntFunction<MessageHandler> streamsBuffer;
    private final MessageHandler readHandler;

    private final Long2ObjectHashMap<MessageConsumer> streams;
    private final Long2ObjectHashMap<MessageConsumer> throttles;
    private final Function<RouteKind, StreamFactory> supplyStreamFactory;
    private final Function<String, MessageConsumer> supplyThrottle;

    private MessageConsumer writeHandler;

    Source(
        Context context,
        MutableDirectBuffer writeBuffer,
        RouteManager router,
        String sourceName,
        State state,
        LongFunction<IntUnaryOperator> groupBudgetClaimer,
        LongFunction<IntUnaryOperator> groupBudgetReleaser,
        Function<RouteKind, StreamFactoryBuilder> supplyStreamFactoryBuilder,
        AtomicLong correlations,
        Function<String, MessageConsumer> supplyThrottle,
        Long2ObjectHashMap<MessageConsumer> streams,
        Long2ObjectHashMap<MessageConsumer> throttles)
    {
        this.nukleusName = context.name();
        this.name = sourceName;
        this.writeBuffer = writeBuffer;
        this.streams = streams;
        this.throttles = throttles;
        this.readHandler = this::handleRead;

        final StreamsLayout layout = new StreamsLayout.Builder()
                .path(context.sourceStreamsPath().apply(sourceName))
                .streamsCapacity(context.streamsBufferCapacity())
                .readonly(false)
                .build();

        this.layout = layout;
        final int messageCountLimit = context.maximumMessagesPerRead();
        final RingBuffer streamsBuffer = layout.streamsBuffer();
        this.streamsBuffer = handler -> streamsBuffer.read(handler, messageCountLimit);
        this.supplyThrottle = supplyThrottle;

        final Map<RouteKind, StreamFactory> streamFactories = new EnumMap<>(RouteKind.class);
        final Function<String, LongSupplier> supplyCounter = name -> () -> context.counters().counter(name).increment() + 1;
        final Function<String, LongConsumer> supplyAccumulator = name -> (i) -> context.counters().counter(name).add(i);
        final AtomicCounter acquires = context.counters().acquires();
        final AtomicCounter releases = context.counters().releases();
        final BufferPool bufferPool = new CountingBufferPool(state.bufferPool(), acquires::increment, releases::increment);
        final Supplier<BufferPool> supplyCountingBufferPool = () -> bufferPool;
        for (RouteKind kind : EnumSet.allOf(RouteKind.class))
        {
            final ReferenceKind sourceRefKind = ReferenceKind.sourceKind(kind);
            final LongSupplier supplySourceCorrelationId = () -> sourceRefKind.nextRef(correlations);
            final ReferenceKind targetRefKind = ReferenceKind.targetKind(kind);
            final LongSupplier supplyTargetCorrelationId = () -> targetRefKind.nextRef(correlations);
            final StreamFactoryBuilder streamFactoryBuilder = supplyStreamFactoryBuilder.apply(kind);
            if (streamFactoryBuilder != null)
            {
                StreamFactory streamFactory = streamFactoryBuilder
                        .setRouteManager(router)
                        .setWriteBuffer(writeBuffer)
                        .setInitialIdSupplier(state::supplyInitialId)
                        .setReplyIdSupplier(state::supplyReplyId)
                        .setTraceSupplier(state::supplyTrace)
                        .setGroupIdSupplier(state::supplyGroupId)
                        .setGroupBudgetClaimer(groupBudgetClaimer)
                        .setGroupBudgetReleaser(groupBudgetReleaser)
                        .setSourceCorrelationIdSupplier(supplySourceCorrelationId)
                        .setTargetCorrelationIdSupplier(supplyTargetCorrelationId)
                        .setCounterSupplier(supplyCounter)
                        .setAccumulatorSupplier(supplyAccumulator)
                        .setBufferPoolSupplier(supplyCountingBufferPool)
                        .build();
                streamFactories.put(kind, streamFactory);
            }
        }
        this.supplyStreamFactory = streamFactories::get;
    }

    @Override
    public String name()
    {
        return name;
    }

    @Override
    public int process()
    {
        return streamsBuffer.applyAsInt(readHandler);
    }

    public void detach()
    {
        writeHandler = (t, b, i, l) -> {};
    }

    @Override
    public void close() throws Exception
    {
        streams.forEach(this::doAbort);

        layout.close();
    }

    @Override
    public String toString()
    {
        return String.format("%s (read)", name);
    }

    private void handleRead(
        int msgTypeId,
        MutableDirectBuffer buffer,
        int index,
        int length)
    {
        final FrameFW frame = frameRO.wrap(buffer, index, index + length);
        final long streamId = frame.streamId();

        try
        {
            if ((msgTypeId & 0x4000_0000) != 0)
            {
                final MessageConsumer throttle = throttles.get(streamId);
                if (throttle != null)
                {
                    switch (msgTypeId)
                    {
                    case WindowFW.TYPE_ID:
                        throttle.accept(msgTypeId, buffer, index, length);
                        break;
                    case ResetFW.TYPE_ID:
                        throttle.accept(msgTypeId, buffer, index, length);
                        throttles.remove(streamId);
                        break;
                    default:
                        break;
                    }
                }
            }
            else
            {
                final MessageConsumer handler = streams.get(streamId);
                if (handler != null)
                {
                    switch (msgTypeId)
                    {
                    case BeginFW.TYPE_ID:
                    case DataFW.TYPE_ID:
                        handler.accept(msgTypeId, buffer, index, length);
                        break;
                    case EndFW.TYPE_ID:
                        handler.accept(msgTypeId, buffer, index, length);
                        streams.remove(streamId);
                        break;
                    case AbortFW.TYPE_ID:
                        handler.accept(msgTypeId, buffer, index, length);
                        streams.remove(streamId);
                        break;
                    default:
                        handleUnrecognized(msgTypeId, buffer, index, length);
                        break;
                    }
                }
                else
                {
                    handleUnrecognized(msgTypeId, buffer, index, length);
                }
            }
        }
        catch (Throwable ex)
        {
            ex.addSuppressed(new Exception(String.format("[%s/%s]\t[0x%016x] %s",
                                                         nukleusName, name, streamId, layout)));
            rethrowUnchecked(ex);
        }
    }

    private void handleUnrecognized(
        int msgTypeId,
        MutableDirectBuffer buffer,
        int index,
        int length)
    {
        if (msgTypeId == BeginFW.TYPE_ID)
        {
            handleBegin(msgTypeId, buffer, index, length);
        }
        else
        {
            frameRO.wrap(buffer, index, index + length);

            final long streamId = frameRO.streamId();

            doReset(streamId);
        }
    }

    private void handleBegin(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        final BeginFW begin = beginRO.wrap(buffer, index, index + length);
        final long sourceId = begin.streamId();
        final long sourceRef = begin.sourceRef();
        final long correlationId = begin.correlationId();

        final long resolveId = (sourceRef == 0L) ? correlationId : sourceRef;
        RouteKind routeKind = ReferenceKind.resolve(resolveId);

        StreamFactory streamFactory = supplyStreamFactory.apply(routeKind);
        if (streamFactory != null)
        {
            final MessageConsumer writeHandler = writeHandler();
            final MessageConsumer newStream = streamFactory.newStream(msgTypeId, buffer, index, length, writeHandler);
            if (newStream != null)
            {
                streams.put(sourceId, newStream);
                newStream.accept(begin.typeId(), begin.buffer(), begin.offset(), begin.sizeof());
            }
            else
            {
                doReset(sourceId);
            }
        }
        else
        {
            doReset(sourceId);
        }
    }

    private MessageConsumer writeHandler()
    {
        // defer to let owner nukleus create first
        if (writeHandler == null)
        {
            writeHandler = supplyThrottle.apply(name);
            assert writeHandler != null;
        }

        return writeHandler;
    }

    private void doReset(
        final long streamId)
    {
        final ResetFW reset = resetRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .streamId(streamId)
                .build();

        final MessageConsumer writeHandler = writeHandler();
        writeHandler.accept(reset.typeId(), reset.buffer(), reset.offset(), reset.sizeof());
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
