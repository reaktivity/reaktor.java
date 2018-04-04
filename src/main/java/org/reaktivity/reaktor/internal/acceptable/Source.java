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

import static org.agrona.LangUtil.rethrowUnchecked;
import static org.reaktivity.reaktor.internal.types.stream.FrameFW.FIELD_OFFSET_TIMESTAMP;

import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.ToIntFunction;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.concurrent.AtomicBuffer;
import org.agrona.concurrent.MessageHandler;
import org.reaktivity.nukleus.Nukleus;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.function.MessagePredicate;
import org.reaktivity.nukleus.route.RouteKind;
import org.reaktivity.nukleus.stream.StreamFactory;
import org.reaktivity.reaktor.internal.layouts.StreamsLayout;
import org.reaktivity.reaktor.internal.router.ReferenceKind;
import org.reaktivity.reaktor.internal.types.stream.AbortFW;
import org.reaktivity.reaktor.internal.types.stream.BeginFW;
import org.reaktivity.reaktor.internal.types.stream.DataFW;
import org.reaktivity.reaktor.internal.types.stream.EndFW;
import org.reaktivity.reaktor.internal.types.stream.FrameFW;
import org.reaktivity.reaktor.internal.types.stream.ResetFW;
import org.reaktivity.reaktor.internal.types.stream.WindowFW;

public final class Source implements Nukleus
{
    private final FrameFW frameRO = new FrameFW();
    private final BeginFW beginRO = new BeginFW();

    private final ResetFW.Builder resetRW = new ResetFW.Builder();

    private final String nukleusName;
    private final String sourceName;
    private final StreamsLayout layout;
    private final AtomicBuffer writeBuffer;
    private final ToIntFunction<MessageHandler> streamsBuffer;
    private final Supplier<String> streamsDescriptor;
    private final Long2ObjectHashMap<MessageConsumer> streams;
    private final Function<RouteKind, StreamFactory> supplyStreamFactory;
    private final int abortTypeId;
    private final boolean timestamps;
    private final MessageHandler readHandler;
    private final MessageConsumer writeHandler;

    private MessagePredicate throttleBuffer;

    Source(
        String nukleusName,
        String sourceName,
        StreamsLayout layout,
        AtomicBuffer writeBuffer,
        Long2ObjectHashMap<MessageConsumer> streams,
        Function<RouteKind, StreamFactory> supplyStreamFactory,
        int abortTypeId,
        boolean timestamps)
    {
        this.nukleusName = nukleusName;
        this.sourceName = sourceName;
        this.layout = layout;
        this.writeBuffer = writeBuffer;
        this.supplyStreamFactory = supplyStreamFactory;
        this.abortTypeId = abortTypeId;
        this.streamsDescriptor = layout::toString;
        this.streamsBuffer = layout.streamsBuffer()::read;
        this.throttleBuffer = layout.throttleBuffer()::write;
        this.streams = streams;
        this.timestamps = timestamps;
        this.readHandler = this::handleRead;
        this.writeHandler = this::handleWrite;
    }

    @Override
    public int process()
    {
        return streamsBuffer.applyAsInt(readHandler);
    }

    @Override
    public void close() throws Exception
    {
        layout.close();
    }

    @Override
    public String name()
    {
        return sourceName;
    }

    @Override
    public String toString()
    {
        return String.format("%s[name=%s]", getClass().getSimpleName(), sourceName);
    }

    private void handleWrite(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        boolean handled;

        if (timestamps)
        {
            ((MutableDirectBuffer) buffer).putLong(index + FIELD_OFFSET_TIMESTAMP, System.nanoTime());
        }

        switch (msgTypeId)
        {
        case WindowFW.TYPE_ID:
            handled = throttleBuffer.test(msgTypeId, buffer, index, length);
            break;
        case ResetFW.TYPE_ID:
            handled = throttleBuffer.test(msgTypeId, buffer, index, length);

            final FrameFW reset = frameRO.wrap(buffer, index, index + length);
            streams.remove(reset.streamId());
            break;
        default:
            handled = true;
            break;
        }

        if (!handled)
        {
            throw new IllegalStateException("Unable to write to throttle buffer");
        }
    }

    private void handleRead(
        int msgTypeId,
        MutableDirectBuffer buffer,
        int index,
        int length)
    {
        frameRO.wrap(buffer, index, index + length);

        final long streamId = frameRO.streamId();

        final MessageConsumer handler = streams.get(streamId);

        try
        {
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
                    handler.accept(abortTypeId, buffer, index, length);
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
        catch (Throwable ex)
        {
            ex.addSuppressed(new Exception(String.format("[%s/%s]\t[0x%016x] %s",
                                                         nukleusName, sourceName, streamId, streamsDescriptor.get())));
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

    void reset()
    {
        throttleBuffer = (t, b, i, l) -> true;
    }

    private void doReset(
        final long streamId)
    {
        final ResetFW reset = resetRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .streamId(streamId)
                .build();

        handleWrite(reset.typeId(), reset.buffer(), reset.offset(), reset.sizeof());
    }
}
