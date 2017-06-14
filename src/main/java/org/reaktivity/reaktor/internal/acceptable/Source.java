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

import java.util.function.Function;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.concurrent.AtomicBuffer;
import org.agrona.concurrent.MessageHandler;
import org.agrona.concurrent.ringbuffer.RingBuffer;
import org.reaktivity.nukleus.Nukleus;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.route.RouteKind;
import org.reaktivity.nukleus.stream.StreamFactory;
import org.reaktivity.reaktor.internal.layouts.StreamsLayout;
import org.reaktivity.reaktor.internal.router.ReferenceKind;
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

    private final String sourceName;
    private final String partitionName;
    private final StreamsLayout layout;
    private final AtomicBuffer writeBuffer;
    private final RingBuffer streamsBuffer;
    private final RingBuffer throttleBuffer;
    private final Long2ObjectHashMap<MessageConsumer> streams;
    private final Function<RouteKind, StreamFactory> supplyStreamFactory;
    private final MessageHandler readHandler;
    private final MessageConsumer writeHandler;

    Source(
        String sourceName,
        String partitionName,
        StreamsLayout layout,
        AtomicBuffer writeBuffer,
        Long2ObjectHashMap<MessageConsumer> streams,
        Function<String, Target> supplyTarget,
        Function<RouteKind, StreamFactory> supplyStreamFactory)
    {
        this.sourceName = sourceName;
        this.partitionName = partitionName;
        this.layout = layout;
        this.writeBuffer = writeBuffer;
        this.supplyStreamFactory = supplyStreamFactory;
        this.streamsBuffer = layout.streamsBuffer();
        this.throttleBuffer = layout.throttleBuffer();
        this.streams = streams;
        this.readHandler = this::handleRead;
        this.writeHandler = this::handleWrite;
    }

    @Override
    public int process()
    {
        return streamsBuffer.read(readHandler);
    }

    @Override
    public void close() throws Exception
    {
        layout.close();
    }

    @Override
    public String name()
    {
        return partitionName;
    }

    public String routableName()
    {
        return sourceName;
    }

    @Override
    public String toString()
    {
        return String.format("%s[name=%s]", getClass().getSimpleName(), partitionName);
    }

    public MessageConsumer writeHandler()
    {
        return writeHandler;
    }

    public void cleanup(
        long streamId)
    {
        streams.remove(streamId);
    }

    private void handleWrite(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        switch (msgTypeId)
        {
        case WindowFW.TYPE_ID:
            throttleBuffer.write(msgTypeId, buffer, index, length);
            break;
        case ResetFW.TYPE_ID:
            throttleBuffer.write(msgTypeId, buffer, index, length);

            final FrameFW reset = frameRO.wrap(buffer, index, index + length);
            streams.remove(reset.streamId());
            break;
        default:
            break;
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

    private void doReset(
        final long streamId)
    {
        final ResetFW reset = resetRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .streamId(streamId)
                .build();

        handleWrite(reset.typeId(), reset.buffer(), reset.offset(), reset.sizeof());
    }
}
