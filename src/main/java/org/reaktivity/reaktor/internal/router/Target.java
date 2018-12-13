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

import static java.lang.String.format;
import static org.reaktivity.reaktor.internal.types.stream.FrameFW.FIELD_OFFSET_TIMESTAMP;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.concurrent.status.AtomicCounter;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.function.MessagePredicate;
import org.reaktivity.reaktor.internal.Counters;
import org.reaktivity.reaktor.internal.layouts.StreamsLayout;
import org.reaktivity.reaktor.internal.types.stream.AbortFW;
import org.reaktivity.reaktor.internal.types.stream.BeginFW;
import org.reaktivity.reaktor.internal.types.stream.DataFW;
import org.reaktivity.reaktor.internal.types.stream.EndFW;
import org.reaktivity.reaktor.internal.types.stream.FrameFW;
import org.reaktivity.reaktor.internal.types.stream.ResetFW;
import org.reaktivity.reaktor.internal.types.stream.WindowFW;

final class Target implements AutoCloseable
{
    private final FrameFW frameRO = new FrameFW();

    private final ResetFW.Builder resetRW = new ResetFW.Builder();

    private final String targetName;
    private final AutoCloseable layout;
    private final MutableDirectBuffer writeBuffer;
    private final Counters counters;
    private final boolean timestamps;
    private final Long2ObjectHashMap<MessageConsumer> streams;
    private final Long2ObjectHashMap<MessageConsumer> throttles;
    private final Long2ObjectHashMap<WriteCounters> countersByRouteId;
    private final MessageConsumer writeHandler;

    private MessagePredicate streamsBuffer;

    Target(
        String targetName,
        StreamsLayout layout,
        MutableDirectBuffer writeBuffer,
        Counters counters,
        boolean timestamps,
        int maximumMessagesPerRead,
        Long2ObjectHashMap<MessageConsumer> streams,
        Long2ObjectHashMap<MessageConsumer> throttles)
    {
        this.targetName = targetName;
        this.layout = layout;
        this.writeBuffer = writeBuffer;
        this.counters = counters;
        this.timestamps = timestamps;
        this.streamsBuffer = layout.streamsBuffer()::write;
        this.streams = streams;
        this.throttles = throttles;
        this.writeHandler = this::handleWrite;
        this.countersByRouteId = new Long2ObjectHashMap<>();
    }

    public void detach()
    {
        streamsBuffer = (t, b, i, l) -> true;
    }

    @Override
    public void close() throws Exception
    {
        throttles.forEach(this::doSyntheticReset);

        layout.close();
    }

    @Override
    public String toString()
    {
        return String.format("%s (write)", targetName);
    }

    public void setThrottle(
        long streamId,
        MessageConsumer throttle)
    {
        throttles.put(streamId, throttle);
    }

    public MessageConsumer writeHandler()
    {
        return writeHandler;
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

        final FrameFW frame = frameRO.wrap(buffer, index, index + length);
        final long streamId = frame.streamId();
        final long routeId = frame.routeId();

        if ((streamId & 0x8000_0000_0000_0000L) == 0L)
        {
            handled = handleWriteInitial(streamId, routeId, msgTypeId, buffer, index, length);
        }
        else
        {
            handled = handleWriteReply(streamId, routeId, msgTypeId, buffer, index, length);
        }

        if (!handled)
        {
            throw new IllegalStateException("Unable to write to streams buffer");
        }
    }

    private boolean handleWriteInitial(
        long streamId,
        long routeId,
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        boolean handled;

        if ((msgTypeId & 0x4000_0000) == 0)
        {
            final WriteCounters counters = countersByRouteId.computeIfAbsent(routeId, WriteCounters::new);

            switch (msgTypeId)
            {
            case BeginFW.TYPE_ID:
                counters.opens.increment();
                handled = streamsBuffer.test(msgTypeId, buffer, index, length);
                break;
            case DataFW.TYPE_ID:
                counters.frames.increment();
                counters.bytes.add(buffer.getInt(index + DataFW.FIELD_OFFSET_LENGTH));
                handled = streamsBuffer.test(msgTypeId, buffer, index, length);
                break;
            case EndFW.TYPE_ID:
                counters.closes.increment();
                handled = streamsBuffer.test(msgTypeId, buffer, index, length);
                throttles.remove(streamId);
                break;
            case AbortFW.TYPE_ID:
                counters.aborts.increment();
                handled = streamsBuffer.test(msgTypeId, buffer, index, length);
                throttles.remove(streamId);
                break;
            default:
                handled = true;
                break;
            }
        }
        else
        {
            switch (msgTypeId)
            {
            case WindowFW.TYPE_ID:
                handled = streamsBuffer.test(msgTypeId, buffer, index, length);
                break;
            case ResetFW.TYPE_ID:
                handled = streamsBuffer.test(msgTypeId, buffer, index, length);
                streams.remove(streamId);
                break;
            default:
                handled = true;
                break;
            }
        }

        return handled;
    }

    private boolean handleWriteReply(
        long streamId,
        long routeId,
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        boolean handled;

        if ((msgTypeId & 0x4000_0000) == 0)
        {
            switch (msgTypeId)
            {
            case BeginFW.TYPE_ID:
                handled = streamsBuffer.test(msgTypeId, buffer, index, length);
                break;
            case DataFW.TYPE_ID:
                handled = streamsBuffer.test(msgTypeId, buffer, index, length);
                break;
            case EndFW.TYPE_ID:
                handled = streamsBuffer.test(msgTypeId, buffer, index, length);
                throttles.remove(streamId);
                break;
            case AbortFW.TYPE_ID:
                handled = streamsBuffer.test(msgTypeId, buffer, index, length);
                throttles.remove(streamId);
                break;
            default:
                handled = true;
                break;
            }
        }
        else
        {
            final WriteCounters counters = countersByRouteId.computeIfAbsent(routeId, WriteCounters::new);
            switch (msgTypeId)
            {
            case WindowFW.TYPE_ID:
                counters.windows.increment();
                handled = streamsBuffer.test(msgTypeId, buffer, index, length);
                break;
            case ResetFW.TYPE_ID:
                counters.resets.increment();
                handled = streamsBuffer.test(msgTypeId, buffer, index, length);
                streams.remove(streamId);
                break;
            default:
                handled = true;
                break;
            }
        }

        return handled;
    }

    private void doSyntheticReset(
        long streamId,
        MessageConsumer sender)
    {
        final long syntheticRouteId = 0L;

        final ResetFW reset = resetRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(syntheticRouteId)
                .streamId(streamId)
                .build();

        sender.accept(reset.typeId(), reset.buffer(), reset.offset(), reset.sizeof());
    }

    final class WriteCounters
    {
        private final AtomicCounter opens;
        private final AtomicCounter closes;
        private final AtomicCounter aborts;
        private final AtomicCounter windows;
        private final AtomicCounter resets;
        private final AtomicCounter bytes;
        private final AtomicCounter frames;

        WriteCounters(
            long routeId)
        {
            this.opens = counters.counter(format("%d.opens.written", routeId));
            this.closes = counters.counter(format("%d.closes.written", routeId));
            this.aborts = counters.counter(format("%d.aborts.written", routeId));
            this.windows = counters.counter(format("%d.windows.written", routeId));
            this.resets = counters.counter(format("%d.resets.written", routeId));
            this.bytes = counters.counter(format("%d.bytes.written", routeId));
            this.frames = counters.counter(format("%d.frames.written", routeId));
        }
    }
}
