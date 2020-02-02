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
package org.reaktivity.reaktor.internal.router;

import static org.reaktivity.reaktor.internal.router.StreamId.instanceId;
import static org.reaktivity.reaktor.internal.router.StreamId.isInitial;
import static org.reaktivity.reaktor.internal.router.StreamId.streamIndex;
import static org.reaktivity.reaktor.internal.router.StreamId.throttleId;
import static org.reaktivity.reaktor.internal.router.StreamId.throttleIndex;
import static org.reaktivity.reaktor.internal.types.stream.FrameFW.FIELD_OFFSET_TIMESTAMP;

import java.util.function.LongFunction;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.collections.Long2ObjectHashMap;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.function.MessagePredicate;
import org.reaktivity.reaktor.ReaktorConfiguration;
import org.reaktivity.reaktor.internal.layouts.StreamsLayout;
import org.reaktivity.reaktor.internal.types.stream.AbortFW;
import org.reaktivity.reaktor.internal.types.stream.BeginFW;
import org.reaktivity.reaktor.internal.types.stream.ChallengeFW;
import org.reaktivity.reaktor.internal.types.stream.DataFW;
import org.reaktivity.reaktor.internal.types.stream.EndFW;
import org.reaktivity.reaktor.internal.types.stream.FlushFW;
import org.reaktivity.reaktor.internal.types.stream.FrameFW;
import org.reaktivity.reaktor.internal.types.stream.ResetFW;
import org.reaktivity.reaktor.internal.types.stream.SignalFW;
import org.reaktivity.reaktor.internal.types.stream.WindowFW;

public final class Target implements AutoCloseable
{
    private final FrameFW frameRO = new FrameFW();

    private final ResetFW.Builder resetRW = new ResetFW.Builder();

    private final int localIndex;
    private final String targetName;
    private final AutoCloseable streamsLayout;
    private final MutableDirectBuffer writeBuffer;
    private final boolean timestamps;
    private final Int2ObjectHashMap<MessageConsumer>[] streams;
    private final Int2ObjectHashMap<MessageConsumer>[] throttles;
    private final Long2ObjectHashMap<WriteCounters> countersByRouteId;
    private final MessageConsumer writeHandler;
    private final LongFunction<WriteCounters> newWriteCounters;

    private MessagePredicate streamsBuffer;

    public Target(
        ReaktorConfiguration config,
        int index,
        MutableDirectBuffer writeBuffer,
        Int2ObjectHashMap<MessageConsumer>[] streams,
        Int2ObjectHashMap<MessageConsumer>[] throttles,
        LongFunction<WriteCounters> newWriteCounters)
    {
        this.timestamps = config.timestamps();
        this.localIndex = index;

        final String targetName = String.format("data%d", index);
        this.targetName = targetName;

        final StreamsLayout streamsLayout = new StreamsLayout.Builder()
                .path(config.directory().resolve(targetName))
                .streamsCapacity(config.streamsBufferCapacity())
                .readonly(true)
                .build();
        this.streamsLayout = streamsLayout;
        this.streamsBuffer = streamsLayout.streamsBuffer()::write;

        this.writeBuffer = writeBuffer;
        this.newWriteCounters = newWriteCounters;
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
        for (int remoteIndex = 0; remoteIndex < throttles.length; remoteIndex++)
        {
            final int remoteIndex0 = remoteIndex;
            throttles[remoteIndex].forEach((id, handler) -> doSyntheticReset(throttleId(localIndex, remoteIndex0, id), handler));
        }

        streamsLayout.close();
    }

    @Override
    public String toString()
    {
        return String.format("%s (write)", targetName);
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
        boolean handled = false;

        if (timestamps)
        {
            ((MutableDirectBuffer) buffer).putLong(index + FIELD_OFFSET_TIMESTAMP, System.nanoTime());
        }

        final FrameFW frame = frameRO.wrap(buffer, index, index + length);
        final long streamId = frame.streamId();
        final long routeId = frame.routeId();

        if (streamId == 0L)
        {
            handled = handleWriteSystem(streamId, routeId, msgTypeId, buffer, index, length);
        }
        else if (isInitial(streamId))
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

    private boolean handleWriteSystem(
        long streamId,
        long routeId,
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        boolean handled = false;

        switch (msgTypeId)
        {
        case FlushFW.TYPE_ID:
            handled = streamsBuffer.test(msgTypeId, buffer, index, length);
            break;
        }

        return handled;
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
            final WriteCounters counters = countersByRouteId.computeIfAbsent(routeId, newWriteCounters);

            switch (msgTypeId)
            {
            case BeginFW.TYPE_ID:
                counters.opens.increment();
                handled = streamsBuffer.test(msgTypeId, buffer, index, length);
                break;
            case DataFW.TYPE_ID:
                counters.frames.increment();
                counters.bytes.getAndAdd(buffer.getInt(index + DataFW.FIELD_OFFSET_LENGTH));
                handled = streamsBuffer.test(msgTypeId, buffer, index, length);
                break;
            case EndFW.TYPE_ID:
                counters.closes.increment();
                handled = streamsBuffer.test(msgTypeId, buffer, index, length);
                throttles[throttleIndex(streamId)].remove(instanceId(streamId));
                break;
            case AbortFW.TYPE_ID:
                counters.aborts.increment();
                handled = streamsBuffer.test(msgTypeId, buffer, index, length);
                throttles[throttleIndex(streamId)].remove(instanceId(streamId));
                break;
            case FlushFW.TYPE_ID:
                handled = streamsBuffer.test(msgTypeId, buffer, index, length);
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
                streams[streamIndex(streamId)].remove(instanceId(streamId));
                break;
            case SignalFW.TYPE_ID:
                handled = streamsBuffer.test(msgTypeId, buffer, index, length);
                break;
            case ChallengeFW.TYPE_ID:
                handled = streamsBuffer.test(msgTypeId, buffer, index, length);
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
                throttles[throttleIndex(streamId)].remove(instanceId(streamId));
                break;
            case AbortFW.TYPE_ID:
                handled = streamsBuffer.test(msgTypeId, buffer, index, length);
                throttles[throttleIndex(streamId)].remove(instanceId(streamId));
                break;
            case FlushFW.TYPE_ID:
                handled = streamsBuffer.test(msgTypeId, buffer, index, length);
                break;
            default:
                handled = true;
                break;
            }
        }
        else
        {
            final WriteCounters counters = countersByRouteId.computeIfAbsent(routeId, newWriteCounters);
            switch (msgTypeId)
            {
            case WindowFW.TYPE_ID:
                counters.windows.increment();
                handled = streamsBuffer.test(msgTypeId, buffer, index, length);
                break;
            case ResetFW.TYPE_ID:
                counters.resets.increment();
                handled = streamsBuffer.test(msgTypeId, buffer, index, length);
                streams[streamIndex(streamId)].remove(instanceId(streamId));
                break;
            case SignalFW.TYPE_ID:
                handled = streamsBuffer.test(msgTypeId, buffer, index, length);
                break;
            case ChallengeFW.TYPE_ID:
                handled = streamsBuffer.test(msgTypeId, buffer, index, length);
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
}
