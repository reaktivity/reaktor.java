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
package org.reaktivity.reaktor.internal.acceptor;

import static org.reaktivity.reaktor.internal.types.stream.FrameFW.FIELD_OFFSET_TIMESTAMP;

import java.util.function.BiConsumer;
import java.util.function.ToIntFunction;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.concurrent.MessageHandler;
import org.reaktivity.nukleus.Nukleus;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.function.MessagePredicate;
import org.reaktivity.reaktor.internal.layouts.StreamsLayout;
import org.reaktivity.reaktor.internal.types.stream.AbortFW;
import org.reaktivity.reaktor.internal.types.stream.BeginFW;
import org.reaktivity.reaktor.internal.types.stream.DataFW;
import org.reaktivity.reaktor.internal.types.stream.EndFW;
import org.reaktivity.reaktor.internal.types.stream.FrameFW;
import org.reaktivity.reaktor.internal.types.stream.ResetFW;
import org.reaktivity.reaktor.internal.types.stream.WindowFW;

final class Target implements Nukleus
{
    private final FrameFW frameRO = new FrameFW();

    private final String name;
    private final AutoCloseable layout;
    private final boolean timestamps;
    private final Long2ObjectHashMap<MessageConsumer> throttles;
    private final MessageHandler readHandler;
    private final MessageConsumer writeHandler;

    private ToIntFunction<MessageHandler> throttleBuffer;
    private MessagePredicate streamsBuffer;

    Target(
        String name,
        StreamsLayout layout,
        boolean timestamps)
    {
        this.name = name;
        this.layout = layout;
        this.timestamps = timestamps;
        this.streamsBuffer = layout.streamsBuffer()::write;
        this.throttleBuffer = layout.throttleBuffer()::read;
        this.throttles = new Long2ObjectHashMap<>();
        this.readHandler = this::handleRead;
        this.writeHandler = this::handleWrite;
    }

    @Override
    public int process()
    {
        return throttleBuffer.applyAsInt(readHandler);
    }

    @Override
    public void close() throws Exception
    {
        layout.close();
    }

    @Override
    public String name()
    {
        return name;
    }

    @Override
    public String toString()
    {
        return name;
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

            final FrameFW end = frameRO.wrap(buffer, index, index + length);
            throttles.remove(end.streamId());
            break;
        case AbortFW.TYPE_ID:
            handled = streamsBuffer.test(msgTypeId, buffer, index, length);

            final FrameFW abort = frameRO.wrap(buffer, index, index + length);
            throttles.remove(abort.streamId());
            break;
        default:
            handled = true;
            break;
        }

        if (!handled)
        {
            throw new IllegalStateException("Unable to write to streams buffer");
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

    public void abort()
    {
        streamsBuffer = (t, b, i, l) -> true;
    }

    public void reset(
        BiConsumer<Long, MessageConsumer> resetHandler)
    {
        throttles.forEach(resetHandler);
    }
}
