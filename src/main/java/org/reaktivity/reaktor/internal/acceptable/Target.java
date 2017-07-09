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

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.concurrent.MessageHandler;
import org.agrona.concurrent.ringbuffer.RingBuffer;
import org.reaktivity.nukleus.Nukleus;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.reaktor.internal.layouts.StreamsLayout;
import org.reaktivity.reaktor.internal.types.stream.AbortFW;
import org.reaktivity.reaktor.internal.types.stream.BeginFW;
import org.reaktivity.reaktor.internal.types.stream.DataFW;
import org.reaktivity.reaktor.internal.types.stream.EndFW;
import org.reaktivity.reaktor.internal.types.stream.FrameFW;
import org.reaktivity.reaktor.internal.types.stream.ResetFW;
import org.reaktivity.reaktor.internal.types.stream.WindowFW;

public final class Target implements Nukleus
{
    private final FrameFW frameRO = new FrameFW();

    private final String name;
    private final AutoCloseable layout;
    private final int abortTypeId;
    private final RingBuffer streamsBuffer;
    private final RingBuffer throttleBuffer;
    private final Long2ObjectHashMap<MessageConsumer> throttles;
    private final MessageHandler readHandler;
    private final MessageConsumer writeHandler;

    public Target(
        String name,
        StreamsLayout layout,
        int abortTypeId)
    {
        this.name = name;
        this.layout = layout;
        this.abortTypeId = abortTypeId;
        this.streamsBuffer = layout.streamsBuffer();
        this.throttleBuffer = layout.throttleBuffer();
        this.throttles = new Long2ObjectHashMap<>();
        this.readHandler = this::handleRead;
        this.writeHandler = this::handleWrite;
    }

    @Override
    public int process()
    {
        return throttleBuffer.read(readHandler);
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
        switch (msgTypeId)
        {
        case BeginFW.TYPE_ID:
            streamsBuffer.write(msgTypeId, buffer, index, length);
            break;
        case DataFW.TYPE_ID:
            streamsBuffer.write(msgTypeId, buffer, index, length);
            break;
        case EndFW.TYPE_ID:
            streamsBuffer.write(msgTypeId, buffer, index, length);

            final FrameFW end = frameRO.wrap(buffer, index, index + length);
            throttles.remove(end.streamId());
            break;
        case AbortFW.TYPE_ID:
            streamsBuffer.write(abortTypeId, buffer, index, length);

            final FrameFW abort = frameRO.wrap(buffer, index, index + length);
            throttles.remove(abort.streamId());
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
}
