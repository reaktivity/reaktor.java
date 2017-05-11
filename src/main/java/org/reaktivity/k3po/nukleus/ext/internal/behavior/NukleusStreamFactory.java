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
package org.reaktivity.k3po.nukleus.ext.internal.behavior;

import static org.jboss.netty.channel.Channels.fireChannelClosed;
import static org.jboss.netty.channel.Channels.fireChannelDisconnected;
import static org.jboss.netty.channel.Channels.fireChannelUnbound;
import static org.jboss.netty.channel.Channels.fireMessageReceived;
import static org.kaazing.k3po.driver.internal.netty.channel.Channels.fireInputShutdown;

import java.nio.ByteBuffer;
import java.util.function.LongConsumer;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.MessageHandler;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.ChannelFuture;
import org.reaktivity.k3po.nukleus.ext.internal.behavior.types.OctetsFW;
import org.reaktivity.k3po.nukleus.ext.internal.behavior.types.stream.BeginFW;
import org.reaktivity.k3po.nukleus.ext.internal.behavior.types.stream.DataFW;
import org.reaktivity.k3po.nukleus.ext.internal.behavior.types.stream.EndFW;

public final class NukleusStreamFactory
{
    private final BeginFW beginRO = new BeginFW();
    private final DataFW dataRO = new DataFW();
    private final EndFW endRO = new EndFW();

    private final LongConsumer unregisterStream;

    public NukleusStreamFactory(
        LongConsumer unregisterStream)
    {
        this.unregisterStream = unregisterStream;
    }

    public MessageHandler newStream(
        NukleusChannel channel,
        NukleusPartition partition,
        ChannelFuture handshakeFuture)
    {
        return new Stream(channel, partition, handshakeFuture)::handleStream;
    }

    private final class Stream
    {
        private final NukleusChannel channel;
        private final NukleusPartition partition;
        private final ChannelFuture handshakeFuture;

        private Stream(
            NukleusChannel channel,
            NukleusPartition partition,
            ChannelFuture handshakeFuture)
        {
            this.channel = channel;
            this.partition = partition;
            this.handshakeFuture = handshakeFuture;
        }

        private void handleStream(
            int msgTypeId,
            MutableDirectBuffer buffer,
            int index,
            int length)
        {
            switch (msgTypeId)
            {
            case BeginFW.TYPE_ID:
                BeginFW begin = beginRO.wrap(buffer, index, index + length);
                onBegin(begin);
                break;
            case DataFW.TYPE_ID:
                DataFW data = dataRO.wrap(buffer, index, index + length);
                onData(data);
                break;
            case EndFW.TYPE_ID:
                EndFW end = endRO.wrap(buffer, index, index + length);
                onEnd(end);
                break;
            }
        }

        private void onBegin(
            BeginFW begin)
        {
            final long streamId = begin.streamId();
            final OctetsFW beginExt = begin.extension();

            final NukleusChannelConfig channelConfig = channel.getConfig();
            final int initialWindow = channelConfig.getWindow();

            int beginExtBytes = beginExt.sizeof();
            if (beginExtBytes != 0)
            {
                final DirectBuffer buffer = beginExt.buffer();
                final int offset = beginExt.offset();

                // TODO: avoid allocation
                final byte[] beginExtCopy = new byte[beginExtBytes];
                buffer.getBytes(offset, beginExtCopy);

                channel.readExtBuffer().writeBytes(beginExtCopy);
            }

            channel.sourceId(streamId);

            handshakeFuture.setSuccess();

            partition.doWindow(channel, initialWindow);
        }

        private void onData(
            DataFW data)
        {
            final long streamId = data.streamId();
            final OctetsFW payload = data.payload();
            final ChannelBuffer message = payload.get(NukleusStreamFactory.this::readBuffer);
            final int readableBytes = message.readableBytes();
            final OctetsFW dataExt = data.extension();

            if (channel.sourceWindow() >= readableBytes)
            {
                channel.sourceWindow(-readableBytes);

                int dataExtBytes = dataExt.sizeof();
                if (dataExtBytes != 0)
                {
                    final DirectBuffer buffer = dataExt.buffer();
                    final int offset = dataExt.offset();

                    // TODO: avoid allocation
                    final byte[] dataExtCopy = new byte[dataExtBytes];
                    buffer.getBytes(offset, dataExtCopy);

                    channel.readExtBuffer().writeBytes(dataExtCopy);
                }

                partition.doWindow(channel, readableBytes);

                fireMessageReceived(channel, message);
            }
            else
            {
                partition.doReset(streamId);
            }
        }

        private void onEnd(
            EndFW end)
        {
            final long streamId = end.streamId();
            unregisterStream.accept(streamId);

            final OctetsFW endExt = end.extension();

            int endExtBytes = endExt.sizeof();
            if (endExtBytes != 0)
            {
                final DirectBuffer buffer = endExt.buffer();
                final int offset = endExt.offset();

                // TODO: avoid allocation
                final byte[] endExtCopy = new byte[endExtBytes];
                buffer.getBytes(offset, endExtCopy);

                channel.readExtBuffer().writeBytes(endExtCopy);
            }

            if (channel.setReadClosed())
            {
                fireInputShutdown(channel);
                fireChannelDisconnected(channel);
                fireChannelUnbound(channel);
                fireChannelClosed(channel);
            }
            else
            {
                fireInputShutdown(channel);
            }
        }
    }

    private ChannelBuffer readBuffer(
        DirectBuffer buffer,
        int index,
        int maxLimit)
    {
        // TODO: avoid allocation
        final ByteBuffer dstBuffer = ByteBuffer.allocate(maxLimit - index);
        buffer.getBytes(index, dstBuffer, dstBuffer.capacity());
        dstBuffer.flip();
        return ChannelBuffers.wrappedBuffer(dstBuffer);
    }
}