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
package org.reaktivity.k3po.nukleus.ext.internal.behavior;

import static org.jboss.netty.channel.Channels.fireChannelClosed;
import static org.jboss.netty.channel.Channels.fireChannelDisconnected;
import static org.jboss.netty.channel.Channels.fireChannelUnbound;
import static org.jboss.netty.channel.Channels.fireExceptionCaught;
import static org.jboss.netty.channel.Channels.fireMessageReceived;
import static org.kaazing.k3po.driver.internal.netty.channel.Channels.fireInputAborted;
import static org.kaazing.k3po.driver.internal.netty.channel.Channels.fireInputShutdown;
import static org.reaktivity.k3po.nukleus.ext.internal.behavior.NukleusExtensionKind.BEGIN;
import static org.reaktivity.k3po.nukleus.ext.internal.behavior.NukleusExtensionKind.DATA;
import static org.reaktivity.k3po.nukleus.ext.internal.behavior.NukleusExtensionKind.END;
import static org.reaktivity.k3po.nukleus.ext.internal.behavior.NullChannelBuffer.NULL_BUFFER;

import java.util.function.LongConsumer;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.MessageHandler;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.ChannelFuture;
import org.reaktivity.k3po.nukleus.ext.internal.behavior.types.OctetsFW;
import org.reaktivity.k3po.nukleus.ext.internal.behavior.types.stream.AbortFW;
import org.reaktivity.k3po.nukleus.ext.internal.behavior.types.stream.BeginFW;
import org.reaktivity.k3po.nukleus.ext.internal.behavior.types.stream.DataFW;
import org.reaktivity.k3po.nukleus.ext.internal.behavior.types.stream.EndFW;
import org.reaktivity.k3po.nukleus.ext.internal.behavior.types.stream.FrameFW;

public final class NukleusStreamFactory
{
    private final FrameFW frameRO = new FrameFW();
    private final BeginFW beginRO = new BeginFW();
    private final DataFW dataRO = new DataFW();
    private final EndFW endRO = new EndFW();
    private final AbortFW abortRO = new AbortFW();

    private final LongConsumer unregisterStream;

    public NukleusStreamFactory(
        LongConsumer unregisterStream)
    {
        this.unregisterStream = unregisterStream;
    }

    public MessageHandler newStream(
        NukleusChannel channel,
        NukleusTarget sender,
        ChannelFuture beginFuture)
    {
        return new Stream(channel, sender, beginFuture)::handleStream;
    }

    private final class Stream
    {
        private final NukleusChannel channel;
        private final NukleusTarget sender;
        private final ChannelFuture beginFuture;
        private int fragments;

        private Stream(
            NukleusChannel channel,
            NukleusTarget sender,
            ChannelFuture beginFuture)
        {
            this.channel = channel;
            this.sender = sender;
            this.beginFuture = beginFuture;
        }

        private void handleStream(
            int msgTypeId,
            MutableDirectBuffer buffer,
            int index,
            int length)
        {
            final FrameFW frame = frameRO.wrap(buffer, index, index + length);
            final long routeId = frame.routeId();
            verifyRouteId(routeId);

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
            case AbortFW.TYPE_ID:
                AbortFW abort = abortRO.wrap(buffer, index, index + length);
                onAbort(abort);
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
            final long group = channelConfig.getGroup();
            final int padding = channelConfig.getPadding();

            int beginExtBytes = beginExt.sizeof();
            if (beginExtBytes != 0)
            {
                final DirectBuffer buffer = beginExt.buffer();
                final int offset = beginExt.offset();

                // TODO: avoid allocation
                final byte[] beginExtCopy = new byte[beginExtBytes];
                buffer.getBytes(offset, beginExtCopy);

                channel.readExtBuffer(BEGIN).writeBytes(beginExtCopy);
            }

            channel.sourceId(streamId);
            channel.sourceAuth(begin.authorization());
            final NukleusChannelConfig config = channel.getConfig();
            if (config.getUpdate() == NukleusUpdateMode.HANDSHAKE || config.getUpdate() == NukleusUpdateMode.STREAM)
            {
                sender.doWindow(channel, initialWindow, padding, group);
            }

            channel.beginInputFuture().setSuccess();

            beginFuture.setSuccess();
        }

        private void onData(
            DataFW data)
        {
            final int flags = data.flags();
            final OctetsFW payload = data.payload();
            final ChannelBuffer message = payload == null ? NULL_BUFFER : payload.get(this::readBuffer);
            final int readableBytes = message.readableBytes();
            final OctetsFW dataExt = data.extension();

            if (channel.readableBytes() >= readableBytes)
            {
                channel.readableBytes(-readableBytes);

                int dataExtBytes = dataExt.sizeof();
                if (dataExtBytes != 0)
                {
                    final DirectBuffer buffer = dataExt.buffer();
                    final int offset = dataExt.offset();

                    // TODO: avoid allocation
                    final byte[] dataExtCopy = new byte[dataExtBytes];
                    buffer.getBytes(offset, dataExtCopy);

                    channel.readExtBuffer(DATA).writeBytes(dataExtCopy);
                }

                if ((flags & 0x02) != 0x00 && fragments != 0)
                {
                    // INIT flag set on non-initial message fragment
                    fireExceptionCaught(channel, new IllegalStateException("invalid message boundary"));
                    sender.doReset(channel);
                }
                else
                {
                    final NukleusChannelConfig config = channel.getConfig();
                    if (config.getUpdate() == NukleusUpdateMode.MESSAGE || config.getUpdate() == NukleusUpdateMode.STREAM)
                    {
                        int padding = config.getPadding();
                        long group = config.getGroup();
                        sender.doWindow(channel, readableBytes + data.padding(), padding, group);
                    }

                    if ((flags & 0x01) != 0x00)
                    {
                        message.markWriterIndex(); // FIN
                        fragments = 0;
                    }
                    else
                    {
                        fragments++;
                    }

                    fireMessageReceived(channel, message);
                }
            }
            else
            {
                sender.doReset(channel);

                if (channel.setReadAborted())
                {
                    if (channel.setReadClosed())
                    {
                        fireInputAborted(channel);
                        fireChannelDisconnected(channel);
                        fireChannelUnbound(channel);
                        fireChannelClosed(channel);
                    }
                    else
                    {
                        fireInputAborted(channel);
                    }
                }
            }
        }

        private void onEnd(
            EndFW end)
        {
            final long streamId = end.streamId();

            if (end.authorization() != channel.sourceAuth())
            {
                sender.doReset(channel);
            }
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

                channel.readExtBuffer(END).writeBytes(endExtCopy);
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

        private void onAbort(
            AbortFW abort)
        {
            final long streamId = abort.streamId();

            if (abort.authorization() != channel.sourceAuth())
            {
                sender.doReset(channel);
            }
            unregisterStream.accept(streamId);

            if (channel.setReadAborted())
            {
                if (channel.setReadClosed())
                {
                    fireInputAborted(channel);
                    fireChannelDisconnected(channel);
                    fireChannelUnbound(channel);
                    fireChannelClosed(channel);
                }
                else
                {
                    fireInputAborted(channel);
                }
            }
        }

        private void verifyRouteId(
            final long routeId)
        {
            if (routeId != channel.routeId())
            {
                throw new IllegalStateException(String.format("routeId: expected %x actual %x", channel.routeId(), routeId));
            }
        }

        private ChannelBuffer readBuffer(
            DirectBuffer buffer,
            int index,
            int maxLimit)
        {
            // TODO: avoid allocation
            final byte[] array = new byte[maxLimit - index];
            buffer.getBytes(index, array);
            return channel.getConfig().getBufferFactory().getBuffer(array, 0, array.length);
        }
    }
}
