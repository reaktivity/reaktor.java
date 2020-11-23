/**
 * Copyright 2016-2020 The Reaktivity Project
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
package org.reaktivity.reaktor.test.internal.k3po.ext.behavior;

import static org.jboss.netty.channel.Channels.fireChannelClosed;
import static org.jboss.netty.channel.Channels.fireChannelDisconnected;
import static org.jboss.netty.channel.Channels.fireChannelUnbound;
import static org.jboss.netty.channel.Channels.fireExceptionCaught;
import static org.jboss.netty.channel.Channels.fireMessageReceived;
import static org.kaazing.k3po.driver.internal.netty.channel.Channels.fireInputAborted;
import static org.kaazing.k3po.driver.internal.netty.channel.Channels.fireInputAdvised;
import static org.kaazing.k3po.driver.internal.netty.channel.Channels.fireInputShutdown;
import static org.reaktivity.reaktor.test.internal.k3po.ext.behavior.NukleusExtensionKind.BEGIN;
import static org.reaktivity.reaktor.test.internal.k3po.ext.behavior.NukleusExtensionKind.CHALLENGE;
import static org.reaktivity.reaktor.test.internal.k3po.ext.behavior.NukleusExtensionKind.DATA;
import static org.reaktivity.reaktor.test.internal.k3po.ext.behavior.NukleusExtensionKind.END;
import static org.reaktivity.reaktor.test.internal.k3po.ext.behavior.NukleusExtensionKind.FLUSH;
import static org.reaktivity.reaktor.test.internal.k3po.ext.behavior.NullChannelBuffer.NULL_BUFFER;
import static org.reaktivity.reaktor.test.internal.k3po.ext.types.NukleusTypeSystem.ADVISORY_FLUSH;

import java.util.function.LongConsumer;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.MessageHandler;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.ChannelFuture;
import org.reaktivity.reaktor.test.internal.k3po.ext.types.OctetsFW;
import org.reaktivity.reaktor.test.internal.k3po.ext.types.stream.AbortFW;
import org.reaktivity.reaktor.test.internal.k3po.ext.types.stream.BeginFW;
import org.reaktivity.reaktor.test.internal.k3po.ext.types.stream.DataFW;
import org.reaktivity.reaktor.test.internal.k3po.ext.types.stream.EndFW;
import org.reaktivity.reaktor.test.internal.k3po.ext.types.stream.FlushFW;
import org.reaktivity.reaktor.test.internal.k3po.ext.types.stream.FrameFW;
import org.reaktivity.reaktor.test.internal.k3po.ext.util.function.LongLongFunction;

public final class NukleusStreamFactory
{
    private final FrameFW frameRO = new FrameFW();
    private final BeginFW beginRO = new BeginFW();
    private final DataFW dataRO = new DataFW();
    private final EndFW endRO = new EndFW();
    private final AbortFW abortRO = new AbortFW();
    private final FlushFW flushRO = new FlushFW();

    private final LongLongFunction<NukleusTarget> supplySender;
    private final LongConsumer unregisterStream;

    public NukleusStreamFactory(
        LongLongFunction<NukleusTarget> supplySender,
        LongConsumer unregisterStream)
    {
        this.supplySender = supplySender;
        this.unregisterStream = unregisterStream;
    }

    public void doReset(
        NukleusChannel channel,
        long traceId)
    {
        final long routeId = channel.routeId();
        final long streamId = channel.sourceId();
        final NukleusTarget sender = supplySender.apply(routeId, streamId);

        sender.doReset(channel, traceId);
        unregisterStream.accept(streamId);
    }

    public void doChallenge(
        NukleusChannel channel,
        long traceId)
    {
        final ChannelBuffer challengeExt = channel.writeExtBuffer(CHALLENGE, true);

        final long routeId = channel.routeId();
        final long streamId = channel.sourceId();

        final NukleusTarget sender = supplySender.apply(routeId, streamId);
        sender.doChallenge(routeId, streamId, traceId, challengeExt);
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
            case FlushFW.TYPE_ID:
                FlushFW flush = flushRO.wrap(buffer, index, index + length);
                onFlush(flush);
                break;
            }
        }

        private void onBegin(
            BeginFW begin)
        {
            final long streamId = begin.streamId();
            final OctetsFW beginExt = begin.extension();

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
            if (config.getUpdate() == NukleusUpdateMode.HANDSHAKE ||
                config.getUpdate() == NukleusUpdateMode.STREAM)
            {
                final NukleusChannelConfig channelConfig = channel.getConfig();
                final int initialWindow = channelConfig.getWindow();
                final int padding = channelConfig.getPadding();
                final long creditorId = channel.creditorId();

                sender.doWindow(channel, creditorId, initialWindow, padding);
            }

            channel.beginInputFuture().setSuccess();

            beginFuture.setSuccess();
        }

        private void onData(
            DataFW data)
        {
            final long traceId = data.traceId();
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
                    sender.doReset(channel, traceId);
                }
                else
                {
                    int credit = data.reserved();

                    final NukleusChannelConfig config = channel.getConfig();
                    if (config.getUpdate() == NukleusUpdateMode.MESSAGE ||
                        config.getUpdate() == NukleusUpdateMode.STREAM ||
                        config.getUpdate() == NukleusUpdateMode.PROACTIVE)
                    {
                        long creditorId = channel.creditorId();
                        int padding = config.getPadding();
                        channel.doSharedCredit(traceId, credit);
                        sender.doWindow(channel, creditorId, credit, padding);
                    }
                    else
                    {
                        channel.pendingSharedCredit(credit);
                    }

                    if ((flags & 0x01) != 0x00 || (flags & 0x04) != 0x00)
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
                sender.doReset(channel, traceId);

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
            final long traceId = end.traceId();

            if (end.authorization() != channel.sourceAuth())
            {
                sender.doReset(channel, traceId);
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
            final long traceId = abort.traceId();

            if (abort.authorization() != channel.sourceAuth())
            {
                sender.doReset(channel, traceId);
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

        private void onFlush(
            FlushFW flush)
        {
            if (flush.authorization() != channel.sourceAuth())
            {
                final long traceId = flush.traceId();
                sender.doReset(channel, traceId);
            }

            final OctetsFW flushExt = flush.extension();

            int flushExtBytes = flushExt.sizeof();
            if (flushExtBytes != 0)
            {
                final DirectBuffer buffer = flushExt.buffer();
                final int offset = flushExt.offset();

                // TODO: avoid allocation
                final byte[] flushExtCopy = new byte[flushExtBytes];
                buffer.getBytes(offset, flushExtCopy);

                channel.readExtBuffer(FLUSH).writeBytes(flushExtCopy);
            }

            fireInputAdvised(channel, ADVISORY_FLUSH);
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
