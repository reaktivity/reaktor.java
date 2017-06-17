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

import static java.lang.Math.min;
import static org.agrona.BitUtil.SIZE_OF_LONG;
import static org.agrona.BitUtil.SIZE_OF_SHORT;
import static org.jboss.netty.channel.Channels.fireChannelBound;
import static org.jboss.netty.channel.Channels.fireChannelClosed;
import static org.jboss.netty.channel.Channels.fireChannelConnected;
import static org.jboss.netty.channel.Channels.fireChannelDisconnected;
import static org.jboss.netty.channel.Channels.fireChannelUnbound;
import static org.jboss.netty.channel.Channels.fireWriteComplete;
import static org.jboss.netty.channel.Channels.succeededFuture;
import static org.kaazing.k3po.driver.internal.netty.channel.Channels.fireChannelAborted;
import static org.kaazing.k3po.driver.internal.netty.channel.Channels.fireFlushed;
import static org.kaazing.k3po.driver.internal.netty.channel.Channels.fireOutputShutdown;

import java.nio.file.Path;
import java.util.Deque;
import java.util.Random;
import java.util.function.Consumer;
import java.util.function.LongConsumer;
import java.util.function.LongFunction;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.MessageHandler;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.ringbuffer.RingBuffer;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.DownstreamMessageEvent;
import org.jboss.netty.channel.MessageEvent;
import org.kaazing.k3po.driver.internal.netty.channel.ChannelAddress;
import org.reaktivity.k3po.nukleus.ext.internal.behavior.layout.Layout;
import org.reaktivity.k3po.nukleus.ext.internal.behavior.layout.StreamsLayout;
import org.reaktivity.k3po.nukleus.ext.internal.behavior.types.stream.BeginFW;
import org.reaktivity.k3po.nukleus.ext.internal.behavior.types.stream.DataFW;
import org.reaktivity.k3po.nukleus.ext.internal.behavior.types.stream.EndFW;
import org.reaktivity.k3po.nukleus.ext.internal.behavior.types.stream.FrameFW;
import org.reaktivity.k3po.nukleus.ext.internal.behavior.types.stream.ResetFW;
import org.reaktivity.k3po.nukleus.ext.internal.behavior.types.stream.WindowFW;
import org.reaktivity.k3po.nukleus.ext.internal.util.function.LongObjectBiConsumer;

final class NukleusTarget implements AutoCloseable
{
    private final FrameFW frameRO = new FrameFW();

    private final BeginFW.Builder beginRW = new BeginFW.Builder();
    private final DataFW.Builder dataRW = new DataFW.Builder();
    private final EndFW.Builder endRW = new EndFW.Builder();

    private final WindowFW windowRO = new WindowFW();
    private final ResetFW resetRO = new ResetFW();

    private final Path partitionPath;
    private final Layout layout;
    private final RingBuffer streamsBuffer;
    private final RingBuffer throttleBuffer;
    private final LongFunction<MessageHandler> lookupThrottle;
    private final MessageHandler throttleHandler;
    private final LongObjectBiConsumer<MessageHandler> registerThrottle;
    private final LongConsumer unregisterThrottle;
    private final MutableDirectBuffer writeBuffer;
    private final LongObjectBiConsumer<NukleusCorrelation> correlateNew;

    NukleusTarget(
        Path partitionPath,
        StreamsLayout layout,
        MutableDirectBuffer writeBuffer,
        LongFunction<MessageHandler> lookupThrottle,
        LongObjectBiConsumer<MessageHandler> registerThrottle,
        LongConsumer unregisterThrottle,
        LongObjectBiConsumer<NukleusCorrelation> correlateNew)
    {
        this.partitionPath = partitionPath;
        this.layout = layout;
        this.streamsBuffer = layout.streamsBuffer();
        this.throttleBuffer = layout.throttleBuffer();
        this.writeBuffer = writeBuffer;

        this.lookupThrottle = lookupThrottle;
        this.registerThrottle = registerThrottle;
        this.unregisterThrottle = unregisterThrottle;
        this.correlateNew = correlateNew;
        this.throttleHandler = this::handleThrottle;
    }

    public int process()
    {
        return throttleBuffer.read(throttleHandler);
    }

    @Override
    public void close()
    {
        layout.close();
    }

    @Override
    public String toString()
    {
        return String.format("%s [%s]", getClass().getSimpleName(), partitionPath);
    }

    public void doConnect(
        NukleusClientChannel clientChannel,
        NukleusChannelAddress remoteAddress,
        ChannelFuture connectFuture)
    {
        try
        {
            final String senderName = remoteAddress.getSenderName();
            final long routeRef = remoteAddress.getRoute();
            final long streamId = clientChannel.targetId();
            final long correlationId = new Random().nextLong();
            ChannelFuture handshakeFuture = succeededFuture(clientChannel);

            NukleusChannelConfig clientConfig = clientChannel.getConfig();
            if (clientConfig.isDuplex())
            {
                ChannelFuture correlatedFuture = Channels.future(clientChannel);
                correlateNew.accept(correlationId, new NukleusCorrelation(clientChannel, correlatedFuture));
                handshakeFuture = correlatedFuture;
            }

            ChannelBuffer beginExt = clientChannel.writeExtBuffer();
            final int writableExtBytes = beginExt.readableBytes();
            final byte[] beginExtCopy = writeExtCopy(beginExt);

            final BeginFW begin = beginRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                   .streamId(streamId)
                   .source(senderName)
                   .sourceRef(routeRef)
                   .correlationId(correlationId)
                   .extension(p -> p.set(beginExtCopy))
                   .build();

            if (!clientChannel.isBound())
            {
                ChannelAddress localAddress = remoteAddress.newReplyToAddress();
                clientChannel.setLocalAddress(localAddress);
                clientChannel.setBound();
                fireChannelBound(clientChannel, localAddress);
            }

            clientChannel.setRemoteAddress(remoteAddress);

            handshakeFuture.addListener(new ChannelFutureListener()
            {
                @Override
                public void operationComplete(
                    ChannelFuture future) throws Exception
                {
                    if (future.isSuccess())
                    {
                        clientChannel.setConnected();

                        connectFuture.setSuccess();
                        fireChannelConnected(clientChannel, clientChannel.getRemoteAddress());
                    }
                    else
                    {
                        connectFuture.setFailure(future.getCause());
                    }
                }
            });

            final Throttle throttle = new Throttle(clientChannel, handshakeFuture);
            registerThrottle.accept(begin.streamId(), throttle::handleThrottle);

            streamsBuffer.write(begin.typeId(), begin.buffer(), begin.offset(), begin.sizeof());

            beginExt.skipBytes(writableExtBytes);
            beginExt.discardReadBytes();
        }
        catch (Exception ex)
        {
            connectFuture.setFailure(ex);
        }
    }

    public void onAccepted(
        NukleusChildChannel childChannel,
        long correlationId,
        ChannelFuture handshakeFuture)
    {
        final NukleusChannelAddress remoteAddress = childChannel.getRemoteAddress();
        final String senderName = remoteAddress.getSenderName();
        final ChannelBuffer beginExt = childChannel.writeExtBuffer();
        final int writableExtBytes = beginExt.readableBytes();
        final byte[] beginExtCopy = writeExtCopy(beginExt);

        final long streamId = childChannel.targetId();

        final BeginFW begin = beginRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .streamId(streamId)
                .source(senderName)
                .sourceRef(0L)
                .correlationId(correlationId)
                .extension(p -> p.set(beginExtCopy))
                .build();

        final Throttle throttle = new Throttle(childChannel, handshakeFuture);
        registerThrottle.accept(begin.streamId(), throttle::handleThrottle);

        streamsBuffer.write(begin.typeId(), begin.buffer(), begin.offset(), begin.sizeof());

        beginExt.skipBytes(writableExtBytes);
        beginExt.discardReadBytes();
    }

    public void doWrite(
        NukleusChannel channel,
        MessageEvent newWriteRequest)
    {
        channel.writeRequests.addLast(newWriteRequest);

        flushThrottledWrites(channel);
    }

    public void doFlush(
        NukleusChannel channel,
        ChannelFuture flushFuture)
    {
        Object message = ChannelBuffers.EMPTY_BUFFER;
        MessageEvent newWriteRequest = new DownstreamMessageEvent(channel, flushFuture, message, null);
        channel.writeRequests.addLast(newWriteRequest);

        flushThrottledWrites(channel);
    }

    public void doShutdownOutput(
        NukleusChannel channel,
        ChannelFuture handlerFuture)
    {
        final long streamId = channel.targetId();
        final ChannelBuffer endExt = channel.writeExtBuffer();
        final int writableExtBytes = endExt.readableBytes();
        final byte[] endExtCopy = writeExtCopy(endExt);

        final EndFW end = endRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .streamId(streamId)
                .extension(p -> p.set(endExtCopy))
                .build();

        streamsBuffer.write(end.typeId(), end.buffer(), end.offset(), end.sizeof());

        endExt.skipBytes(writableExtBytes);
        endExt.discardReadBytes();

        fireOutputShutdown(channel);
        handlerFuture.setSuccess();

        if (channel.setWriteClosed())
        {
            fireChannelDisconnected(channel);
            fireChannelUnbound(channel);
            fireChannelClosed(channel);
        }
    }

    public void doClose(
        NukleusChannel channel,
        ChannelFuture handlerFuture)
    {
        final long streamId = channel.targetId();
        final ChannelBuffer endExt = channel.writeExtBuffer();
        final int writableExtBytes = endExt.readableBytes();
        final byte[] endExtCopy = writeExtCopy(endExt);

        final EndFW end = endRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .streamId(streamId)
                .extension(p -> p.set(endExtCopy))
                .build();

        streamsBuffer.write(end.typeId(), end.buffer(), end.offset(), end.sizeof());

        endExt.skipBytes(writableExtBytes);
        endExt.discardReadBytes();

        handlerFuture.setSuccess();

        if (channel.setClosed())
        {
            fireChannelDisconnected(channel);
            fireChannelUnbound(channel);
            fireChannelClosed(channel);
        }
    }

    private void flushThrottledWrites(
        NukleusChannel channel)
    {
        final Deque<MessageEvent> writeRequests = channel.writeRequests;

        while (channel.targetWritable() && !writeRequests.isEmpty())
        {
            MessageEvent writeRequest = writeRequests.peekFirst();
            ChannelBuffer writeBuf = (ChannelBuffer) writeRequest.getMessage();
            ChannelBuffer writeExt = channel.writeExtBuffer();

            if (writeBuf.readable() || writeExt.readable())
            {
                final boolean flushing = !writeBuf.readable();
                final int writableBytes = min(channel.targetWriteableBytes(writeBuf.readableBytes()), (1 << SIZE_OF_SHORT) - 1);
                final int writeReaderIndex = writeBuf.readerIndex();

                final int writableExtBytes = writeExt.readableBytes();
                final byte[] writeExtCopy = writeExtCopy(writeExt);

                // TODO: avoid allocation
                final byte[] writeCopy = new byte[writableBytes];
                writeBuf.getBytes(writeReaderIndex, writeCopy);

                final long streamId = channel.targetId();
                final DataFW data = dataRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                        .streamId(streamId)
                        .payload(p -> p.set(writeCopy))
                        .extension(p -> p.set(writeExtCopy))
                        .build();

                streamsBuffer.write(data.typeId(), data.buffer(), data.offset(), data.sizeof());

                channel.targetWritten(writableBytes, 1);

                writeBuf.skipBytes(writableBytes);

                writeExt.skipBytes(writableExtBytes);
                writeExt.discardReadBytes();

                if (flushing)
                {
                    fireFlushed(channel);
                }
                else
                {
                    fireWriteComplete(channel, writableBytes);
                }
            }

            if (!writeBuf.readable())
            {
                writeRequests.removeFirst();

                ChannelFuture handlerFuture = writeRequest.getFuture();
                handlerFuture.setSuccess();
            }
        }
    }

    private byte[] writeExtCopy(
        ChannelBuffer writeExt)
    {
        final int writableExtBytes = writeExt.readableBytes();
        final byte[] writeExtArray = writeExt.array();
        final int writeExtArrayOffset = writeExt.arrayOffset();
        final int writeExtReaderIndex = writeExt.readerIndex();

        // TODO: avoid allocation
        final byte[] writeExtCopy = new byte[writableExtBytes];
        System.arraycopy(writeExtArray, writeExtArrayOffset + writeExtReaderIndex, writeExtCopy, 0, writeExtCopy.length);
        return writeExtCopy;
    }

    private void handleThrottle(
        int msgTypeId,
        MutableDirectBuffer buffer,
        int index,
        int length)
    {
        frameRO.wrap(buffer, index, index + length);

        final long streamId = frameRO.streamId();

        final MessageHandler handler = lookupThrottle.apply(streamId);

        if (handler != null)
        {
            handler.onMessage(msgTypeId, buffer, index, length);
        }
    }

    private final MutableDirectBuffer resetBuffer = new UnsafeBuffer(new byte[SIZE_OF_LONG]);
    private final ResetFW.Builder resetRW = new ResetFW.Builder();

    private final class Throttle
    {
        private final NukleusChannel channel;

        private Consumer<WindowFW> windowHandler;
        private Consumer<ResetFW> resetHandler;
        private boolean resetPending;

        private Throttle(
            NukleusChannel channel,
            ChannelFuture handshakeFuture)
        {
            this.channel = channel;
            this.windowHandler = this::processWindow;
            this.resetHandler = this::processResetBeforeHandshake;

            handshakeFuture.addListener(this::onHandshakeCompleted);
        }

        private void handleThrottle(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            switch (msgTypeId)
            {
            case WindowFW.TYPE_ID:
                final WindowFW window = windowRO.wrap(buffer, index, index + length);
                windowHandler.accept(window);
                break;
            case ResetFW.TYPE_ID:
                final ResetFW reset = resetRO.wrap(buffer, index, index + length);
                resetHandler.accept(reset);
                break;
            default:
                throw new IllegalArgumentException("Unexpected message type: " + msgTypeId);
            }
        }

        private void processWindow(
            WindowFW window)
        {
            final int update = window.update();
            final int frames = window.frames();

            channel.targetWindowUpdate(update, frames);

            flushThrottledWrites(channel);
        }

        private void processReset(
            ResetFW reset)
        {
            fireChannelAborted(channel);

            final long streamId = reset.streamId();
            unregisterThrottle.accept(streamId);
        }

        private void processResetBeforeHandshake(
            ResetFW reset)
        {
            this.resetPending = true;
            this.resetHandler = this::processReset;
        }

        private void onHandshakeCompleted(
            ChannelFuture future)
        {
            this.resetHandler = this::processReset;

            if (resetPending)
            {
                final long streamId = channel.sourceId();
                final ResetFW reset = resetRW.wrap(resetBuffer, 0, resetBuffer.capacity())
                        .streamId(streamId)
                        .build();

                resetHandler.accept(reset);
            }
        }
    }
}
