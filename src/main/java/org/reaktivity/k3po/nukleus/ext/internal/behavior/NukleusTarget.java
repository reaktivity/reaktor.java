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

import static java.util.Arrays.asList;
import static org.jboss.netty.channel.Channels.fireChannelClosed;
import static org.jboss.netty.channel.Channels.fireChannelConnected;
import static org.jboss.netty.channel.Channels.fireChannelDisconnected;
import static org.jboss.netty.channel.Channels.fireChannelInterestChanged;
import static org.jboss.netty.channel.Channels.fireChannelUnbound;
import static org.jboss.netty.channel.Channels.fireWriteComplete;
import static org.jboss.netty.channel.Channels.future;
import static org.jboss.netty.channel.Channels.succeededFuture;
import static org.kaazing.k3po.driver.internal.netty.channel.Channels.fireFlushed;
import static org.kaazing.k3po.driver.internal.netty.channel.Channels.fireOutputAborted;
import static org.kaazing.k3po.driver.internal.netty.channel.Channels.fireOutputShutdown;
import static org.reaktivity.k3po.nukleus.ext.internal.behavior.NukleusExtensionKind.BEGIN;
import static org.reaktivity.k3po.nukleus.ext.internal.behavior.NukleusExtensionKind.DATA;
import static org.reaktivity.k3po.nukleus.ext.internal.behavior.NukleusExtensionKind.END;
import static org.reaktivity.k3po.nukleus.ext.internal.behavior.NullChannelBuffer.NULL_BUFFER;

import java.nio.file.Path;
import java.util.Deque;
import java.util.function.Consumer;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.MessageHandler;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.ringbuffer.RingBuffer;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.ChannelException;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.DownstreamMessageEvent;
import org.jboss.netty.channel.MessageEvent;
import org.kaazing.k3po.driver.internal.netty.channel.CompositeChannelFuture;
import org.reaktivity.k3po.nukleus.ext.internal.behavior.layout.Layout;
import org.reaktivity.k3po.nukleus.ext.internal.behavior.layout.StreamsLayout;
import org.reaktivity.k3po.nukleus.ext.internal.behavior.types.OctetsFW;
import org.reaktivity.k3po.nukleus.ext.internal.behavior.types.stream.AbortFW;
import org.reaktivity.k3po.nukleus.ext.internal.behavior.types.stream.BeginFW;
import org.reaktivity.k3po.nukleus.ext.internal.behavior.types.stream.DataFW;
import org.reaktivity.k3po.nukleus.ext.internal.behavior.types.stream.EndFW;
import org.reaktivity.k3po.nukleus.ext.internal.behavior.types.stream.ResetFW;
import org.reaktivity.k3po.nukleus.ext.internal.behavior.types.stream.WindowFW;
import org.reaktivity.k3po.nukleus.ext.internal.util.function.LongObjectBiConsumer;

final class NukleusTarget implements AutoCloseable
{
    private final BeginFW.Builder beginRW = new BeginFW.Builder();
    private final DataFW.Builder dataRW = new DataFW.Builder();
    private final EndFW.Builder endRW = new EndFW.Builder();
    private final AbortFW.Builder abortRW = new AbortFW.Builder();

    private final WindowFW windowRO = new WindowFW();
    private final ResetFW resetRO = new ResetFW();
    private final OctetsFW octetsRO = new OctetsFW();

    private final MutableDirectBuffer resetBuffer = new UnsafeBuffer(new byte[ResetFW.FIELD_OFFSET_EXTENSION]);
    private final ResetFW.Builder resetRW = new ResetFW.Builder();
    private final WindowFW.Builder windowRW = new WindowFW.Builder();

    private final Path streamsPath;
    private final Layout layout;
    private final RingBuffer streamsBuffer;
    private final LongObjectBiConsumer<MessageHandler> registerThrottle;
    private final LongConsumer unregisterThrottle;
    private final MutableDirectBuffer writeBuffer;
    private final LongObjectBiConsumer<NukleusCorrelation> correlateNew;
    private final LongSupplier supplyTimestamp;
    private final LongSupplier supplyTrace;

    NukleusTarget(
        Path streamsPath,
        StreamsLayout layout,
        MutableDirectBuffer writeBuffer,
        LongObjectBiConsumer<MessageHandler> registerThrottle,
        LongConsumer unregisterThrottle,
        LongObjectBiConsumer<NukleusCorrelation> correlateNew,
        LongSupplier supplyTimestamp,
        LongSupplier supplyTrace)
    {
        this.streamsPath = streamsPath;
        this.layout = layout;
        this.streamsBuffer = layout.streamsBuffer();
        this.writeBuffer = writeBuffer;

        this.registerThrottle = registerThrottle;
        this.unregisterThrottle = unregisterThrottle;
        this.correlateNew = correlateNew;
        this.supplyTimestamp = supplyTimestamp;
        this.supplyTrace = supplyTrace;
    }

    @Override
    public void close()
    {
        layout.close();
    }

    @Override
    public String toString()
    {
        return String.format("%s [%s]", getClass().getSimpleName(), streamsPath);
    }

    public RingBuffer streamsBuffer()
    {
        return streamsBuffer;
    }

    public void doConnect(
        NukleusClientChannel clientChannel,
        NukleusChannelAddress localAddress,
        NukleusChannelAddress remoteAddress,
        ChannelFuture connectFuture)
    {
        try
        {
            final long routeId = clientChannel.routeId();
            final long initialId = clientChannel.targetId();
            final long replyId = initialId & 0xffff_ffff_ffff_fffeL;
            clientChannel.sourceId(replyId);

            ChannelFuture windowFuture = future(clientChannel);
            ChannelFuture replyFuture = succeededFuture(clientChannel);

            NukleusChannelConfig clientConfig = clientChannel.getConfig();
            switch (clientConfig.getTransmission())
            {
            case DUPLEX:
            {
                ChannelFuture correlatedFuture = clientChannel.beginInputFuture();
                correlateNew.accept(replyId, new NukleusCorrelation(clientChannel, correlatedFuture));
                replyFuture = correlatedFuture;
                break;
            }
            case HALF_DUPLEX:
            {
                ChannelFuture correlatedFuture = clientChannel.beginInputFuture();
                correlateNew.accept(replyId, new NukleusCorrelation(clientChannel, correlatedFuture));
                correlatedFuture.addListener(f -> fireChannelInterestChanged(f.getChannel()));
                break;
            }
            default:
                break;
            }
            long authorization = remoteAddress.getAuthorization();
            clientChannel.targetAuth(authorization);

            ChannelBuffer beginExt = clientChannel.writeExtBuffer(BEGIN, true);
            final int writableExtBytes = beginExt.readableBytes();
            final byte[] beginExtCopy = writeExtCopy(beginExt);

            final BeginFW begin = beginRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                   .routeId(routeId)
                   .streamId(initialId)
                   .timestamp(supplyTimestamp.getAsLong())
                   .trace(supplyTrace.getAsLong())
                   .authorization(authorization)
                   .extension(p -> p.set(beginExtCopy))
                   .build();

            clientChannel.setRemoteAddress(remoteAddress);

            ChannelFuture handshakeFuture = new CompositeChannelFuture<>(clientChannel, asList(windowFuture, replyFuture));
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

            final Throttle throttle = new Throttle(clientChannel, windowFuture, handshakeFuture);
            registerThrottle.accept(begin.streamId(), throttle::handleThrottle);

            streamsBuffer.write(begin.typeId(), begin.buffer(), begin.offset(), begin.sizeof());

            beginExt.skipBytes(writableExtBytes);
            beginExt.discardReadBytes();

            clientChannel.beginOutputFuture().setSuccess();
        }
        catch (Exception ex)
        {
            connectFuture.setFailure(ex);
        }
    }

    public void doConnectAbort(
        NukleusClientChannel clientChannel)
    {
        final long routeId = clientChannel.routeId();
        final long initialId = clientChannel.targetId();

        final AbortFW abort = abortRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(initialId)
                .timestamp(supplyTimestamp.getAsLong())
                .trace(supplyTrace.getAsLong())
                .build();

        streamsBuffer.write(abort.typeId(), abort.buffer(), abort.offset(), abort.sizeof());
    }

    public void doPrepareReply(
        NukleusChannel channel,
        ChannelFuture windowFuture,
        ChannelFuture handshakeFuture)
    {
        final Throttle throttle = new Throttle(channel, windowFuture, handshakeFuture);
        registerThrottle.accept(channel.targetId(), throttle::handleThrottle);
    }

    public void doBeginReply(
        NukleusChannel channel)
    {
        final ChannelBuffer beginExt = channel.writeExtBuffer(BEGIN, true);
        final int writableExtBytes = beginExt.readableBytes();
        final byte[] beginExtCopy = writeExtCopy(beginExt);

        final long routeId = channel.routeId();
        final long replyId = channel.targetId();

        final BeginFW begin = beginRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(replyId)
                .timestamp(supplyTimestamp.getAsLong())
                .trace(supplyTrace.getAsLong())
                .extension(p -> p.set(beginExtCopy))
                .build();

        streamsBuffer.write(begin.typeId(), begin.buffer(), begin.offset(), begin.sizeof());

        beginExt.skipBytes(writableExtBytes);
        beginExt.discardReadBytes();

        channel.beginOutputFuture().setSuccess();
    }

    public void doWrite(
        NukleusChannel channel,
        MessageEvent newWriteRequest)
    {
        doFlushBegin(channel);

        channel.writeRequests.addLast(newWriteRequest);

        flushThrottledWrites(channel);
    }

    public void doFlush(
        NukleusChannel channel,
        ChannelFuture flushFuture)
    {
        doFlushBegin(channel);
        if (channel.writeExtBuffer(DATA, true).readable())
        {
            if (channel.writeRequests.isEmpty())
            {
                Object message = NULL_BUFFER;
                MessageEvent newWriteRequest = new DownstreamMessageEvent(channel, flushFuture, message, null);
                channel.writeRequests.addLast(newWriteRequest);
            }
            flushThrottledWrites(channel);
        }
        else
        {
            flushFuture.setSuccess();
            fireFlushed(channel);
        }
    }

    public void doAbortOutput(
        NukleusChannel channel,
        ChannelFuture abortFuture)
    {
        doFlushBegin(channel);

        final long routeId = channel.routeId();
        final long streamId = channel.targetId();
        final long authorization = channel.targetAuth();

        final AbortFW abort = abortRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(streamId)
                .timestamp(supplyTimestamp.getAsLong())
                .trace(supplyTrace.getAsLong())
                .authorization(authorization)
                .build();

        streamsBuffer.write(abort.typeId(), abort.buffer(), abort.offset(), abort.sizeof());

        abortFuture.setSuccess();

        if (channel.setWriteAborted())
        {
            if (channel.setWriteClosed())
            {
                fireChannelDisconnected(channel);
                fireChannelUnbound(channel);
                fireChannelClosed(channel);
            }
        }
    }

    public void doShutdownOutput(
        NukleusChannel channel,
        ChannelFuture handlerFuture)
    {
        doFlushBegin(channel);

        final long routeId = channel.routeId();
        final long streamId = channel.targetId();
        final ChannelBuffer endExt = channel.writeExtBuffer(END, true);
        final int writableExtBytes = endExt.readableBytes();
        final byte[] endExtCopy = writeExtCopy(endExt);
        final long authorization = channel.targetAuth();

        final EndFW end = endRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(streamId)
                .timestamp(supplyTimestamp.getAsLong())
                .trace(supplyTrace.getAsLong())
                .authorization(authorization)
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
        doFlushBegin(channel);

        final long routeId = channel.routeId();
        final long streamId = channel.targetId();
        final ChannelBuffer endExt = channel.writeExtBuffer(END, true);
        final int writableExtBytes = endExt.readableBytes();
        final byte[] endExtCopy = writeExtCopy(endExt);

        final EndFW end = endRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(streamId)
                .timestamp(supplyTimestamp.getAsLong())
                .trace(supplyTrace.getAsLong())
                .authorization(channel.targetAuth())
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

    private boolean doFlushBegin(
        NukleusChannel channel)
    {
        final boolean doFlush = !channel.beginOutputFuture().isDone();

        if (doFlush)
        {
            // SERVER, HALF_DUPLEX
            doBeginReply(channel);
        }

        return doFlush;
    }

    private void flushThrottledWrites(
        NukleusChannel channel)
    {
        final Deque<MessageEvent> writeRequests = channel.writeRequests;
        final long authorization = channel.targetAuth();

        loop:
        while (channel.writable() && !writeRequests.isEmpty())
        {
            MessageEvent writeRequest = writeRequests.peekFirst();
            ChannelBuffer writeBuf = (ChannelBuffer) writeRequest.getMessage();
            ChannelBuffer writeExt = channel.writeExtBuffer(DATA, true);

            if (writeBuf.readable() || writeExt.readable())
            {
                final boolean flushing = writeBuf == NULL_BUFFER;
                final int writableBytes = channel.writableBytes(writeBuf.readableBytes());

                // allow extension-only DATA frames to be flushed immediately
                if (writableBytes > 0 || !writeBuf.readable())
                {
                    final int writeReaderIndex = writeBuf.readerIndex();

                    if (writeReaderIndex == 0)
                    {
                        channel.targetWriteRequestProgressing();
                    }

                    final int writableExtBytes = writeExt.readableBytes();
                    final byte[] writeExtCopy = writeExtCopy(writeExt);

                    // extension-only DATA frames should have null payload (default)
                    OctetsFW writeCopy = null;
                    if (writeBuf != NULL_BUFFER)
                    {
                        // TODO: avoid allocation
                        byte[] writeCopyBytes = new byte[writableBytes];
                        writeBuf.getBytes(writeReaderIndex, writeCopyBytes);
                        writeCopy = octetsRO.wrap(new UnsafeBuffer(writeCopyBytes), 0, writableBytes);
                    }

                    int flags = 0;

                    if (writableBytes == writeBuf.readableBytes())
                    {
                        flags |= 0x01;  // FIN
                    }

                    if (writeReaderIndex == 0)
                    {
                        flags |= 0x02;  // INIT
                    }

                    final long streamId = channel.targetId();
                    final long routeId = channel.routeId();

                    final DataFW data = dataRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                            .routeId(routeId)
                            .streamId(streamId)
                            .timestamp(supplyTimestamp.getAsLong())
                            .trace(supplyTrace.getAsLong())
                            .authorization(authorization)
                            .flags(flags)
                            .groupId(0)
                            .padding(channel.writablePadding)
                            .payload(writeCopy)
                            .extension(p -> p.set(writeExtCopy))
                            .build();

                    streamsBuffer.write(data.typeId(), data.buffer(), data.offset(), data.sizeof());

                    channel.writtenBytes(writableBytes);

                    writeBuf.skipBytes(writableBytes);

                    writeExt.skipBytes(writableExtBytes);
                    writeExt.discardReadBytes();
                }

                if (flushing)
                {
                    fireFlushed(channel);
                }
                else
                {
                    fireWriteComplete(channel, writableBytes);
                }

                channel.targetWriteRequestProgress();
            }
            else if (channel.isTargetWriteRequestInProgress())
            {
                break loop;
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

    void doWindow(
        final NukleusChannel channel,
        final int credit,
        final int padding,
        final long groupId)
    {
        final long routeId = channel.routeId();
        final long streamId = channel.sourceId();

        channel.readableBytes(credit);

        final WindowFW window = windowRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(streamId)
                .timestamp(supplyTimestamp.getAsLong())
                .trace(supplyTrace.getAsLong())
                .credit(credit)
                .padding(padding)
                .groupId(groupId)
                .build();

        streamsBuffer.write(window.typeId(), window.buffer(), window.offset(), window.sizeof());
    }

    void doReset(
        final NukleusChannel channel)
    {
        final long routeId = channel.routeId();
        final long streamId = channel.sourceId();

        doReset(routeId, streamId);
    }

    void doReset(
        final long routeId,
        final long streamId)
    {
        final ResetFW reset = resetRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(streamId)
                .timestamp(supplyTimestamp.getAsLong())
                .trace(supplyTrace.getAsLong())
                .build();

        streamsBuffer.write(reset.typeId(), reset.buffer(), reset.offset(), reset.sizeof());
    }

    private final class Throttle
    {
        private final NukleusChannel channel;
        private final ChannelFuture windowFuture;
        private final ChannelFuture handshakeFuture;

        private Consumer<WindowFW> windowHandler;
        private Consumer<ResetFW> resetHandler;

        private Throttle(
            NukleusChannel channel,
            ChannelFuture windowFuture,
            ChannelFuture handshakeFuture)
        {
            this.channel = channel;
            this.windowHandler = this::processWindowBeforeWritable;
            this.windowFuture = windowFuture;
            this.handshakeFuture = handshakeFuture;

            boolean isChildChannel = channel.getParent() != null;
            boolean isHalfDuplex = channel.getConfig().getTransmission() == NukleusTransmission.HALF_DUPLEX;

            this.resetHandler = isChildChannel && isHalfDuplex ? this::processReset : this::processResetBeforeHandshake;
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
            final int credit = window.credit();
            final int padding = window.padding();
            channel.writableWindow(credit, padding);

            flushThrottledWrites(channel);
        }

        private void processReset(
            ResetFW reset)
        {
            final long streamId = reset.streamId();
            unregisterThrottle.accept(streamId);

            if (channel.setWriteAborted())
            {
                if (channel.setWriteClosed())
                {
                    fireOutputAborted(channel);
                    fireChannelDisconnected(channel);
                    fireChannelUnbound(channel);
                    fireChannelClosed(channel);
                }
                else
                {
                    fireOutputAborted(channel);
                }
            }
        }

        private void processWindowBeforeWritable(
            WindowFW window)
        {
            this.windowHandler = this::processWindow;

            windowFuture.setSuccess();
            windowHandler.accept(window);
        }

        private void processResetBeforeHandshake(
            ResetFW reset)
        {
            handshakeFuture.setFailure(new ChannelException("handshake failed"));
        }

        private void onHandshakeCompleted(
            ChannelFuture future)
        {
            this.resetHandler = this::processReset;

            if (!future.isSuccess())
            {
                final long streamId = channel.sourceId();
                final long routeId = channel.routeId();

                final ResetFW reset = resetRW.wrap(resetBuffer, 0, resetBuffer.capacity())
                        .routeId(routeId)
                        .streamId(streamId)
                        .timestamp(supplyTimestamp.getAsLong())
                        .trace(supplyTrace.getAsLong())
                        .build();

                resetHandler.accept(reset);
            }
        }
    }
}
