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
package org.reaktivity.k3po.nukleus.ext.internal.behavior;

import static org.jboss.netty.channel.Channels.fireChannelBound;
import static org.jboss.netty.channel.Channels.fireChannelConnected;
import static org.jboss.netty.channel.Channels.future;
import static org.reaktivity.k3po.nukleus.ext.internal.behavior.NukleusTransmission.DUPLEX;
import static org.reaktivity.k3po.nukleus.ext.internal.behavior.NukleusTransmission.SIMPLEX;

import java.nio.file.Path;
import java.util.function.BiFunction;
import java.util.function.LongFunction;
import java.util.function.LongSupplier;

import org.agrona.LangUtil;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.MessageHandler;
import org.agrona.concurrent.ringbuffer.RingBuffer;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.reaktivity.k3po.nukleus.ext.internal.behavior.layout.StreamsLayout;
import org.reaktivity.k3po.nukleus.ext.internal.behavior.types.stream.BeginFW;
import org.reaktivity.k3po.nukleus.ext.internal.behavior.types.stream.FrameFW;
import org.reaktivity.k3po.nukleus.ext.internal.behavior.types.stream.ResetFW;
import org.reaktivity.k3po.nukleus.ext.internal.behavior.types.stream.WindowFW;
import org.reaktivity.k3po.nukleus.ext.internal.util.function.LongLongFunction;
import org.reaktivity.k3po.nukleus.ext.internal.util.function.LongObjectBiConsumer;
import org.reaktivity.nukleus.function.MessagePredicate;

final class NukleusPartition implements AutoCloseable
{
    private final FrameFW frameRO = new FrameFW();
    private final BeginFW beginRO = new BeginFW();

    private final WindowFW.Builder windowRW = new WindowFW.Builder();
    private final ResetFW.Builder resetRW = new ResetFW.Builder();

    private final String scopeName;
    private final String sourceName;
    private final Path partitionPath;
    private final StreamsLayout layout;
    private final RingBuffer streamsBuffer;
    private final LongLongFunction<NukleusServerChannel> lookupRoute;
    private final LongFunction<MessageHandler> lookupStream;
    private final LongFunction<MessageHandler> lookupThrottle;
    private final MessageHandler streamHandler;
    private final LongObjectBiConsumer<MessageHandler> registerStream;
    private final MutableDirectBuffer writeBuffer;
    private final NukleusStreamFactory streamFactory;
    private final LongFunction<NukleusCorrelation> correlateEstablished;
    private final BiFunction<String, String, NukleusTarget> supplyTarget;
    private final LongSupplier supplyTimestamp;
    private final LongSupplier supplyTrace;

    private MessagePredicate throttleBuffer;

    NukleusPartition(
        String scopeName,
        String sourceName,
        Path partitionPath,
        StreamsLayout layout,
        LongLongFunction<NukleusServerChannel> lookupRoute,
        LongFunction<MessageHandler> lookupStream,
        LongObjectBiConsumer<MessageHandler> registerStream,
        LongFunction<MessageHandler> lookupThrottle,
        MutableDirectBuffer writeBuffer,
        NukleusStreamFactory streamFactory,
        LongFunction<NukleusCorrelation> correlateEstablished,
        BiFunction<String, String, NukleusTarget> supplyTarget,
        LongSupplier supplyTimestamp,
        LongSupplier supplyTrace)
    {
        this.scopeName = scopeName;
        this.sourceName = sourceName;
        this.partitionPath = partitionPath;
        this.layout = layout;
        this.streamsBuffer = layout.streamsBuffer();
        this.writeBuffer = writeBuffer;

        this.lookupRoute = lookupRoute;
        this.lookupStream = lookupStream;
        this.lookupThrottle = lookupThrottle;
        this.registerStream = registerStream;
        this.streamHandler = this::handleStream;
        this.streamFactory = streamFactory;
        this.correlateEstablished = correlateEstablished;
        this.supplyTarget = supplyTarget;
        this.supplyTimestamp = supplyTimestamp;
        this.supplyTrace = supplyTrace;
    }

    public int process()
    {
        return streamsBuffer.read(streamHandler);
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

    private void handleStream(
        int msgTypeId,
        MutableDirectBuffer buffer,
        int index,
        int length)
    {
        final FrameFW frame = frameRO.wrap(buffer, index, index + length);
        final long streamId = frame.streamId();

        if ((msgTypeId & 0x4000_0000) != 0)
        {
            final MessageHandler handler = lookupThrottle.apply(streamId);

            if (handler != null)
            {
                handler.onMessage(msgTypeId, buffer, index, length);
            }
        }
        else
        {
            final MessageHandler handler = lookupStream.apply(streamId);

            if (handler != null)
            {
                handler.onMessage(msgTypeId, buffer, index, length);
            }
            else
            {
                handleUnrecognized(msgTypeId, buffer, index, length);
            }
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
            final BeginFW begin = beginRO.wrap(buffer, index, index + length);
            handleBegin(begin);
        }
        else
        {
            final FrameFW frame = frameRO.wrap(buffer, index, index + length);

            final long streamId = frame.streamId();

            doReset(streamId);
        }
    }

    private void handleBegin(
        BeginFW begin)
    {
        final long sourceRef = begin.sourceRef();
        final long sourceId = begin.streamId();
        final NukleusServerChannel serverChannel = lookupRoute.apply(sourceRef, begin.authorization());

        if (serverChannel != null)
        {
            handleBeginInitial(begin, serverChannel);
        }
        else
        {
            if (sourceRef == 0L)
            {
                handleBeginReply(begin);
            }
            else
            {
                doReset(sourceId);
            }
        }
    }

    private void handleBeginInitial(
        final BeginFW begin,
        final NukleusServerChannel serverChannel)
    {
        final long sourceId = begin.streamId();
        final long correlationId = begin.correlationId();
        final long replyId = sourceId | 0x8000_0000_0000_0000L;
        NukleusChildChannel childChannel = doAccept(serverChannel, replyId, correlationId);

        final ChannelFuture handshakeFuture = future(childChannel);
        final MessageHandler newStream = streamFactory.newStream(childChannel, this, handshakeFuture);
        registerStream.accept(sourceId, newStream);
        newStream.onMessage(begin.typeId(), (MutableDirectBuffer) begin.buffer(), begin.offset(), begin.sizeof());

        fireChannelBound(childChannel, childChannel.getLocalAddress());

        NukleusChannelConfig childConfig = childChannel.getConfig();
        NukleusChannelAddress remoteAddress = childChannel.getRemoteAddress();
        String receiverName = remoteAddress.getReceiverName();
        String senderName = remoteAddress.getSenderName();
        NukleusTarget remoteTarget = supplyTarget.apply(receiverName, senderName);

        if (childConfig.getTransmission() == DUPLEX)
        {
            remoteTarget.doBeginReply(childChannel, handshakeFuture);
        }

        fireChannelConnected(childChannel, childChannel.getRemoteAddress());
    }

    private void handleBeginReply(
        final BeginFW begin)
    {
        final long correlationId = begin.correlationId();
        final long sourceId = begin.streamId();
        final NukleusCorrelation correlation = correlateEstablished.apply(correlationId);

        if (correlation != null)
        {
            final ChannelFuture handshakeFuture = correlation.correlatedFuture();
            final NukleusClientChannel clientChannel = (NukleusClientChannel) handshakeFuture.getChannel();

            final MessageHandler newStream = streamFactory.newStream(clientChannel, this, handshakeFuture);
            registerStream.accept(sourceId, newStream);

            newStream.onMessage(begin.typeId(), (MutableDirectBuffer) begin.buffer(), begin.offset(), begin.sizeof());
        }
        else
        {
            doReset(sourceId);
        }
    }

    void doWindow(
        final NukleusChannel channel,
        final int credit,
        final int padding,
        final long groupId)
    {
        final long streamId = channel.sourceId();

        channel.readableBytes(credit);

        final WindowFW window = windowRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .streamId(streamId)
                .timestamp(supplyTimestamp.getAsLong())
                .trace(supplyTrace.getAsLong())
                .credit(credit)
                .padding(padding)
                .groupId(groupId)
                .build();

        throttleBuffer().test(window.typeId(), window.buffer(), window.offset(), window.sizeof());
    }

    void doReset(
        final long streamId)
    {
        final ResetFW reset = resetRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .streamId(streamId)
                .timestamp(supplyTimestamp.getAsLong())
                .trace(supplyTrace.getAsLong())
                .build();

        throttleBuffer().test(reset.typeId(), reset.buffer(), reset.offset(), reset.sizeof());
    }

    private NukleusChildChannel doAccept(
        NukleusServerChannel serverChannel,
        long targetId,
        long correlationId)
    {
        try
        {
            NukleusServerChannelConfig serverConfig = serverChannel.getConfig();
            ChannelPipelineFactory pipelineFactory = serverConfig.getPipelineFactory();
            ChannelPipeline pipeline = pipelineFactory.getPipeline();

            final NukleusChannelAddress serverAddress = serverChannel.getLocalAddress();
            NukleusChannelAddress remoteAddress = serverAddress.newReplyToAddress();

            // fire child serverChannel opened
            ChannelFactory channelFactory = serverChannel.getFactory();
            NukleusChildChannelSink childSink = new NukleusChildChannelSink();
            NukleusChildChannel childChannel =
                  new NukleusChildChannel(serverChannel, channelFactory, pipeline, childSink, serverChannel.reaktor, targetId);

            NukleusChannelConfig childConfig = childChannel.getConfig();
            childConfig.setBufferFactory(serverConfig.getBufferFactory());
            childConfig.setTransmission(serverConfig.getTransmission());
            childConfig.setThrottle(serverConfig.getThrottle());
            childConfig.setWindow(serverConfig.getWindow());
            childConfig.setGroup(serverConfig.getGroup());
            childConfig.setPadding(serverConfig.getPadding());
            childConfig.setAlignment(serverConfig.getAlignment());

            childConfig.setCorrelation(correlationId);

            if (childConfig.getTransmission() == SIMPLEX)
            {
                childChannel.setWriteClosed();
            }

            childChannel.setLocalAddress(serverAddress);
            childChannel.setRemoteAddress(remoteAddress);

            return childChannel;
        }
        catch (Exception ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }

        // unreachable
        return null;
    }

    private MessagePredicate throttleBuffer()
    {
        if (throttleBuffer == null)
        {
            // defer to let owner scope create buffer
            NukleusTarget replyTarget = supplyTarget.apply(sourceName, scopeName);
            throttleBuffer = (t, b, o, l) -> replyTarget.streamsBuffer().write(t, b, o, l);
            assert throttleBuffer != null;
        }

        return throttleBuffer;
    }
}
