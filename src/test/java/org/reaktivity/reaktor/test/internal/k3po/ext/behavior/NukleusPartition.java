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

import static org.jboss.netty.channel.Channels.fireChannelBound;
import static org.jboss.netty.channel.Channels.fireChannelConnected;
import static org.jboss.netty.channel.Channels.future;
import static org.reaktivity.reaktor.internal.router.BudgetId.budgetMask;
import static org.reaktivity.reaktor.internal.router.BudgetId.ownerIndex;
import static org.reaktivity.reaktor.test.internal.k3po.ext.behavior.NukleusExtensionKind.BEGIN;
import static org.reaktivity.reaktor.test.internal.k3po.ext.behavior.NukleusTransmission.SIMPLEX;

import java.nio.file.Path;
import java.util.function.IntFunction;
import java.util.function.LongFunction;

import org.agrona.DirectBuffer;
import org.agrona.LangUtil;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.MessageHandler;
import org.agrona.concurrent.ringbuffer.RingBuffer;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.kaazing.k3po.driver.internal.behavior.handler.RejectedHandler;
import org.reaktivity.reaktor.internal.budget.DefaultBudgetCreditor;
import org.reaktivity.reaktor.internal.types.stream.DataFW;
import org.reaktivity.reaktor.test.internal.k3po.ext.behavior.layout.StreamsLayout;
import org.reaktivity.reaktor.test.internal.k3po.ext.types.OctetsFW;
import org.reaktivity.reaktor.test.internal.k3po.ext.types.stream.BeginFW;
import org.reaktivity.reaktor.test.internal.k3po.ext.types.stream.FrameFW;
import org.reaktivity.reaktor.test.internal.k3po.ext.util.function.LongLongFunction;
import org.reaktivity.reaktor.test.internal.k3po.ext.util.function.LongObjectBiConsumer;

final class NukleusPartition implements AutoCloseable
{
    private final FrameFW frameRO = new FrameFW();
    private final BeginFW beginRO = new BeginFW();
    private final DataFW dataRO = new DataFW();

    private final LabelManager labels;
    private final Path streamsPath;
    private final int scopeIndex;
    private final StreamsLayout layout;
    private final RingBuffer streamsBuffer;
    private final LongLongFunction<NukleusServerChannel> lookupRoute;
    private final LongFunction<MessageHandler> lookupStream;
    private final LongFunction<MessageHandler> lookupThrottle;
    private final MessageHandler streamHandler;
    private final LongObjectBiConsumer<MessageHandler> registerStream;
    private final NukleusStreamFactory streamFactory;
    private final LongFunction<NukleusCorrelation> correlateEstablished;
    private final LongLongFunction<NukleusTarget> supplySender;
    private final IntFunction<NukleusTarget> supplyTarget;

    NukleusPartition(
        LabelManager labels,
        Path streamsPath,
        int scopeIndex,
        StreamsLayout layout,
        LongLongFunction<NukleusServerChannel> lookupRoute,
        LongFunction<MessageHandler> lookupStream,
        LongObjectBiConsumer<MessageHandler> registerStream,
        LongFunction<MessageHandler> lookupThrottle,
        NukleusStreamFactory streamFactory,
        LongFunction<NukleusCorrelation> correlateEstablished,
        LongLongFunction<NukleusTarget> supplySender,
        IntFunction<NukleusTarget> supplyTarget)
    {
        this.labels = labels;
        this.streamsPath = streamsPath;
        this.scopeIndex = scopeIndex;
        this.layout = layout;
        this.streamsBuffer = layout.streamsBuffer();

        this.lookupRoute = lookupRoute;
        this.lookupStream = lookupStream;
        this.lookupThrottle = lookupThrottle;
        this.registerStream = registerStream;
        this.streamHandler = this::handleStream;
        this.streamFactory = streamFactory;
        this.correlateEstablished = correlateEstablished;
        this.supplySender = supplySender;
        this.supplyTarget = supplyTarget;
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
        return String.format("%s [%s]", getClass().getSimpleName(), streamsPath);
    }

    void doSystemWindow(
        NukleusChannel channel,
        long traceId)
    {
        final int pendingSharedBudget = channel.pendingSharedBudget();

        if (pendingSharedBudget != 0)
        {
            final long budgetId = channel.creditorId();
            assert budgetId != 0L;

            final int ownerIndex = ownerIndex(budgetId);
            final NukleusTarget target = supplyTarget.apply(ownerIndex);

            target.doSystemWindow(traceId, budgetId, pendingSharedBudget);
        }
    }

    int scopeIndex()
    {
        return scopeIndex;
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
        switch (msgTypeId)
        {
        case BeginFW.TYPE_ID:
            final BeginFW begin = beginRO.wrap(buffer, index, index + length);
            handleBegin(begin);
            break;
        case DataFW.TYPE_ID:
            final DataFW data = dataRO.wrap(buffer, index, index + length);
            final long traceId = data.traceId();
            final long budgetId = data.budgetId();
            if (budgetId != 0L)
            {
                final int reserved = data.reserved();
                final int ownerIndex = ownerIndex(budgetId);
                final NukleusTarget target = supplyTarget.apply(ownerIndex);

                target.doSystemWindow(traceId, budgetId, reserved);
            }
            break;
        }
    }

    private void handleBegin(
        BeginFW begin)
    {
        final long routeId = begin.routeId();
        final long streamId = begin.streamId();
        final long sequence = begin.sequence();
        final long acknowledge = begin.acknowledge();
        final long traceId = begin.traceId();
        final long authorization = begin.authorization();

        if ((streamId & 0x0000_0000_0000_0001L) != 0L)
        {
            final NukleusServerChannel serverChannel = lookupRoute.apply(routeId, authorization);
            if (serverChannel != null)
            {
                handleBeginInitial(begin, serverChannel);
            }
            else
            {
                supplySender.apply(routeId, streamId).doReset(routeId, streamId, sequence, acknowledge, traceId);
            }
        }
        else
        {
            handleBeginReply(begin);
        }
    }

    private void handleBeginInitial(
        final BeginFW begin,
        final NukleusServerChannel serverChannel)
    {
        final long routeId = begin.routeId();
        final long initialId = begin.streamId();
        final long sequence = begin.sequence();
        final long acknowledge = begin.acknowledge();
        final long traceId = begin.traceId();
        final long replyId = initialId & 0xffff_ffff_ffff_fffeL;

        final NukleusChildChannel childChannel = doAccept(serverChannel, routeId, initialId, replyId);
        final NukleusTarget sender = supplySender.apply(routeId, initialId);
        final ChannelPipeline pipeline = childChannel.getPipeline();

        if (pipeline.get(RejectedHandler.class) != null)
        {
            final OctetsFW beginExt = begin.extension();
            int beginExtBytes = beginExt.sizeof();
            if (beginExtBytes != 0)
            {
                final DirectBuffer buffer = beginExt.buffer();
                final int offset = beginExt.offset();

                // TODO: avoid allocation
                final byte[] beginExtCopy = new byte[beginExtBytes];
                buffer.getBytes(offset, beginExtCopy);

                childChannel.readExtBuffer(BEGIN).writeBytes(beginExtCopy);
            }

            childChannel.setWriteClosed();

            fireChannelBound(childChannel, childChannel.getLocalAddress());

            sender.doReset(routeId, initialId, sequence, acknowledge, traceId);

            childChannel.setReadClosed();
        }
        else
        {
            final ChannelFuture beginFuture = future(childChannel);
            final ChannelFuture windowFuture = future(childChannel);

            final MessageHandler newStream = streamFactory.newStream(childChannel, sender, beginFuture);
            registerStream.accept(initialId, newStream);
            newStream.onMessage(begin.typeId(), (MutableDirectBuffer) begin.buffer(), begin.offset(), begin.sizeof());

            fireChannelBound(childChannel, childChannel.getLocalAddress());

            ChannelFuture handshakeFuture = beginFuture;

            sender.doPrepareReply(childChannel, windowFuture, handshakeFuture);

            NukleusChannelConfig childConfig = childChannel.getConfig();
            switch (childConfig.getTransmission())
            {
            case DUPLEX:
                sender.doBeginReply(childChannel);
                break;
            default:
                windowFuture.setSuccess();
                break;
            }

            fireChannelConnected(childChannel, childChannel.getRemoteAddress());
        }
    }

    private void handleBeginReply(
        final BeginFW begin)
    {
        final long routeId = begin.routeId();
        final long replyId = begin.streamId();
        final long sequence = begin.sequence();
        final long acknowledge = begin.acknowledge();
        final long traceId = begin.traceId();
        final NukleusCorrelation correlation = correlateEstablished.apply(replyId);
        final NukleusTarget sender = supplySender.apply(routeId, replyId);

        if (correlation != null)
        {
            final ChannelFuture beginFuture = correlation.correlatedFuture();
            final NukleusClientChannel clientChannel = (NukleusClientChannel) beginFuture.getChannel();

            final MessageHandler newStream = streamFactory.newStream(clientChannel, sender, beginFuture);
            registerStream.accept(replyId, newStream);

            newStream.onMessage(begin.typeId(), (MutableDirectBuffer) begin.buffer(), begin.offset(), begin.sizeof());
        }
        else
        {
            sender.doReset(routeId, replyId, sequence, acknowledge, traceId);
        }
    }

    private NukleusChildChannel doAccept(
        NukleusServerChannel serverChannel,
        long routeId,
        long initialId,
        long replyId)
    {
        try
        {
            NukleusServerChannelConfig serverConfig = serverChannel.getConfig();
            ChannelPipelineFactory pipelineFactory = serverConfig.getPipelineFactory();
            ChannelPipeline pipeline = pipelineFactory.getPipeline();

            final NukleusChannelAddress serverAddress = serverChannel.getLocalAddress();
            final String replyAddress = labels.lookupLabel((int)(routeId >> 48) & 0xffff);
            NukleusChannelAddress remoteAddress = serverAddress.newReplyToAddress(replyAddress);

            // fire child serverChannel opened
            ChannelFactory channelFactory = serverChannel.getFactory();
            NukleusChildChannelSink childSink = new NukleusChildChannelSink();
            NukleusChildChannel childChannel =
                    new NukleusChildChannel(serverChannel, channelFactory, pipeline, childSink, initialId, replyId);

            NukleusChannelConfig childConfig = childChannel.getConfig();
            childConfig.setBufferFactory(serverConfig.getBufferFactory());
            childConfig.setTransmission(serverConfig.getTransmission());
            childConfig.setThrottle(serverConfig.getThrottle());
            childConfig.setWindow(serverConfig.getWindow());
            childConfig.setBudgetId(serverConfig.getBudgetId());
            childConfig.setPadding(serverConfig.getPadding());
            childConfig.setAlignment(serverConfig.getAlignment());
            childConfig.setCapabilities(serverConfig.getCapabilities());

            if (childConfig.getTransmission() == SIMPLEX)
            {
                childChannel.setWriteClosed();
            }

            childChannel.routeId(routeId);
            childChannel.setLocalAddress(serverAddress);
            childChannel.setRemoteAddress(remoteAddress);

            final long budgetId = childConfig.getBudgetId();
            if (budgetId != 0L)
            {
                final long creditorId = budgetId | budgetMask(scopeIndex);

                DefaultBudgetCreditor creditor = serverChannel.reaktor.supplyCreditor(childChannel);
                childChannel.setCreditor(creditor, creditorId);

                final int sharedWindow = childConfig.getSharedWindow();
                creditor.creditById(0L, budgetId, sharedWindow);
            }

            return childChannel;
        }
        catch (Exception ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }

        // unreachable
        return null;
    }
}
