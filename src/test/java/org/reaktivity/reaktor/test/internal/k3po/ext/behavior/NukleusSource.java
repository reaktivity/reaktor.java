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
package org.reaktivity.reaktor.test.internal.k3po.ext.behavior;

import static org.jboss.netty.channel.Channels.fireChannelClosed;
import static org.jboss.netty.channel.Channels.fireChannelDisconnected;
import static org.jboss.netty.channel.Channels.fireChannelUnbound;

import java.nio.file.Path;
import java.util.function.LongFunction;

import org.agrona.CloseHelper;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.concurrent.MessageHandler;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.reaktivity.reaktor.internal.budget.DefaultBudgetCreditor;
import org.reaktivity.reaktor.internal.budget.DefaultBudgetCreditor.BudgetFlusher;
import org.reaktivity.reaktor.internal.layouts.BudgetsLayout;
import org.reaktivity.reaktor.test.internal.k3po.ext.NukleusExtConfiguration;
import org.reaktivity.reaktor.test.internal.k3po.ext.behavior.layout.StreamsLayout;
import org.reaktivity.reaktor.test.internal.k3po.ext.util.function.LongLongFunction;

public final class NukleusSource implements AutoCloseable
{
    private final LabelManager labels;
    private final Path streamsPath;
    private final NukleusStreamFactory streamFactory;
    private final LongLongFunction<NukleusTarget> supplySender;
    private final NukleusPartition partition;
    private final Long2ObjectHashMap<Long2ObjectHashMap<NukleusServerChannel>> routesByIdAndAuth;
    private final DefaultBudgetCreditor creditor;

    public NukleusSource(
        NukleusExtConfiguration config,
        LabelManager labels,
        int scopeIndex,
        LongFunction<NukleusCorrelation> correlateEstablished,
        LongLongFunction<NukleusTarget> supplySender,
        BudgetFlusher flushWatchers,
        Long2ObjectHashMap<MessageHandler> streamsById,
        Long2ObjectHashMap<MessageHandler> throttlesById)
    {
        this.labels = labels;
        this.streamsPath = config.directory().resolve(String.format("data%d", scopeIndex));
        this.streamFactory = new NukleusStreamFactory(streamsById::remove);
        this.supplySender = supplySender;
        this.routesByIdAndAuth = new Long2ObjectHashMap<>();

        BudgetsLayout budgets = new BudgetsLayout.Builder()
                .path(config.directory().resolve(String.format("budgets%d", scopeIndex)))
                .capacity(config.budgetsBufferCapacity())
                .owner(true)
                .build();

        StreamsLayout streams = new StreamsLayout.Builder()
                .path(streamsPath)
                .streamsCapacity(config.streamsBufferCapacity())
                .readonly(false)
                .build();

        this.partition = new NukleusPartition(labels, streamsPath, scopeIndex, streams,
                this::lookupRoute,
                streamsById::get, streamsById::put, throttlesById::get,
                streamFactory, correlateEstablished, supplySender);
        this.creditor = new DefaultBudgetCreditor(scopeIndex, budgets, flushWatchers);
    }

    @Override
    public String toString()
    {
        return String.format("%s [%s]", getClass().getSimpleName(), streamsPath);
    }

    public void doRoute(
        String receiverAddress,
        long authorization,
        NukleusServerChannel serverChannel)
    {
        long receiverId = labels.supplyLabelId(receiverAddress);
        routesByAuth(receiverId).put(authorization, serverChannel);
    }

    public void doUnroute(
        String receiverAddress,
        long authorization,
        NukleusServerChannel serverChannel)
    {
        long receiverId = labels.supplyLabelId(receiverAddress);
        Long2ObjectHashMap<NukleusServerChannel> channels = routesByIdAndAuth.get(receiverId);
        if (channels != null && channels.remove(authorization) != null && channels.isEmpty())
        {
            routesByIdAndAuth.remove(receiverId);
        }
    }

    public void doAbortInput(
        NukleusChannel channel,
        ChannelFuture abortFuture)
    {
        boolean isClientChannel = channel.getParent() == null;
        boolean isHalfDuplex = channel.getConfig().getTransmission() == NukleusTransmission.HALF_DUPLEX;
        ChannelFuture beginFuture = isClientChannel && isHalfDuplex ? channel.beginOutputFuture() : channel.beginInputFuture();
        if (beginFuture.isSuccess())
        {
            doAbortInputAfterBegin(channel, abortFuture);
        }
        else
        {
            beginFuture.addListener(new ChannelFutureListener()
            {
                @Override
                public void operationComplete(
                    ChannelFuture future) throws Exception
                {
                    if (future.isSuccess())
                    {
                        doAbortInputAfterBegin(channel, abortFuture);
                    }
                    else
                    {
                        abortFuture.setFailure(future.getCause());
                    }
                }
            });
        }
    }

    private void doAbortInputAfterBegin(
        NukleusChannel channel,
        ChannelFuture abortFuture)
    {
        final long routeId = channel.routeId();
        final long streamId = channel.sourceId();
        final NukleusTarget sender = supplySender.apply(routeId, streamId);

        sender.doReset(channel);
        abortFuture.setSuccess();
        if (channel.setReadAborted())
        {
            if (channel.setReadClosed())
            {
                fireChannelDisconnected(channel);
                fireChannelUnbound(channel);
                fireChannelClosed(channel);
            }
        }
    }

    public int process()
    {
        return partition.process();
    }

    @Override
    public void close()
    {
        CloseHelper.quietClose(creditor);

        partition.close();
    }

    Path streamsPath()
    {
        return streamsPath;
    }

    int scopeIndex()
    {
        return partition.scopeIndex();
    }

    DefaultBudgetCreditor creditor()
    {
        return creditor;
    }

    private NukleusServerChannel lookupRoute(
        long routeId,
        long authorization)
    {
        long remoteId = (routeId >> 32) & 0xffff;
        Long2ObjectHashMap<NukleusServerChannel> routesByAuth = routesByAuth(remoteId);
        return routesByAuth.get(authorization);
    }

    private Long2ObjectHashMap<NukleusServerChannel> routesByAuth(
        long routeId)
    {
        return routesByIdAndAuth.computeIfAbsent(routeId, this::newRoutesByAuth);
    }

    private Long2ObjectHashMap<NukleusServerChannel> newRoutesByAuth(
        long routeId)
    {
        return new Long2ObjectHashMap<NukleusServerChannel>();
    }
}
