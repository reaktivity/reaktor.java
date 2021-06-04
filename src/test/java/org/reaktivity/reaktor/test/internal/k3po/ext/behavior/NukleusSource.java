/**
 * Copyright 2016-2021 The Reaktivity Project
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
import static org.reaktivity.reaktor.test.internal.k3po.ext.types.NukleusTypeSystem.ADVISORY_CHALLENGE;

import java.nio.file.Path;
import java.util.function.IntFunction;
import java.util.function.LongFunction;
import java.util.function.LongSupplier;

import org.agrona.CloseHelper;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.concurrent.MessageHandler;
import org.jboss.netty.channel.ChannelException;
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
    private final Path streamsPath;
    private final NukleusStreamFactory streamFactory;
    private final LongSupplier supplyTraceId;
    private final NukleusPartition partition;
    private final Long2ObjectHashMap<Long2ObjectHashMap<NukleusServerChannel>> routesByIdAndAuth;
    private final DefaultBudgetCreditor creditor;

    public NukleusSource(
        NukleusExtConfiguration config,
        int scopeIndex,
        LongSupplier supplyTraceId,
        LongFunction<NukleusCorrelation> correlateEstablished,
        LongLongFunction<NukleusTarget> supplySender,
        IntFunction<NukleusTarget> supplyTarget,
        BudgetFlusher flushWatchers,
        Long2ObjectHashMap<MessageHandler> streamsById,
        Long2ObjectHashMap<MessageHandler> throttlesById)
    {
        this.streamsPath = config.directory().resolve(String.format("data%d", scopeIndex));
        this.streamFactory = new NukleusStreamFactory(supplySender, streamsById::remove);
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

        this.supplyTraceId = supplyTraceId;
        this.partition = new NukleusPartition(streamsPath, scopeIndex, streams, this::lookupRoute,
                streamsById::get, streamsById::put, throttlesById::get,
                streamFactory, correlateEstablished, supplySender, supplyTarget);
        this.creditor = new DefaultBudgetCreditor(scopeIndex, budgets, flushWatchers);
    }

    @Override
    public String toString()
    {
        return String.format("%s [%s]", getClass().getSimpleName(), streamsPath);
    }

    public void doRoute(
        long routeId,
        long authorization,
        NukleusServerChannel serverChannel)
    {
        routesByAuth(routeId).put(authorization, serverChannel);
    }

    public void doUnroute(
        long routeId,
        long authorization,
        NukleusServerChannel serverChannel)
    {
        Long2ObjectHashMap<NukleusServerChannel> channels = routesByIdAndAuth.get(routeId);
        if (channels != null && channels.remove(authorization) != null && channels.isEmpty())
        {
            routesByIdAndAuth.remove(routeId);
        }
    }

    public void doAdviseInput(
        NukleusChannel channel,
        ChannelFuture adviseFuture,
        Object value)
    {
        if (value == ADVISORY_CHALLENGE)
        {
            final long traceId = supplyTraceId.getAsLong();

            streamFactory.doChallenge(channel, traceId);

            adviseFuture.setSuccess();
        }
        else
        {
            adviseFuture.setFailure(new ChannelException("unexpected: " + value));
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
        final long traceId = supplyTraceId.getAsLong();

        streamFactory.doReset(channel, traceId);
        partition.doSystemWindow(channel, traceId);

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
        Long2ObjectHashMap<NukleusServerChannel> routesByAuth = routesByAuth(routeId);
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
