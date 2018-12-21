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

import static org.jboss.netty.channel.Channels.fireChannelClosed;
import static org.jboss.netty.channel.Channels.fireChannelDisconnected;
import static org.jboss.netty.channel.Channels.fireChannelUnbound;

import java.nio.file.Path;
import java.util.function.LongFunction;

import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.concurrent.MessageHandler;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.reaktivity.k3po.nukleus.ext.internal.NukleusExtConfiguration;
import org.reaktivity.k3po.nukleus.ext.internal.behavior.layout.StreamsLayout;
import org.reaktivity.k3po.nukleus.ext.internal.util.function.LongLongFunction;

public final class NukleusSource implements AutoCloseable
{
    private final LabelManager labels;
    private final Path streamsPath;
    private final NukleusStreamFactory streamFactory;
    private final LongLongFunction<NukleusTarget> supplySender;
    private final NukleusPartition partition;
    private final Long2ObjectHashMap<Long2ObjectHashMap<NukleusServerChannel>> routesByIdAndAuth;

    public NukleusSource(
        NukleusExtConfiguration config,
        LabelManager labels,
        Path streamsPath,
        LongFunction<NukleusCorrelation> correlateEstablished,
        LongLongFunction<NukleusTarget> supplySender,
        Long2ObjectHashMap<MessageHandler> streamsById,
        Long2ObjectHashMap<MessageHandler> throttlesById)
    {
        this.labels = labels;
        this.streamsPath = streamsPath;
        this.streamFactory = new NukleusStreamFactory(streamsById::remove);
        this.supplySender = supplySender;
        this.routesByIdAndAuth = new Long2ObjectHashMap<>();

        StreamsLayout layout = new StreamsLayout.Builder()
                .path(streamsPath)
                .streamsCapacity(config.streamsBufferCapacity())
                .readonly(false)
                .build();

        this.partition = new NukleusPartition(labels, streamsPath, layout,
                this::lookupRoute,
                streamsById::get, streamsById::put, throttlesById::get,
                streamFactory, correlateEstablished, supplySender);
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
        ChannelFuture beginInputFuture = channel.beginInputFuture();
        if (beginInputFuture.isSuccess())
        {
            doAbortInputAfterBeginReply(channel, abortFuture);
        }
        else
        {
            beginInputFuture.addListener(new ChannelFutureListener()
            {
                @Override
                public void operationComplete(
                    ChannelFuture future) throws Exception
                {
                    if (future.isSuccess())
                    {
                        doAbortInputAfterBeginReply(channel, abortFuture);
                    }
                    else
                    {
                        abortFuture.setFailure(future.getCause());
                    }
                }
            });
        }
    }

    private void doAbortInputAfterBeginReply(
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
        partition.close();
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
