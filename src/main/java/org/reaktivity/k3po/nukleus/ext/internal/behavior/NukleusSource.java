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

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.LongFunction;
import java.util.function.LongSupplier;

import org.agrona.CloseHelper;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.ArrayUtil;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.concurrent.MessageHandler;
import org.jboss.netty.channel.ChannelException;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.reaktivity.k3po.nukleus.ext.internal.behavior.layout.StreamsLayout;
import org.reaktivity.nukleus.Configuration;

public final class NukleusSource implements AutoCloseable
{
    private final Configuration config;
    private final Path streamsDirectory;
    private final String sourceName;
    private final MutableDirectBuffer writeBuffer;

    private final Long2ObjectHashMap<Long2ObjectHashMap<NukleusServerChannel>> routesByRefAndAuth =
            new Long2ObjectHashMap<>();
    private final Long2ObjectHashMap<MessageHandler> streamsById;
    private final Map<String, NukleusPartition> partitionsByName;

    private final NukleusStreamFactory streamFactory;
    private final LongFunction<NukleusCorrelation> correlateEstablished;
    private final BiFunction<String, String, NukleusTarget> supplyTarget;
    private final LongSupplier supplyTimestamp;
    private final LongSupplier supplyTrace;

    private NukleusPartition[] partitions;

    public NukleusSource(
        Configuration config,
        Path streamsDirectory,
        String sourceName,
        MutableDirectBuffer writeBuffer,
        LongFunction<NukleusCorrelation> correlateEstablished,
        BiFunction<String, String, NukleusTarget> supplyTarget,
        LongSupplier supplyTimestamp,
        LongSupplier supplyTrace)
    {
        this.config = config;
        this.streamsDirectory = streamsDirectory;
        this.sourceName = sourceName;
        this.writeBuffer = writeBuffer;
        this.streamsById = new Long2ObjectHashMap<>();
        this.partitionsByName = new LinkedHashMap<>();
        this.partitions = new NukleusPartition[0];
        this.streamFactory = new NukleusStreamFactory(streamsById::remove);
        this.correlateEstablished = correlateEstablished;
        this.supplyTarget = supplyTarget;
        this.supplyTimestamp = supplyTimestamp;
        this.supplyTrace = supplyTrace;
    }

    @Override
    public String toString()
    {
        return String.format("%s [%s/%s[#...]]", getClass().getSimpleName(), streamsDirectory, sourceName);
    }

    public void doRoute(
        long sourceRef,
        long authorization,
        NukleusServerChannel serverChannel)
    {
        // TODO: detect bind collision
        routesByRefAndAuth.computeIfAbsent(sourceRef, key -> new Long2ObjectHashMap<NukleusServerChannel>())
            .put(authorization, serverChannel);
    }

    public void doUnroute(
        long sourceRef,
        long authorization,
        NukleusServerChannel serverChannel)
    {
        Long2ObjectHashMap<NukleusServerChannel> channels = routesByRefAndAuth.get(sourceRef);
        if (channels != null && channels.remove(authorization) != null && channels.isEmpty())
        {
            routesByRefAndAuth.remove(sourceRef);
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
        NukleusPartition partition = findPartition(channel);

        if (partition != null)
        {
            partition.doReset(channel.sourceId());
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
        else
        {
            abortFuture.setFailure(new ChannelException("Partition not found for " + channel));
        }
    }

    public NukleusPartition supplyPartition(
        String sourceName)
    {
        return partitionsByName.computeIfAbsent(sourceName, this::newPartition);
    }

    public int process()
    {
        int workCount = 0;

        for (int i=0; i < partitions.length; i++)
        {
            workCount += partitions[i].process();
        }

        return workCount;
    }

    @Override
    public void close()
    {
        for(NukleusPartition partition : partitionsByName.values())
        {
            CloseHelper.quietClose(partition);
        }
    }

    private NukleusPartition findPartition(
        NukleusChannel channel)
    {
        NukleusChannelAddress localAddress = channel.getLocalAddress();
        String senderName = localAddress.getSenderName();

        return partitionsByName.get(senderName);
    }

    private NukleusPartition newPartition(
        String partitionName)
    {
        Path partitionPath = streamsDirectory.resolve(partitionName);

        StreamsLayout layout = new StreamsLayout.Builder()
                .path(partitionPath)
                .streamsCapacity(config.streamsBufferCapacity())
                .throttleCapacity(config.throttleBufferCapacity())
                .readonly(false)
                .build();

        NukleusPartition partition = new NukleusPartition(partitionPath, layout,
                (r, a) -> routesByRefAndAuth.computeIfAbsent(r, key -> new Long2ObjectHashMap<NukleusServerChannel>()).get(a),
                streamsById::get, streamsById::put,
                writeBuffer, streamFactory, correlateEstablished, supplyTarget, supplyTimestamp, supplyTrace);

        this.partitions = ArrayUtil.add(this.partitions, partition);

        return partition;
    }
}
