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

import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.LongFunction;

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

    private final Map<List<Long>, NukleusServerChannel> routesByRef;
    private final Long2ObjectHashMap<MessageHandler> streamsById;
    private final Map<String, NukleusPartition> partitionsByName;

    private final NukleusStreamFactory streamFactory;
    private final LongFunction<NukleusCorrelation> correlateEstablished;
    private final BiFunction<String, String, NukleusTarget> supplyTarget;

    private NukleusPartition[] partitions;

    public NukleusSource(
        Configuration config,
        Path streamsDirectory,
        String sourceName,
        MutableDirectBuffer writeBuffer,
        LongFunction<NukleusCorrelation> correlateEstablished,
        BiFunction<String, String, NukleusTarget> supplyTarget)
    {
        this.config = config;
        this.streamsDirectory = streamsDirectory;
        this.sourceName = sourceName;
        this.writeBuffer = writeBuffer;
        this.routesByRef = new HashMap<>();
        this.streamsById = new Long2ObjectHashMap<>();
        this.partitionsByName = new LinkedHashMap<>();
        this.partitions = new NukleusPartition[0];
        this.streamFactory = new NukleusStreamFactory(streamsById::remove);
        this.correlateEstablished = correlateEstablished;
        this.supplyTarget = supplyTarget;
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
        routesByRef.putIfAbsent(Arrays.asList(sourceRef, authorization), serverChannel);
    }

    public void doUnroute(
        long sourceRef,
        NukleusServerChannel serverChannel)
    {
        routesByRef.remove(sourceRef, serverChannel);
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
        }
        else
        {
            abortFuture.setFailure(new ChannelException("Partition not found for " + channel));
        }
    }

    public void onReadable(
        String partitionName)
    {
        partitionsByName.computeIfAbsent(partitionName, this::newPartition);
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
        String partitionName = localAddress.getSenderPartition();

        NukleusPartition partition = partitionsByName.get(partitionName);
        if (partition == null)
        {
            partition = partitionsByName.entrySet()
                                        .stream()
                                        .filter(e -> e.getKey().startsWith(senderName + "#"))
                                        .findFirst()
                                        .map(e -> e.getValue())
                                        .orElse(null);
        }

        return partition;
    }

    private NukleusPartition newPartition(
        String partitionName)
    {
        Path partitionPath = streamsDirectory.resolve(partitionName);

        StreamsLayout layout = new StreamsLayout.Builder()
                .path(partitionPath)
                .streamsCapacity(config.streamsBufferCapacity())
                .throttleCapacity(config.throttleBufferCapacity())
                .readonly(true)
                .build();

        NukleusPartition partition = new NukleusPartition(partitionPath, layout,
                (r, s) -> routesByRef.get(Arrays.asList(r, s)), streamsById::get, streamsById::put,
                writeBuffer, streamFactory, correlateEstablished, supplyTarget);

        this.partitions = ArrayUtil.add(this.partitions, partition);

        return partition;
    }
}
