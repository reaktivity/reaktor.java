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
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.LongFunction;

import org.agrona.CloseHelper;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.ArrayUtil;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.concurrent.MessageHandler;
import org.jboss.netty.channel.ChannelFuture;
import org.reaktivity.k3po.nukleus.ext.internal.behavior.layout.StreamsLayout;
import org.reaktivity.nukleus.Configuration;

public final class NukleusSource implements AutoCloseable
{
    private final Configuration config;
    private final Path streamsDirectory;
    private final String sourceName;
    private final MutableDirectBuffer writeBuffer;

    private final Long2ObjectHashMap<NukleusServerChannel> routesByRef;
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
        this.routesByRef = new Long2ObjectHashMap<>();
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
        NukleusServerChannel serverChannel)
    {
        // TODO: detect bind collision
        routesByRef.putIfAbsent(sourceRef, serverChannel);
    }

    public void doUnroute(
        long sourceRef,
        NukleusServerChannel serverChannel)
    {
        routesByRef.remove(sourceRef, serverChannel);
    }

    public void doAbort(
        String partitionName,
        long streamId,
        ChannelFuture abortFuture)
    {
        NukleusPartition partition = supplyPartition(partitionName);
        partition.doReset(streamId);

        abortFuture.setSuccess();
    }

    public void onReadable(
        String partitionName)
    {
        supplyPartition(partitionName);
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

    private NukleusPartition supplyPartition(
        String partitionName)
    {
        return partitionsByName.computeIfAbsent(partitionName, this::newPartition);
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
                routesByRef::get, streamsById::get, streamsById::put,
                writeBuffer, streamFactory, correlateEstablished, supplyTarget);

        this.partitions = ArrayUtil.add(this.partitions, partition);

        return partition;
    }
}
