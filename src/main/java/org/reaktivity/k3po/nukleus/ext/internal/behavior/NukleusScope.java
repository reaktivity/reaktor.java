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
import java.util.function.LongSupplier;

import org.agrona.CloseHelper;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.ArrayUtil;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.concurrent.MessageHandler;
import org.agrona.concurrent.UnsafeBuffer;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.MessageEvent;
import org.reaktivity.k3po.nukleus.ext.internal.behavior.layout.StreamsLayout;
import org.reaktivity.nukleus.Configuration;

public final class NukleusScope implements AutoCloseable
{
    private final Map<String, NukleusSource> sourcesByName;
    private final Map<Path, NukleusTarget> targetsByPath;

    private final Configuration config;
    private final Path streamsDirectory;
    private final MutableDirectBuffer writeBuffer;
    private final Long2ObjectHashMap<MessageHandler> throttlesById;
    private final Long2ObjectHashMap<NukleusCorrelation> correlations;
    private final LongSupplier supplyTimestamp;
    private final LongSupplier supplyTrace;

    private NukleusSource[] sources = new NukleusSource[0];
    private NukleusTarget[] targets = new NukleusTarget[0];

    public NukleusScope(
        Configuration config,
        Path directory,
        LongSupplier supplyTimestamp,
        LongSupplier supplyTrace)
    {
        this.config = config;
        this.streamsDirectory = directory.resolve("streams");

        this.writeBuffer = new UnsafeBuffer(new byte[config.streamsBufferCapacity() / 8]);
        this.throttlesById = new Long2ObjectHashMap<>();
        this.correlations = new Long2ObjectHashMap<>();
        this.sourcesByName = new LinkedHashMap<>();
        this.targetsByPath = new LinkedHashMap<>();
        this.supplyTimestamp = supplyTimestamp;
        this.supplyTrace = supplyTrace;
    }

    @Override
    public String toString()
    {
        return String.format("%s [%s]", getClass().getSimpleName(), streamsDirectory);
    }

    public void doRoute(
        String senderName,
        String receiverName,
        long routeRef,
        long authorization,
        NukleusServerChannel serverChannel)
    {
        NukleusSource source = supplySource(senderName);
        source.doRoute(routeRef, authorization, serverChannel);
        supplySource(receiverName);
    }

    public void doUnroute(
        String senderName,
        long routeRef,
        long authorization,
        NukleusServerChannel serverChannel)
    {
        NukleusSource source = supplySource(senderName);
        source.doUnroute(routeRef, authorization, serverChannel);
    }

    public void doConnect(
        NukleusClientChannel clientChannel,
        NukleusChannelAddress remoteAddress,
        ChannelFuture connectFuture)
    {
        supplySource(remoteAddress.getReceiverName());

        NukleusTarget target = supplyTarget(clientChannel, remoteAddress);
        target.doConnect(clientChannel, remoteAddress, connectFuture);
    }

    public void doAbortOutput(
        NukleusChannel channel,
        ChannelFuture abortFuture)
    {
        NukleusTarget target = supplyTarget(channel);
        target.doAbortOutput(channel, abortFuture);
    }

    public void doAbortInput(
        NukleusChannel channel,
        ChannelFuture abortFuture)
    {
        String senderName = channel.getLocalAddress().getSenderName();
        NukleusSource source = supplySource(senderName);
        source.doAbortInput(channel, abortFuture);
    }

    public void doWrite(
        NukleusChannel channel,
        MessageEvent writeRequest)
    {
        NukleusTarget target = supplyTarget(channel);
        target.doWrite(channel, writeRequest);
    }

    public void doFlush(
        NukleusChannel channel,
        ChannelFuture flushFuture)
    {
        NukleusTarget target = supplyTarget(channel);
        target.doFlush(channel, flushFuture);
    }

    public void doShutdownOutput(
        NukleusChannel channel,
        ChannelFuture shutdownFuture)
    {
        NukleusTarget target = supplyTarget(channel);
        target.doShutdownOutput(channel, shutdownFuture);
    }

    public void doClose(
        NukleusChannel channel,
        ChannelFuture handlerFuture)
    {
        if (!channel.isWriteClosed())
        {
            NukleusTarget target = supplyTarget(channel);
            target.doClose(channel, handlerFuture);
        }
        else if (!channel.isReadClosed())
        {
            doAbortInput(channel, handlerFuture);
        }
        else
        {
            handlerFuture.setSuccess();
        }
    }

    public int process()
    {
        int workCount = 0;

        for (int i=0; i < sources.length; i++)
        {
            workCount += sources[i].process();
        }

        for (int i=0; i < targets.length; i++)
        {
            workCount += targets[i].process();
        }

        return workCount;
    }

    @Override
    public void close()
    {
        for(NukleusSource source : sourcesByName.values())
        {
            CloseHelper.quietClose(source);
        }

        for(NukleusTarget target : targetsByPath.values())
        {
            CloseHelper.quietClose(target);
        }
    }

    public NukleusSource supplySource(
        String source)
    {
        return sourcesByName.computeIfAbsent(source, this::newSource);
    }

    private NukleusSource newSource(
        String sourceName)
    {
        NukleusSource source = new NukleusSource(config, streamsDirectory, sourceName, writeBuffer,
                correlations::remove, this::supplyTarget, supplyTimestamp, supplyTrace);

        source.supplyPartition(sourceName);

        this.sources = ArrayUtil.add(this.sources, source);

        return source;
    }

    private NukleusTarget supplyTarget(
        NukleusChannel channel)
    {
        return supplyTarget(channel, channel.getRemoteAddress());
    }

    private NukleusTarget supplyTarget(
        NukleusChannel channel,
        NukleusChannelAddress remoteAddress)
    {
        String receiverName = remoteAddress.getReceiverName();
        final String senderName = remoteAddress.getSenderName();
        return supplyTarget(receiverName, senderName);
    }

    private NukleusTarget supplyTarget(
        String receiverName,
        String senderName)
    {
        final Path targetPath = config.directory()
                .resolve(receiverName)
                .resolve("streams")
                .resolve(senderName);

        return targetsByPath.computeIfAbsent(targetPath, this::newTarget);
    }

    private NukleusTarget newTarget(
        Path targetPath)
    {
        StreamsLayout layout = new StreamsLayout.Builder()
                .path(targetPath)
                .streamsCapacity(config.streamsBufferCapacity())
                .throttleCapacity(config.throttleBufferCapacity())
                .readonly(true)
                .build();

        NukleusTarget target = new NukleusTarget(targetPath, layout, writeBuffer,
                throttlesById::get, throttlesById::put, throttlesById::remove,
                correlations::put, supplyTimestamp, supplyTrace);

        this.targets = ArrayUtil.add(this.targets, target);

        return target;
    }
}
