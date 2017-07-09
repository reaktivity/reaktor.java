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
import java.nio.file.WatchService;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
    private static final Pattern SOURCE_NAME = Pattern.compile("([^#]+).*");

    private final Map<String, NukleusSource> sourcesByName;
    private final Map<Path, NukleusTarget> targetsByPath;

    private final Configuration config;
    private final Path streamsDirectory;
    private final NukleusWatcher watcher;
    private final MutableDirectBuffer writeBuffer;
    private final Long2ObjectHashMap<MessageHandler> throttlesById;
    private final Long2ObjectHashMap<NukleusCorrelation> correlations;

    private NukleusSource[] sources = new NukleusSource[0];
    private NukleusTarget[] targets = new NukleusTarget[0];

    public NukleusScope(
        Configuration config,
        Path directory,
        Supplier<WatchService> watchService)
    {
        this.config = config;
        this.streamsDirectory = directory.resolve("streams");

        NukleusWatcher watcher = new NukleusWatcher(watchService, streamsDirectory);
        watcher.setRouter(this);
        this.watcher = watcher;

        this.writeBuffer = new UnsafeBuffer(new byte[config.streamsBufferCapacity() / 8]);
        this.throttlesById = new Long2ObjectHashMap<>();
        this.correlations = new Long2ObjectHashMap<>();
        this.sourcesByName = new LinkedHashMap<>();
        this.targetsByPath = new LinkedHashMap<>();
    }

    @Override
    public String toString()
    {
        return String.format("%s [%s]", getClass().getSimpleName(), streamsDirectory);
    }

    public void doRoute(
        String sourceName,
        long sourceRef,
        NukleusServerChannel serverChannel)
    {
        NukleusSource source = supplySource(sourceName);
        source.doRoute(sourceRef, serverChannel);
    }

    public void doUnroute(
        String sourceName,
        long sourceRef,
        NukleusServerChannel serverChannel)
    {
        NukleusSource source = supplySource(sourceName);
        source.doUnroute(sourceRef, serverChannel);
    }

    public void doConnect(
        NukleusClientChannel clientChannel,
        NukleusChannelAddress remoteAddress,
        ChannelFuture connectFuture)
    {
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
        NukleusTarget target = supplyTarget(channel);
        target.doClose(channel, handlerFuture);
    }

    public void onReadable(
        Path sourcePath)
    {
        String sourceName = source(sourcePath);
        NukleusSource source = supplySource(sourceName);
        String partitionName = sourcePath.getFileName().toString();
        source.onReadable(partitionName);
    }

    public void onExpired(
        Path sourcePath)
    {
        // TODO
    }

    public int process()
    {
        int workCount = 0;

        workCount += watcher.process();

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
        CloseHelper.quietClose(watcher);

        for(NukleusSource source : sourcesByName.values())
        {
            CloseHelper.quietClose(source);
        }

        for(NukleusTarget target : targetsByPath.values())
        {
            CloseHelper.quietClose(target);
        }
    }

    private NukleusSource supplySource(
        String source)
    {
        return sourcesByName.computeIfAbsent(source, this::newSource);
    }

    private NukleusSource newSource(
        String sourceName)
    {
        NukleusSource source = new NukleusSource(config, streamsDirectory, sourceName, writeBuffer,
                correlations::remove, this::supplyTarget);

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
        final NukleusChannelConfig channelConfig = channel.getConfig();
        final String partitionName = channelConfig.getWritePartition();

        String receiverName = remoteAddress.getReceiverName();
        return supplyTarget(receiverName, partitionName);
    }

    private NukleusTarget supplyTarget(
        String receiverName,
        String senderPartitionName)
    {
        final Path targetPath = config.directory()
                .resolve(receiverName)
                .resolve("streams")
                .resolve(senderPartitionName);

        return targetsByPath.computeIfAbsent(targetPath, this::newTarget);
    }

    private NukleusTarget newTarget(
        Path targetPath)
    {
        StreamsLayout layout = new StreamsLayout.Builder()
                .path(targetPath)
                .streamsCapacity(config.streamsBufferCapacity())
                .throttleCapacity(config.throttleBufferCapacity())
                .readonly(false)
                .build();

        NukleusTarget target = new NukleusTarget(targetPath, layout, writeBuffer,
                throttlesById::get, throttlesById::put, throttlesById::remove,
                correlations::put);

        this.targets = ArrayUtil.add(this.targets, target);

        return target;
    }

    private static String source(
        Path path)
    {
        Matcher matcher = SOURCE_NAME.matcher(path.getName(path.getNameCount() - 1).toString());
        if (matcher.matches())
        {
            return matcher.group(1);
        }
        else
        {
            throw new IllegalStateException();
        }
    }
}
