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

import static java.lang.System.identityHashCode;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.LongSupplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.agrona.CloseHelper;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.ArrayUtil;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.concurrent.MessageHandler;
import org.agrona.concurrent.UnsafeBuffer;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.MessageEvent;
import org.reaktivity.k3po.nukleus.ext.internal.NukleusExtConfiguration;
import org.reaktivity.k3po.nukleus.ext.internal.behavior.layout.StreamsLayout;

public final class NukleusScope implements AutoCloseable
{
    private static final Pattern ADDRESS_PATTERN = Pattern.compile("^([^#]+)(:?#.*)?$");

    private final Map<String, NukleusTarget> targetsByName;
    private final Int2ObjectHashMap<NukleusTarget> targetsByLabelId;

    private final NukleusExtConfiguration config;
    private final LabelManager labels;
    private final Path streamsPath;
    private final MutableDirectBuffer writeBuffer;
    private final Long2ObjectHashMap<MessageHandler> streamsById;
    private final Long2ObjectHashMap<MessageHandler> throttlesById;
    private final Long2ObjectHashMap<NukleusCorrelation> correlations;
    private final LongSupplier supplyTimestamp;
    private final LongSupplier supplyTrace;
    private final NukleusSource source;

    private NukleusTarget[] targets = new NukleusTarget[0];

    public NukleusScope(
        NukleusExtConfiguration config,
        LabelManager labels,
        String scopeName,
        LongSupplier supplyTimestamp,
        LongSupplier supplyTrace)
    {
        this.config = config;
        this.labels = labels;
        this.streamsPath = config.directory().resolve(scopeName).resolve("streams");

        this.writeBuffer = new UnsafeBuffer(new byte[config.streamsBufferCapacity() / 8]);
        this.streamsById = new Long2ObjectHashMap<>();
        this.throttlesById = new Long2ObjectHashMap<>();
        this.correlations = new Long2ObjectHashMap<>();
        this.targetsByName = new LinkedHashMap<>();
        this.targetsByLabelId = new Int2ObjectHashMap<>();
        this.supplyTimestamp = supplyTimestamp;
        this.supplyTrace = supplyTrace;
        this.source = new NukleusSource(config, labels, streamsPath,
                correlations::remove, this::supplySender,
                streamsById, throttlesById);
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
        source.doRoute(receiverAddress, authorization, serverChannel);
    }

    public void doUnroute(
        String receiverAddress,
        long authorization,
        NukleusServerChannel serverChannel)
    {
        source.doUnroute(receiverAddress, authorization, serverChannel);
    }

    public void doConnect(
        NukleusClientChannel clientChannel,
        NukleusChannelAddress remoteAddress,
        ChannelFuture connectFuture)
    {
        final String receiverName = remoteAddress.getReceiverName();
        NukleusTarget target = supplyTarget(receiverName);
        final String replyAddress = remoteAddress.getSenderAddress();
        final NukleusChannelAddress localAddress = remoteAddress.newReplyToAddress(replyAddress);
        clientChannel.routeId(routeId(remoteAddress));
        target.doConnect(clientChannel, localAddress, remoteAddress, connectFuture);
    }

    private long routeId(NukleusChannelAddress remoteAddress)
    {
        final long localId = labels.supplyLabelId(remoteAddress.getSenderAddress());
        final long remoteId = labels.supplyLabelId(remoteAddress.getReceiverAddress());
        return localId << 48 | remoteId << 32 | identityHashCode(remoteAddress);
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

    public int process()
    {
        return source.process();
    }

    @Override
    public void close()
    {
        CloseHelper.quietClose(source);

        for(NukleusTarget target : targetsByName.values())
        {
            CloseHelper.quietClose(target);
        }
    }

    public NukleusTarget supplySender(
        long routeId,
        long streamId)
    {
        final boolean initial = (streamId & 0x8000_0000_0000_0000L) == 0L;
        final int labelId = initial ? localId(routeId) : remoteId(routeId);
        return targetsByLabelId.computeIfAbsent(labelId, this::newTarget);
    }

    private NukleusTarget newTarget(
        int labelId)
    {
        final String addressName = labels.lookupLabel(labelId);
        final Matcher matcher = ADDRESS_PATTERN.matcher(addressName);
        matcher.matches();
        final String targetName = matcher.group(1);
        return supplyTarget(targetName);
    }

    private NukleusTarget supplyTarget(
        NukleusChannel channel)
    {
        return supplyTarget(channel.getRemoteAddress().getReceiverName());
    }

    private NukleusTarget supplyTarget(
        String targetName)
    {
        return targetsByName.computeIfAbsent(targetName, this::newTarget);
    }

    private NukleusTarget newTarget(
        String targetName)
    {
        final Path targetPath = config.directory()
                .resolve(targetName)
                .resolve("streams");

        final StreamsLayout layout = new StreamsLayout.Builder()
                .path(targetPath)
                .streamsCapacity(config.streamsBufferCapacity())
                .readonly(true)
                .build();

        final NukleusTarget target = new NukleusTarget(labels, targetPath, layout, writeBuffer,
                throttlesById::put, throttlesById::remove,
                correlations::put, supplyTimestamp, supplyTrace);

        this.targets = ArrayUtil.add(this.targets, target);

        return target;
    }

    private int localId(
        long routeId)
    {
        return (int)(routeId >> 48) & 0xffff;
    }

    private int remoteId(
        long routeId)
    {
        return (int)(routeId >> 32) & 0xffff;
    }
}
