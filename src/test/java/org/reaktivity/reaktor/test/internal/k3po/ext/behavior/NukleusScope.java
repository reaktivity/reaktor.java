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

import static java.lang.System.identityHashCode;
import static org.reaktivity.reaktor.internal.router.BudgetId.ownerIndex;
import static org.reaktivity.reaktor.test.internal.k3po.ext.behavior.NukleusTransmission.HALF_DUPLEX;

import java.nio.file.Path;
import java.util.function.LongSupplier;
import java.util.function.ToIntFunction;

import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.ArrayUtil;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.concurrent.MessageHandler;
import org.agrona.concurrent.UnsafeBuffer;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.MessageEvent;
import org.reaktivity.reaktor.internal.budget.DefaultBudgetCreditor;
import org.reaktivity.reaktor.internal.budget.DefaultBudgetDebitor;
import org.reaktivity.reaktor.internal.layouts.BudgetsLayout;
import org.reaktivity.reaktor.internal.types.stream.FlushFW;
import org.reaktivity.reaktor.internal.types.stream.WindowFW;
import org.reaktivity.reaktor.test.internal.k3po.ext.NukleusExtConfiguration;
import org.reaktivity.reaktor.test.internal.k3po.ext.behavior.layout.StreamsLayout;

public final class NukleusScope implements AutoCloseable
{
    private final WindowFW windowRO = new WindowFW();
    private final FlushFW flushRO = new FlushFW();

    private final Int2ObjectHashMap<NukleusTarget> targetsByIndex;
    private final Int2ObjectHashMap<DefaultBudgetDebitor> debitorsByIndex;

    private final NukleusExtConfiguration config;
    private final LabelManager labels;
    private final MutableDirectBuffer writeBuffer;
    private final Long2ObjectHashMap<MessageHandler> streamsById;
    private final Long2ObjectHashMap<MessageHandler> throttlesById;
    private final Long2ObjectHashMap<NukleusCorrelation> correlations;
    private final ToIntFunction<String> lookupTargetIndex;
    private final LongSupplier supplyTimestamp;
    private final LongSupplier supplyTraceId;
    private final NukleusSource source;

    private NukleusTarget[] targets = new NukleusTarget[0];

    public NukleusScope(
        NukleusExtConfiguration config,
        LabelManager labels,
        int scopeIndex,
        ToIntFunction<String> lookupTargetIndex,
        LongSupplier supplyTimestamp,
        LongSupplier supplyTraceId)
    {
        this.config = config;
        this.labels = labels;

        this.writeBuffer = new UnsafeBuffer(new byte[config.streamsBufferCapacity() / 8]);
        this.streamsById = new Long2ObjectHashMap<>();
        this.throttlesById = new Long2ObjectHashMap<>();
        this.correlations = new Long2ObjectHashMap<>();
        this.targetsByIndex = new Int2ObjectHashMap<>();
        this.debitorsByIndex = new Int2ObjectHashMap<>();
        this.lookupTargetIndex = lookupTargetIndex;
        this.supplyTimestamp = supplyTimestamp;
        this.supplyTraceId = supplyTraceId;
        this.source = new NukleusSource(config, labels, scopeIndex, supplyTraceId,
                correlations::remove, this::supplySender, this::supplyTarget,
                this::doSystemFlush, streamsById, throttlesById);

        this.throttlesById.put(0L, this::onSystemMessage);
    }

    @Override
    public String toString()
    {
        return String.format("%s [%s]", getClass().getSimpleName(), source.streamsPath());
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
        NukleusChannelAddress localAddress,
        NukleusChannelAddress remoteAddress,
        ChannelFuture connectFuture)
    {
        final String receiverAddress = remoteAddress.getReceiverAddress();
        final int targetIndex = lookupTargetIndex.applyAsInt(receiverAddress);
        NukleusTarget target = supplyTarget(targetIndex);
        clientChannel.setRemoteScope(targetIndex);
        clientChannel.routeId(routeId(remoteAddress));
        target.doConnect(clientChannel, localAddress, remoteAddress, connectFuture);
    }

    public void doConnectAbort(
        NukleusClientChannel clientChannel,
        NukleusChannelAddress remoteAddress)
    {
        final String receiverAddress = remoteAddress.getReceiverAddress();
        final int targetIndex = lookupTargetIndex.applyAsInt(receiverAddress);
        NukleusTarget target = supplyTarget(targetIndex);
        target.doConnectAbort(clientChannel);
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
        final boolean readClosed = channel.getCloseFuture().isDone() || channel.isReadClosed();

        NukleusTarget target = supplyTarget(channel);
        target.doClose(channel, handlerFuture);

        if (!readClosed && channel.getConfig().getTransmission() == HALF_DUPLEX)
        {
            final ChannelFuture abortFuture = Channels.future(channel);
            source.doAbortInput(channel, abortFuture);
            assert abortFuture.isSuccess();
        }
    }

    public int process()
    {
        return source.process();
    }

    @Override
    public void close()
    {
        CloseHelper.quietClose(source);

        for (NukleusTarget target : targetsByIndex.values())
        {
            CloseHelper.quietClose(target);
        }

        for (DefaultBudgetDebitor debitor : debitorsByIndex.values())
        {
            CloseHelper.quietClose(debitor);
        }
    }

    public NukleusTarget supplySender(
        long routeId,
        long streamId)
    {
        final int targetIndex = replyToIndex(streamId);
        return supplyTarget(targetIndex);
    }

    public DefaultBudgetDebitor supplyDebitor(
        long budgetId)
    {
        final int ownerIndex = ownerIndex(budgetId);
        return debitorsByIndex.computeIfAbsent(ownerIndex, this::newDebitor);
    }

    public DefaultBudgetCreditor creditor()
    {
        return source.creditor();
    }

    private DefaultBudgetDebitor newDebitor(
        int ownerIndex)
    {
        final int watcherIndex = source.scopeIndex();
        final BudgetsLayout layout = new BudgetsLayout.Builder()
                .path(config.directory().resolve(String.format("budgets%d", ownerIndex)))
                .owner(false)
                .build();

        return new DefaultBudgetDebitor(watcherIndex, ownerIndex, layout);

    }

    private void onSystemMessage(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        switch (msgTypeId)
        {
        case WindowFW.TYPE_ID:
            final WindowFW window = windowRO.wrap(buffer, index, index + length);
            onSystemWindow(window);
            break;
        case FlushFW.TYPE_ID:
            final FlushFW flush = flushRO.wrap(buffer, index, index + length);
            onSystemFlush(flush);
            break;
        }
    }

    private void onSystemWindow(
        WindowFW window)
    {
        final long traceId = window.traceId();
        final long budgetId = window.budgetId();
        final int credit = window.credit();

        creditor().creditById(traceId, budgetId, credit);
    }

    private void onSystemFlush(
        FlushFW flush)
    {
        final long traceId = flush.traceId();
        final long budgetId = flush.budgetId();

        final int ownerIndex = ownerIndex(budgetId);
        final DefaultBudgetDebitor debitor = debitorsByIndex.get(ownerIndex);
        if (debitor != null)
        {
            debitor.flush(traceId, budgetId);
        }
    }

    private void doSystemFlush(
        long traceId,
        long budgetId,
        long watchers)
    {
        for (int watcherIndex = 0; watcherIndex < Long.SIZE; watcherIndex++)
        {
            if ((watchers & (1L << watcherIndex)) != 0L)
            {
                final NukleusTarget target = supplyTarget(watcherIndex);
                target.doSystemFlush(traceId, budgetId);
            }
        }
    }

    private NukleusTarget supplyTarget(
        NukleusChannel channel)
    {
        return supplyTarget(channel.getRemoteScope());
    }

    private NukleusTarget supplyTarget(
        int targetIndex)
    {
        return targetsByIndex.computeIfAbsent(targetIndex, this::newTarget);
    }

    private NukleusTarget newTarget(
        int targetIndex)
    {
        final Path targetPath = config.directory()
                .resolve(String.format("data%d", targetIndex));

        final StreamsLayout layout = new StreamsLayout.Builder()
                .path(targetPath)
                .readonly(true)
                .build();

        final NukleusTarget target = new NukleusTarget(source.scopeIndex(), targetPath, layout, writeBuffer,
                throttlesById::put, throttlesById::remove, correlations::put,
                supplyTimestamp, supplyTraceId);

        this.targets = ArrayUtil.add(this.targets, target);

        return target;
    }

    private long routeId(
        NukleusChannelAddress remoteAddress)
    {
        final long localId = labels.supplyLabelId(remoteAddress.getSenderAddress());
        final long remoteId = labels.supplyLabelId(remoteAddress.getReceiverAddress());
        return localId << 48 | remoteId << 32 | 0xf000_0000L | (identityHashCode(remoteAddress) & 0x0fff_ffffL);
    }

    private static int replyToIndex(
        long streamId)
    {
        return isInitial(streamId) ? localIndex(streamId) : remoteIndex(streamId);
    }

    private static int localIndex(
        long streamId)
    {
        return (int)(streamId >> 56) & 0x7f;
    }

    private static int remoteIndex(
        long streamId)
    {
        return (int)(streamId >> 48) & 0x7f;
    }

    private static boolean isInitial(
        long streamId)
    {
        return (streamId & 0x0000_0000_0000_0001L) != 0L;
    }
}
