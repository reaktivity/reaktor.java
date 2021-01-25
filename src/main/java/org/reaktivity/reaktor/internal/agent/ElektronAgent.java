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
package org.reaktivity.reaktor.internal.agent;

import static java.lang.String.format;
import static java.lang.System.currentTimeMillis;
import static java.lang.ThreadLocal.withInitial;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.agrona.CloseHelper.quietClose;
import static org.reaktivity.nukleus.budget.BudgetCreditor.NO_BUDGET_ID;
import static org.reaktivity.nukleus.concurrent.Signaler.NO_CANCEL_ID;
import static org.reaktivity.reaktor.internal.router.BudgetId.ownerIndex;
import static org.reaktivity.reaktor.internal.router.RouteId.localId;
import static org.reaktivity.reaktor.internal.router.RouteId.remoteId;
import static org.reaktivity.reaktor.internal.router.StreamId.instanceId;
import static org.reaktivity.reaktor.internal.router.StreamId.isInitial;
import static org.reaktivity.reaktor.internal.router.StreamId.remoteIndex;
import static org.reaktivity.reaktor.internal.router.StreamId.streamId;
import static org.reaktivity.reaktor.internal.router.StreamId.streamIndex;
import static org.reaktivity.reaktor.internal.router.StreamId.throttleIndex;

import java.net.InetAddress;
import java.util.BitSet;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.function.IntConsumer;
import java.util.function.IntFunction;
import java.util.function.LongConsumer;
import java.util.function.LongFunction;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.agrona.DeadlineTimerWheel;
import org.agrona.DeadlineTimerWheel.TimerHandler;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.ArrayUtil;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.AgentTerminationException;
import org.agrona.concurrent.MessageHandler;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.ringbuffer.RingBuffer;
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.status.CountersManager;
import org.agrona.hints.ThreadHints;
import org.reaktivity.nukleus.AgentBuilder;
import org.reaktivity.nukleus.Elektron;
import org.reaktivity.nukleus.Nukleus;
import org.reaktivity.nukleus.budget.BudgetDebitor;
import org.reaktivity.nukleus.buffer.BufferPool;
import org.reaktivity.nukleus.buffer.CountingBufferPool;
import org.reaktivity.nukleus.concurrent.Signaler;
import org.reaktivity.nukleus.concurrent.SignalingExecutor;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.function.MessageFunction;
import org.reaktivity.nukleus.function.MessagePredicate;
import org.reaktivity.nukleus.route.Address;
import org.reaktivity.nukleus.route.AddressFactory;
import org.reaktivity.nukleus.route.AddressFactoryBuilder;
import org.reaktivity.nukleus.route.RouteKind;
import org.reaktivity.nukleus.route.RouteManager;
import org.reaktivity.nukleus.stream.StreamFactory;
import org.reaktivity.nukleus.stream.StreamFactoryBuilder;
import org.reaktivity.reaktor.ReaktorConfiguration;
import org.reaktivity.reaktor.internal.Counters;
import org.reaktivity.reaktor.internal.LabelManager;
import org.reaktivity.reaktor.internal.budget.DefaultBudgetCreditor;
import org.reaktivity.reaktor.internal.budget.DefaultBudgetDebitor;
import org.reaktivity.reaktor.internal.layouts.BudgetsLayout;
import org.reaktivity.reaktor.internal.layouts.BufferPoolLayout;
import org.reaktivity.reaktor.internal.layouts.MetricsLayout;
import org.reaktivity.reaktor.internal.layouts.StreamsLayout;
import org.reaktivity.reaktor.internal.router.Resolver;
import org.reaktivity.reaktor.internal.router.StreamId;
import org.reaktivity.reaktor.internal.router.Target;
import org.reaktivity.reaktor.internal.router.WriteCounters;
import org.reaktivity.reaktor.internal.types.Flyweight;
import org.reaktivity.reaktor.internal.types.OctetsFW;
import org.reaktivity.reaktor.internal.types.control.Role;
import org.reaktivity.reaktor.internal.types.control.RouteFW;
import org.reaktivity.reaktor.internal.types.control.UnrouteFW;
import org.reaktivity.reaktor.internal.types.stream.AbortFW;
import org.reaktivity.reaktor.internal.types.stream.BeginFW;
import org.reaktivity.reaktor.internal.types.stream.ChallengeFW;
import org.reaktivity.reaktor.internal.types.stream.DataFW;
import org.reaktivity.reaktor.internal.types.stream.EndFW;
import org.reaktivity.reaktor.internal.types.stream.ExtensionFW;
import org.reaktivity.reaktor.internal.types.stream.FlushFW;
import org.reaktivity.reaktor.internal.types.stream.FrameFW;
import org.reaktivity.reaktor.internal.types.stream.ReaktorSignalExFW;
import org.reaktivity.reaktor.internal.types.stream.ResetFW;
import org.reaktivity.reaktor.internal.types.stream.SignalFW;
import org.reaktivity.reaktor.internal.types.stream.WindowFW;

public class ElektronAgent implements Agent
{
    private static final Pattern ADDRESS_PATTERN = Pattern.compile("^([^#]+)(:?#.*)$");

    private static final int SYSTEM_SIGNAL_ROUTED = 1;
    private static final int SYSTEM_SIGNAL_UNROUTED = 2;

    private final FrameFW frameRO = new FrameFW();
    private final BeginFW beginRO = new BeginFW();
    private final DataFW dataRO = new DataFW();
    private final FlushFW flushRO = new FlushFW();
    private final WindowFW windowRO = new WindowFW();
    private final SignalFW signalRO = new SignalFW();
    private final ExtensionFW extensionRO = new ExtensionFW();
    private final ReaktorSignalExFW reaktorSignalExRO = new ReaktorSignalExFW();
    private final AbortFW.Builder abortRW = new AbortFW.Builder();
    private final FlushFW.Builder flushRW = new FlushFW.Builder();
    private final ResetFW.Builder resetRW = new ResetFW.Builder();
    private final WindowFW.Builder windowRW = new WindowFW.Builder();
    private final SignalFW.Builder signalRW = new SignalFW.Builder();
    private final ReaktorSignalExFW.Builder reaktorSignalExRW = new ReaktorSignalExFW.Builder();

    private final int reaktorTypeId;
    private final int localIndex;
    private final ReaktorConfiguration config;
    private final LabelManager labels;
    private final Function<String, BitSet> affinityMask;
    private final String elektronName;
    private final Counters counters;
    private final Function<String, InetAddress[]> resolveHost;
    private final boolean timestamps;
    private final MetricsLayout metricsLayout;
    private final StreamsLayout streamsLayout;
    private final BufferPoolLayout bufferPoolLayout;
    private final RingBuffer streamsBuffer;
    private final MutableDirectBuffer writeBuffer;
    private final MutableDirectBuffer signalBuffer;
    private final Int2ObjectHashMap<MessageConsumer>[] streams;
    private final Int2ObjectHashMap<MessageConsumer>[] throttles;
    private final Long2ObjectHashMap<ReadCounters> countersByRouteId;
    private final Int2ObjectHashMap<MessageConsumer> writersByIndex;
    private final Int2ObjectHashMap<Target> targetsByIndex;
    private final Map<String, ElektronRef> elektronByName;
    private final BufferPool bufferPool;
    private final int shift;
    private final long mask;
    private final MessageHandler readHandler;
    private final TimerHandler expireHandler;
    private final int readLimit;
    private final int expireLimit;
    private final LongFunction<? extends ReadCounters> newReadCounters;
    private final IntFunction<MessageConsumer> supplyWriter;
    private final IntFunction<Target> newTarget;
    private final LongFunction<WriteCounters> newWriteCounters;
    private final LongFunction<Affinity> resolveAffinity;
    private final Long2ObjectHashMap<Address> addressesByRouteId;

    private final RouteManager resolver;

    private final DefaultBudgetCreditor creditor;
    private final Int2ObjectHashMap<DefaultBudgetDebitor> debitorsByIndex;

    // TODO: copy-on-write
    private final Int2ObjectHashMap<StreamFactory> streamFactoriesByAddressId;

    private final Long2ObjectHashMap<Affinity> affinityByRemoteId;
    private final Supplier<DirectBuffer> routesBufferRef;

    private final DeadlineTimerWheel timerWheel;
    private final Long2ObjectHashMap<Runnable> tasksByTimerId;
    private final Long2ObjectHashMap<Future<?>> futuresById;
    private final SignalingExecutor executor;
    private final ElektronSignaler signaler;

    private long streamId;
    private long traceId;
    private long budgetId;

    private volatile Agent[] agents;

    private long lastReadStreamId;


    public ElektronAgent(
        int index,
        int count,
        ReaktorConfiguration config,
        LabelManager labels,
        ExecutorService executorService,
        Function<String, BitSet> affinityMask,
        Supplier<DirectBuffer> routesBufferRef,
        Supplier<AgentBuilder> supplyAgentBuilder)
    {
        this.reaktorTypeId = labels.supplyLabelId(config.name());
        this.localIndex = index;
        this.config = config;
        this.labels = labels;
        this.affinityMask = affinityMask;
        this.routesBufferRef = routesBufferRef;

        final MetricsLayout metricsLayout = new MetricsLayout.Builder()
                .path(config.directory().resolve(String.format("metrics%d", index)))
                .labelsBufferCapacity(config.counterLabelsBufferCapacity())
                .valuesBufferCapacity(config.counterValuesBufferCapacity())
                .readonly(false)
                .build();

        final StreamsLayout streamsLayout = new StreamsLayout.Builder()
                .path(config.directory().resolve(String.format("data%d", index)))
                .streamsCapacity(config.streamsBufferCapacity())
                .readonly(false)
                .build();

        final BufferPoolLayout bufferPoolLayout = new BufferPoolLayout.Builder()
                .path(config.directory().resolve(String.format("buffers%d", index)))
                .slotCapacity(config.bufferSlotCapacity())
                .slotCount(config.bufferPoolCapacity() / config.bufferSlotCapacity())
                .readonly(false)
                .build();

        this.elektronName = String.format("reaktor/data#%d", index);
        this.metricsLayout = metricsLayout;
        this.streamsLayout = streamsLayout;
        this.bufferPoolLayout = bufferPoolLayout;

        final CountersManager countersManager =
                new CountersManager(metricsLayout.labelsBuffer(), metricsLayout.valuesBuffer());
        this.counters = new Counters(countersManager);

        this.resolveHost = config.hostResolver();
        this.timestamps = config.timestamps();
        this.readLimit = config.maximumMessagesPerRead();
        this.expireLimit = config.maximumExpirationsPerPoll();
        this.streamsBuffer = streamsLayout.streamsBuffer();
        this.writeBuffer = new UnsafeBuffer(new byte[config.bufferSlotCapacity() + 1024]);
        this.signalBuffer = new UnsafeBuffer(new byte[config.bufferSlotCapacity() + 1024]);
        this.streams = initDispatcher();
        this.throttles = initDispatcher();
        this.countersByRouteId = new Long2ObjectHashMap<>();
        this.streamFactoriesByAddressId = new Int2ObjectHashMap<>();
        this.readHandler = this::handleRead;
        this.expireHandler = this::handleExpire;
        this.newReadCounters = this::newReadCounters;
        this.supplyWriter = this::supplyWriter;
        this.newTarget = this::newTarget;
        this.newWriteCounters = this::newWriteCounters;
        this.resolveAffinity = this::resolveAffinity;
        this.addressesByRouteId = new Long2ObjectHashMap<>();
        this.elektronByName = new ConcurrentHashMap<>();
        this.affinityByRemoteId = new Long2ObjectHashMap<>();
        this.targetsByIndex = new Int2ObjectHashMap<>();
        this.writersByIndex = new Int2ObjectHashMap<>();
        this.agents = new Agent[0];

        this.timerWheel = new DeadlineTimerWheel(MILLISECONDS, currentTimeMillis(), 512, 1024);
        this.tasksByTimerId = new Long2ObjectHashMap<>();
        this.futuresById = new Long2ObjectHashMap<>();
        this.executor = new ElektronExecutor(executorService);
        this.signaler = new ElektronSignaler(executorService);

        this.resolver = new ResolverRef(this::newResolver);

        final BufferPool bufferPool = bufferPoolLayout.bufferPool();

        final int reserved = Byte.SIZE;
        final int shift = Long.SIZE - reserved;
        final long initial = ((long) index) << shift;
        final long mask = initial | (-1L >>> reserved);

        this.shift = shift;
        this.mask = mask;
        this.bufferPool = bufferPool;
        this.streamId = initial;
        this.traceId = initial;
        this.budgetId = initial;

        final BudgetsLayout budgetsLayout = new BudgetsLayout.Builder()
                .path(config.directory().resolve(String.format("budgets%d", index)))
                .capacity(config.budgetsBufferCapacity())
                .owner(true)
                .build();

        this.creditor = new DefaultBudgetCreditor(index, budgetsLayout, this::doSystemFlush, this::supplyBudgetId,
            signaler::executeTaskAt, config.childCleanupLingerMillis());
        this.debitorsByIndex = new Int2ObjectHashMap<DefaultBudgetDebitor>();

        if (supplyAgentBuilder != null)
        {
            final AgentBuilder agentBuilder = supplyAgentBuilder.get();
            final Agent agent = agentBuilder
                    .setRouteManager(resolver)
                    .setExecutor(executor)
                    .setWriteBuffer(writeBuffer)
                    .setAddressIdSupplier(labels::supplyLabelId)
                    .setStreamFactorySupplier(this::supplyStreamFactory)
                    .setThrottleSupplier(this::supplyThrottle)
                    .setThrottleRemover(this::removeThrottle)
                    .setInitialIdSupplier(this::supplyInitialId)
                    .setReplyIdSupplier(this::supplyReplyId)
                    .setTraceIdSupplier(this::supplyTraceId)
                    .setGroupIdSupplier(this::supplyBudgetId)
                    .setBufferPool(bufferPool)
                    .build();
            this.agents = ArrayUtil.add(agents, agent);
        }
    }

    private void onSystemMessage(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        switch (msgTypeId)
        {
        case FlushFW.TYPE_ID:
            final FlushFW flush = flushRO.wrap(buffer, index, index + length);
            onSystemFlush(flush);
            break;
        case WindowFW.TYPE_ID:
            final WindowFW window = windowRO.wrap(buffer, index, index + length);
            onSystemWindow(window);
            break;
        case SignalFW.TYPE_ID:
            final SignalFW signal = signalRO.wrap(buffer, index, index + length);
            onSystemSignal(signal);
            break;
        }
    }

    private void onSystemFlush(
        FlushFW flush)
    {
        final long traceId = flush.traceId();
        final long budgetId = flush.budgetId();

        final int ownerIndex = ownerIndex(budgetId);
        final DefaultBudgetDebitor debitor = debitorsByIndex.get(ownerIndex);

        if (ReaktorConfiguration.DEBUG_BUDGETS)
        {
            System.out.format("[%d] [0x%016x] [0x%016x] FLUSH %08x %s\n",
                    System.nanoTime(), traceId, budgetId, ownerIndex, debitor);
        }

        if (debitor != null)
        {
            debitor.flush(traceId, budgetId);
        }
    }

    private void onSystemWindow(
        WindowFW window)
    {
        final long traceId = window.traceId();
        final long budgetId = window.budgetId();
        final int reserved = window.maximum();

        creditor.creditById(traceId, budgetId, reserved);

        long parentBudgetId = creditor.parentBudgetId(budgetId);
        if (parentBudgetId != NO_BUDGET_ID)
        {
            doSystemWindowIfNecessary(traceId, parentBudgetId, reserved);
        }
    }

    private void onSystemSignal(
        SignalFW signal)
    {
        final int signalId = signal.signalId();
        switch (signalId)
        {
        case SYSTEM_SIGNAL_ROUTED:
            onSystemRoutedSignal(signal);
            break;
        case SYSTEM_SIGNAL_UNROUTED:
            onSystemUnroutedSignal(signal);
            break;
        }
    }

    private void onSystemRoutedSignal(
        SignalFW signal)
    {
        final long routeId = signal.routeId();
        final OctetsFW extension = signal.extension();
        final ExtensionFW signalEx = extension.get(extensionRO::wrap);
        assert signalEx.typeId() == reaktorTypeId;
        final ReaktorSignalExFW reaktorSignalEx = extension.get(reaktorSignalExRO::wrap);
        assert reaktorSignalEx.kind() == ReaktorSignalExFW.KIND_ROUTE;
        final RouteFW route = reaktorSignalEx.route();
        final RouteKind routeKind = RouteKind.valueOf(route.role().get().ordinal());
        final String nukleusName = route.nukleus().asString();
        final String localName = route.localAddress().asString();
        final ElektronRef elektronRef = elektronByName.get(nukleusName);
        final AddressFactory addressFactory = elektronRef.addressFactories.get(routeKind);
        if (addressFactory != null)
        {
            final Address newAddress = addressFactory.newAddress(localName);
            assert nukleusName.equals(newAddress.nukleus());
            addressesByRouteId.put(routeId, newAddress);
            final MessageConsumer routeHandler = newAddress.routeHandler();
            assert routeHandler != null;
            routeHandler.accept(route.typeId(), route.buffer(), route.offset(), route.sizeof());
        }
    }

    private void onSystemUnroutedSignal(
        SignalFW signal)
    {
        final long routeId = signal.routeId();
        final Address address = addressesByRouteId.remove(routeId);
        if (address != null)
        {
            final OctetsFW extension = signal.extension();
            final ExtensionFW signalEx = extension.get(extensionRO::wrap);
            assert signalEx.typeId() == reaktorTypeId;
            final ReaktorSignalExFW reaktorSignalEx = extension.get(reaktorSignalExRO::wrap);
            assert reaktorSignalEx.kind() == ReaktorSignalExFW.KIND_UNROUTE;
            final UnrouteFW unroute = reaktorSignalEx.unroute();
            final MessageConsumer routeHandler = address.routeHandler();
            assert routeHandler != null;
            routeHandler.accept(unroute.typeId(), unroute.buffer(), unroute.offset(), unroute.sizeof());
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
                if (ReaktorConfiguration.DEBUG_BUDGETS)
                {
                    System.out.format("[%d] [0x%016x] [0x%016x] flush %d\n",
                            System.nanoTime(), traceId, budgetId, watcherIndex);
                }

                final MessageConsumer writer = supplyWriter(watcherIndex);
                final FlushFW flush = flushRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                        .routeId(0L)
                        .streamId(0L)
                        .sequence(0L)
                        .acknowledge(0L)
                        .maximum(0)
                        .traceId(traceId)
                        .budgetId(budgetId)
                        .reserved(0)
                        .build();

                writer.accept(flush.typeId(), flush.buffer(), flush.offset(), flush.sizeof());
            }
        }
    }

    private void doSystemWindow(
        long traceId,
        long budgetId,
        int reserved)
    {
        if (ReaktorConfiguration.DEBUG_BUDGETS)
        {
            System.out.format("[%d] [0x%016x] [0x%016x] doSystemWindow credit=%d \n",
                System.nanoTime(), traceId, budgetId, reserved);
        }

        final int targetIndex = ownerIndex(budgetId);
        final MessageConsumer writer = supplyWriter(targetIndex);
        final WindowFW window = windowRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                                        .routeId(0L)
                                        .streamId(0L)
                                        .sequence(0L)
                                        .acknowledge(0L)
                                        .maximum(reserved)
                                        .traceId(traceId)
                                        .budgetId(budgetId)
                                        .padding(0)
                                        .build();
        writer.accept(window.typeId(), window.buffer(), window.offset(), window.sizeof());
    }

    private static class ResolverRef implements RouteManager
    {
        private final ThreadLocal<Resolver> resolver;

        ResolverRef(
            Supplier<Resolver> supplyResolver)
        {
            resolver = ThreadLocal.withInitial(supplyResolver);
        }

        @Override
        public <R> R resolveExternal(
            long authorization,
            MessagePredicate filter,
            MessageFunction<R> mapper)
        {
            return resolver.get().resolveExternal(authorization, filter, mapper);
        }

        @Override
        public <R> R resolve(
            long routeId,
            long authorization,
            MessagePredicate filter,
            MessageFunction<R> mapper)
        {
            return resolver.get().resolve(routeId, authorization, filter, mapper);
        }

        @Override
        public void forEach(
            MessageConsumer consumer)
        {
            resolver.get().forEach(consumer);
        }

        @Override
        public MessageConsumer supplyReceiver(
            long streamId)
        {
            return resolver.get().supplyReceiver(streamId);
        }

        @Override
        public void setThrottle(
            long streamId,
            MessageConsumer throttle)
        {
            resolver.get().setThrottle(streamId, throttle);
        }

        @Override
        public void clearThrottle(
            long streamId)
        {
            resolver.get().clearThrottle(streamId);
        }
    }

    private Resolver newResolver()
    {
        return new Resolver(routesBufferRef, throttles, this::supplyInitialWriter);
    }

    @Override
    public String roleName()
    {
        return elektronName;
    }

    @Override
    public int doWork() throws Exception
    {
        int workDone = 0;

        try
        {
            for (final Agent agent : agents)
            {
                workDone += agent.doWork();
            }

            if (timerWheel.timerCount() != 0L)
            {
                final long now = currentTimeMillis();
                int expiredMax = expireLimit;
                while (timerWheel.currentTickTime() <= now && expiredMax > 0)
                {
                    final int expired = timerWheel.poll(now, expireHandler, expiredMax);

                    workDone += expired;
                    expiredMax -= expired;
                }
            }

            workDone += streamsBuffer.read(readHandler, readLimit);
        }
        catch (Throwable ex)
        {
            ex.addSuppressed(new Exception(String.format("[%s]\t[0x%016x] %s",
                                                         elektronName, lastReadStreamId, streamsLayout)));
            throw new AgentTerminationException(ex);
        }

        return workDone;
    }

    public long counter(
        String name)
    {
        final LongSupplier counter = counters.readonlyCounter(name);
        return counter != null ? counter.getAsLong() : 0L;
    }

    @Override
    public void onClose()
    {
        while (config.drainOnClose() &&
               streamsBuffer.consumerPosition() < streamsBuffer.producerPosition())
        {
            ThreadHints.onSpinWait();
        }

        for (final Agent agent : agents)
        {
            agent.onClose();
        }

        int acquiredBuffers = 0;
        int acquiredCreditors = 0;
        long acquiredDebitors = 0L;

        if (config.syntheticAbort())
        {
            final Int2ObjectHashMap<MessageConsumer> handlers = new Int2ObjectHashMap<>();
            for (int senderIndex = 0; senderIndex < streams.length; senderIndex++)
            {
                handlers.clear();
                streams[senderIndex].forEach(handlers::put);

                final int senderIndex0 = senderIndex;
                handlers.forEach((id, handler) -> doSyntheticAbort(streamId(localIndex, senderIndex0, id), handler));
            }

            acquiredBuffers = bufferPool.acquiredSlots();
            acquiredCreditors = creditor.acquired();
            acquiredDebitors = debitorsByIndex.values()
                                              .stream()
                                              .mapToInt(DefaultBudgetDebitor::acquired)
                                              .sum();
        }

        targetsByIndex.forEach((k, v) -> v.detach());
        targetsByIndex.forEach((k, v) -> quietClose(v));

        quietClose(streamsLayout);
        quietClose(metricsLayout);
        quietClose(bufferPoolLayout);

        debitorsByIndex.forEach((k, v) -> quietClose(v));
        quietClose(creditor);

        if (acquiredBuffers != 0 || acquiredCreditors != 0 || acquiredDebitors != 0L)
        {
            throw new IllegalStateException(
                    String.format("Some resources not released: %d buffers, %d creditors, %d debitors",
                                  acquiredBuffers, acquiredCreditors, acquiredDebitors));
        }
    }

    @Override
    public String toString()
    {
        return elektronName;
    }

    public void onRouteable(
        long routeId,
        Nukleus nukleus)
    {
        String nukleusName = nukleus.name();
        int localAddressId = localId(routeId);
        String localAddress = labels.lookupLabel(localAddressId);
        BitSet affinity = affinityMask.apply(localAddress);
        if (affinity.get(localIndex))
        {
            elektronByName.computeIfAbsent(nukleusName, name -> new ElektronRef(name, nukleus.supplyElektron(localIndex)));
        }
    }

    public void onRouted(
        Nukleus nukleus,
        RouteKind routeKind,
        long routeId,
        OctetsFW extension)
    {
        String nukleusName = nukleus.name();
        int localAddressId = localId(routeId);
        int remoteAddressId = remoteId(routeId);
        String localAddress = labels.lookupLabel(localAddressId);
        String remoteAddress = labels.lookupLabel(remoteAddressId);
        BitSet affinity = affinityMask.apply(localAddress);
        if (affinity.get(localIndex))
        {
            elektronByName.computeIfPresent(nukleusName, (a, r) -> r.assign(routeKind, localAddressId));

            final SignalFW signal = signalRW.wrap(signalBuffer, 0, signalBuffer.capacity())
                                            .routeId(routeId)
                                            .streamId(0L)
                                            .sequence(0L)
                                            .acknowledge(0L)
                                            .maximum(0)
                                            .cancelId(NO_CANCEL_ID)
                                            .signalId(SYSTEM_SIGNAL_ROUTED)
                                            .extension(m -> m.set(visitRoutedSignalEx(routeId,
                                                                                      nukleusName,
                                                                                      routeKind,
                                                                                      localAddress,
                                                                                      remoteAddress,
                                                                                      extension)))
                                            .build();
            streamsBuffer.write(signal.typeId(), signal.buffer(), signal.offset(), signal.sizeof());
        }
    }

    public void onUnrouted(
        Nukleus nukleus,
        RouteKind routeKind,
        long routeId)
    {
        String nukleusName = nukleus.name();
        int localAddressId = localId(routeId);
        String localAddress = labels.lookupLabel(localAddressId);
        BitSet affinity = affinityMask.apply(localAddress);
        if (affinity.get(localIndex))
        {
            elektronByName.computeIfPresent(nukleusName, (a, r) -> r.unassign(routeKind, localAddressId));

            final SignalFW signal = signalRW.wrap(signalBuffer, 0, signalBuffer.capacity())
                                            .routeId(routeId)
                                            .streamId(0L)
                                            .sequence(0L)
                                            .acknowledge(0L)
                                            .maximum(0)
                                            .cancelId(NO_CANCEL_ID)
                                            .signalId(SYSTEM_SIGNAL_UNROUTED)
                                            .extension(m -> m.set(visitUnroutedSignalEx(routeId, nukleusName)))
                                            .build();
            streamsBuffer.write(signal.typeId(), signal.buffer(), signal.offset(), signal.sizeof());
        }
    }

    private Flyweight.Builder.Visitor visitRoutedSignalEx(
        long routeId,
        String nukleusName,
        RouteKind routeKind,
        String localAddress,
        String remoteAddress,
        OctetsFW extension)
    {
        return (b, o, l) -> reaktorSignalExRW.wrap(b, o, l)
                                             .typeId(reaktorTypeId)
                                             .route(r -> r.correlationId(routeId)
                                                          .nukleus(nukleusName)
                                                          .role(m -> m.set(Role.valueOf(routeKind.ordinal())))
                                                          .localAddress(localAddress)
                                                          .remoteAddress(remoteAddress)
                                                          .extension(extension))
                                             .build()
                                             .sizeof();
    }

    private Flyweight.Builder.Visitor visitUnroutedSignalEx(
        long routeId,
        String nukleusName)
    {
        return (b, o, l) -> reaktorSignalExRW.wrap(b, o, l)
                                             .typeId(reaktorTypeId)
                                             .unroute(u -> u.correlationId(routeId)
                                                            .nukleus(nukleusName)
                                                            .routeId(routeId))
                                             .build()
                                             .sizeof();
    }

    private boolean handleExpire(
        TimeUnit timeUnit,
        long now,
        long timerId)
    {
        final Runnable task = tasksByTimerId.remove(timerId);
        if (task != null)
        {
            task.run();
        }
        return true;
    }

    private void handleRead(
        int msgTypeId,
        MutableDirectBuffer buffer,
        int index,
        int length)
    {
        final FrameFW frame = frameRO.wrap(buffer, index, index + length);
        final long streamId = frame.streamId();
        final long routeId = frame.routeId();
        final long sequence = frame.sequence();
        final long acknowledge = frame.acknowledge();
        final int maximum = frame.maximum();

        this.lastReadStreamId = streamId;

        if (streamId == 0L)
        {
            onSystemMessage(msgTypeId, buffer, index, length);
        }
        else if (isInitial(streamId))
        {
            handleReadInitial(routeId, streamId, sequence, acknowledge, maximum, msgTypeId, buffer, index, length);
        }
        else
        {
            handleReadReply(routeId, streamId, sequence, acknowledge, maximum, msgTypeId, buffer, index, length);
        }
    }

    private void handleReadInitial(
        long routeId,
        long streamId,
        long sequence,
        long acknowledge,
        int maximum,
        int msgTypeId,
        MutableDirectBuffer buffer,
        int index,
        int length)
    {
        final int instanceId = instanceId(streamId);

        if ((msgTypeId & 0x4000_0000) == 0)
        {
            final Int2ObjectHashMap<MessageConsumer> dispatcher = streams[streamIndex(streamId)];
            final MessageConsumer handler = dispatcher.get(instanceId);
            if (handler != null)
            {
                switch (msgTypeId)
                {
                case BeginFW.TYPE_ID:
                    handler.accept(msgTypeId, buffer, index, length);
                    break;
                case DataFW.TYPE_ID:
                    handler.accept(msgTypeId, buffer, index, length);
                    break;
                case EndFW.TYPE_ID:
                    handler.accept(msgTypeId, buffer, index, length);
                    dispatcher.remove(instanceId);
                    break;
                case AbortFW.TYPE_ID:
                    handler.accept(msgTypeId, buffer, index, length);
                    dispatcher.remove(instanceId);
                    break;
                case FlushFW.TYPE_ID:
                    handler.accept(msgTypeId, buffer, index, length);
                    break;
                default:
                    doReset(routeId, streamId, sequence, acknowledge, maximum);
                    break;
                }
            }
            else
            {
                handleDefaultReadInitial(msgTypeId, buffer, index, length);
            }
        }
        else
        {
            final Int2ObjectHashMap<MessageConsumer> dispatcher = throttles[throttleIndex(streamId)];
            final MessageConsumer throttle = dispatcher.get(instanceId);
            if (throttle != null)
            {
                final ReadCounters counters = countersByRouteId.computeIfAbsent(routeId, newReadCounters);
                switch (msgTypeId)
                {
                case WindowFW.TYPE_ID:
                    counters.windows.increment();
                    throttle.accept(msgTypeId, buffer, index, length);
                    break;
                case ResetFW.TYPE_ID:
                    counters.resets.increment();
                    throttle.accept(msgTypeId, buffer, index, length);
                    dispatcher.remove(instanceId);
                    break;
                case SignalFW.TYPE_ID:
                    final SignalFW signal = signalRO.wrap(buffer, index, index + length);
                    final long cancelId = signal.cancelId();
                    if (cancelId != NO_CANCEL_ID)
                    {
                        futuresById.remove(cancelId);
                    }
                    throttle.accept(msgTypeId, buffer, index, length);
                    break;
                case ChallengeFW.TYPE_ID:
                    throttle.accept(msgTypeId, buffer, index, length);
                    break;
                default:
                    break;
                }
            }
            else
            {
                switch (msgTypeId)
                {
                case SignalFW.TYPE_ID:
                    final SignalFW signal = signalRO.wrap(buffer, index, index + length);
                    final long cancelId = signal.cancelId();
                    if (cancelId != NO_CANCEL_ID)
                    {
                        futuresById.remove(cancelId);
                    }
                    break;
                }
            }
        }
    }

    private void handleDefaultReadInitial(
        int msgTypeId,
        MutableDirectBuffer buffer,
        int index,
        int length)
    {
        switch (msgTypeId)
        {
        case BeginFW.TYPE_ID:
            final MessageConsumer newHandler = handleBeginInitial(msgTypeId, buffer, index, length);
            if (newHandler != null)
            {
                newHandler.accept(msgTypeId, buffer, index, length);
            }
            else
            {
                final FrameFW frame = frameRO.wrap(buffer, index, index + length);
                final long streamId = frame.streamId();
                final long routeId = frame.routeId();
                final long sequence = frame.sequence();
                final long acknowledge = frame.acknowledge();
                final int maximum = frame.maximum();

                doReset(routeId, streamId, sequence, acknowledge, maximum);
            }
            break;
        case DataFW.TYPE_ID:
            handleDroppedReadData(msgTypeId, buffer, index, length);
            break;
        }
    }

    private void handleDroppedReadFrame(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        switch (msgTypeId)
        {
        case DataFW.TYPE_ID:
            handleDroppedReadData(msgTypeId, buffer, index, length);
            break;
        }
    }

    private void handleDroppedReadData(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        assert msgTypeId == DataFW.TYPE_ID;

        final DataFW data = dataRO.wrap(buffer, index, index + length);
        final long traceId = data.traceId();
        final long budgetId = data.budgetId();
        final int reserved = data.reserved();

        doSystemWindowIfNecessary(traceId, budgetId, reserved);
    }

    private void doSystemWindowIfNecessary(
        long traceId,
        long budgetId,
        int reserved)
    {
        if (budgetId != 0L && reserved > 0)
        {
            doSystemWindow(traceId, budgetId, reserved);
        }
    }

    private void handleReadReply(
        long routeId,
        long streamId,
        long sequence,
        long acknowledge,
        int maximum,
        int msgTypeId,
        MutableDirectBuffer buffer,
        int index,
        int length)
    {
        final int instanceId = instanceId(streamId);

        if ((msgTypeId & 0x4000_0000) == 0)
        {
            final Int2ObjectHashMap<MessageConsumer> dispatcher = streams[streamIndex(streamId)];
            final MessageConsumer handler = dispatcher.get(instanceId);
            if (handler != null)
            {
                final ReadCounters counters = countersByRouteId.computeIfAbsent(routeId, newReadCounters);
                switch (msgTypeId)
                {
                case BeginFW.TYPE_ID:
                    counters.opens.increment();
                    handler.accept(msgTypeId, buffer, index, length);
                    break;
                case DataFW.TYPE_ID:
                    counters.frames.increment();
                    counters.bytes.getAndAdd(buffer.getInt(index + DataFW.FIELD_OFFSET_LENGTH));
                    handler.accept(msgTypeId, buffer, index, length);
                    break;
                case EndFW.TYPE_ID:
                    counters.closes.increment();
                    handler.accept(msgTypeId, buffer, index, length);
                    dispatcher.remove(instanceId);
                    break;
                case AbortFW.TYPE_ID:
                    counters.aborts.increment();
                    handler.accept(msgTypeId, buffer, index, length);
                    dispatcher.remove(instanceId);
                    break;
                case FlushFW.TYPE_ID:
                    handler.accept(msgTypeId, buffer, index, length);
                    break;
                default:
                    doReset(routeId, streamId, sequence, acknowledge, maximum);
                    break;
                }
            }
            else
            {
                handleDefaultReadReply(msgTypeId, buffer, index, length);
            }
        }
        else
        {
            final Int2ObjectHashMap<MessageConsumer> dispatcher = throttles[throttleIndex(streamId)];
            final MessageConsumer throttle = dispatcher.get(instanceId);
            if (throttle != null)
            {
                switch (msgTypeId)
                {
                case WindowFW.TYPE_ID:
                    throttle.accept(msgTypeId, buffer, index, length);
                    break;
                case ResetFW.TYPE_ID:
                    throttle.accept(msgTypeId, buffer, index, length);
                    dispatcher.remove(instanceId);
                    break;
                case SignalFW.TYPE_ID:
                    final SignalFW signal = signalRO.wrap(buffer, index, index + length);
                    final long cancelId = signal.cancelId();
                    if (cancelId != NO_CANCEL_ID)
                    {
                        futuresById.remove(cancelId);
                    }
                    throttle.accept(msgTypeId, buffer, index, length);
                    break;
                case ChallengeFW.TYPE_ID:
                    throttle.accept(msgTypeId, buffer, index, length);
                    break;
                default:
                    break;
                }
            }
            else
            {
                switch (msgTypeId)
                {
                case SignalFW.TYPE_ID:
                    final SignalFW signal = signalRO.wrap(buffer, index, index + length);
                    final long cancelId = signal.cancelId();
                    if (cancelId != NO_CANCEL_ID)
                    {
                        futuresById.remove(cancelId);
                    }
                    break;
                }
            }
        }
    }

    private void handleDefaultReadReply(
        int msgTypeId,
        MutableDirectBuffer buffer,
        int index,
        int length)
    {
        if (msgTypeId == BeginFW.TYPE_ID)
        {
            final FrameFW frame = frameRO.wrap(buffer, index, index + length);
            final long routeId = frame.routeId();
            final long streamId = frame.streamId();
            final long sequence = frame.sequence();
            final long acknowledge = frame.acknowledge();
            final int maximum = frame.maximum();
            final MessageConsumer newHandler = handleBeginReply(msgTypeId, buffer, index, length);
            if (newHandler != null)
            {

                final ReadCounters counters = countersByRouteId.computeIfAbsent(routeId, newReadCounters);
                counters.opens.increment();
                newHandler.accept(msgTypeId, buffer, index, length);
            }
            else
            {
                doReset(routeId, streamId, sequence, acknowledge, maximum);
            }
        }
        else if (msgTypeId == DataFW.TYPE_ID)
        {
            handleDroppedReadData(msgTypeId, buffer, index, length);
        }
    }

    private MessageConsumer handleBeginInitial(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        final BeginFW begin = beginRO.wrap(buffer, index, index + length);
        final long routeId = begin.routeId();
        final long streamId = begin.streamId();
        final int addressId = remoteId(routeId);

        MessageConsumer newStream = null;

        final StreamFactory streamFactory = streamFactoriesByAddressId.get(addressId);
        if (streamFactory != null)
        {
            final MessageConsumer replyTo = supplyReplyTo(streamId);
            newStream = streamFactory.newStream(msgTypeId, buffer, index, length, replyTo);
            if (newStream != null)
            {
                streams[streamIndex(streamId)].put(instanceId(streamId), newStream);
            }
        }

        return newStream;
    }

    private MessageConsumer handleBeginReply(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        final BeginFW begin = beginRO.wrap(buffer, index, index + length);
        final long routeId = begin.routeId();
        final long streamId = begin.streamId();
        final int labelId = localId(routeId);

        MessageConsumer newStream = null;

        final StreamFactory streamFactory = streamFactoriesByAddressId.get(labelId);
        if (streamFactory != null)
        {
            final MessageConsumer replyTo = supplyReplyTo(streamId);
            newStream = streamFactory.newStream(msgTypeId, buffer, index, length, replyTo);
            if (newStream != null)
            {
                streams[streamIndex(streamId)].put(instanceId(streamId), newStream);
            }
        }

        return newStream;
    }

    private void doReset(
        final long routeId,
        final long streamId,
        final long sequence,
        final long acknowledge,
        final int maximum)
    {
        final ResetFW reset = resetRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(streamId)
                .sequence(sequence)
                .acknowledge(acknowledge)
                .maximum(maximum)
                .build();

        final MessageConsumer replyTo = supplyReplyTo(streamId);
        replyTo.accept(reset.typeId(), reset.buffer(), reset.offset(), reset.sizeof());
    }

    private void doSyntheticAbort(
        long streamId,
        MessageConsumer stream)
    {
        final long syntheticAbortRouteId = 0L;

        final AbortFW abort = abortRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                                     .routeId(syntheticAbortRouteId)
                                     .streamId(streamId)
                                     .sequence(-1L)
                                     .acknowledge(-1L)
                                     .maximum(0)
                                     .build();

        stream.accept(abort.typeId(), abort.buffer(), abort.offset(), abort.sizeof());
    }

    private MessageConsumer supplyReplyTo(
        long streamId)
    {
        final int index = streamIndex(streamId);
        return writersByIndex.computeIfAbsent(index, supplyWriter);
    }

    private MessageConsumer supplyInitialWriter(
        long streamId)
    {
        final int index = remoteIndex(streamId);
        return writersByIndex.computeIfAbsent(index, supplyWriter);
    }

    private MessageConsumer supplyWriter(
        int index)
    {
        return supplyTarget(index).writeHandler();
    }

    private Target supplyTarget(
        int index)
    {
        return targetsByIndex.computeIfAbsent(index, newTarget);
    }

    private Target newTarget(
        int index)
    {
        return new Target(config, index, writeBuffer, streams, throttles, newWriteCounters);
    }

    private ReadCounters newReadCounters(
        long routeId)
    {
        final int localId = localId(routeId);
        final String nukleus = nukleus(localId);
        return new ReadCounters(counters, nukleus, routeId);
    }

    private WriteCounters newWriteCounters(
        long routeId)
    {
        final int localId = localId(routeId);
        final String nukleus = nukleus(localId);
        return new WriteCounters(counters, nukleus, routeId);
    }

    private String nukleus(
        int localId)
    {
        final String localAddress = labels.lookupLabel(localId);
        final Matcher matcher = ADDRESS_PATTERN.matcher(localAddress);
        matcher.matches();
        return matcher.group(1);
    }

    private static final class ReadCounters
    {
        private final AtomicCounter opens;
        private final AtomicCounter closes;
        private final AtomicCounter aborts;
        private final AtomicCounter windows;
        private final AtomicCounter resets;
        private final AtomicCounter bytes;
        private final AtomicCounter frames;

        ReadCounters(
            Counters counters,
            String nukleus,
            long routeId)
        {
            this.opens = counters.counter(format("%s.%d.opens.read", nukleus, routeId));
            this.closes = counters.counter(format("%s.%d.closes.read", nukleus, routeId));
            this.aborts = counters.counter(format("%s.%d.aborts.read", nukleus, routeId));
            this.windows = counters.counter(format("%s.%d.windows.read", nukleus, routeId));
            this.resets = counters.counter(format("%s.%d.resets.read", nukleus, routeId));
            this.bytes = counters.counter(format("%s.%d.bytes.read", nukleus, routeId));
            this.frames = counters.counter(format("%s.%d.frames.read", nukleus, routeId));
        }
    }

    private final class ElektronRef
    {
        private final Elektron elektron;
        private final Map<RouteKind, StreamFactory> streamFactories;
        private final Map<RouteKind, AddressFactory> addressFactories;

        private int count;

        private ElektronRef(
            String nukleusName,
            Elektron elekron)
        {
            this.elektron = requireNonNull(elekron);

            final Map<RouteKind, StreamFactory> streamFactories = new EnumMap<>(RouteKind.class);
            final Map<RouteKind, AddressFactory> addressFactories = new EnumMap<>(RouteKind.class);
            final Map<String, AtomicCounter> countersByName = new HashMap<>();
            final Function<String, AtomicCounter> newCounter = counters::counter;
            final Function<String, LongSupplier> supplyCounter =
                name -> () -> countersByName.computeIfAbsent(name, newCounter).increment() + 1;
            final Function<String, LongConsumer> supplyAccumulator = name -> inc -> counters.counter(name).getAndAdd(inc);
            final AtomicCounter acquires = counters.counter(String.format("%s.acquires", nukleusName));
            final AtomicCounter releases = counters.counter(String.format("%s.releases", nukleusName));
            final BufferPool countingPool = new CountingBufferPool(bufferPool, acquires::increment, releases::increment);
            final Supplier<BufferPool> supplyCountingBufferPool = () -> countingPool;

            for (RouteKind routeKind : EnumSet.allOf(RouteKind.class))
            {
                final AddressFactoryBuilder addressFactoryBuilder = elektron.addressFactoryBuilder(routeKind);
                if (addressFactoryBuilder != null)
                {
                    AddressFactory addressFactory = newAddressFactory(addressFactoryBuilder);
                    addressFactories.put(routeKind, addressFactory);
                }

                final StreamFactoryBuilder streamFactoryBuilder = elektron.streamFactoryBuilder(routeKind);
                if (streamFactoryBuilder != null)
                {
                    StreamFactory streamFactory =
                            newStreamFactory(supplyCounter, supplyAccumulator, supplyCountingBufferPool, streamFactoryBuilder);
                    streamFactories.put(routeKind, streamFactory);
                }
            }
            this.addressFactories = addressFactories;
            this.streamFactories = streamFactories;
        }

        public ElektronRef assign(
            RouteKind routeKind,
            int labelId)
        {
            synchronized (this)
            {
                if (this.count == 0)
                {
                    final Agent agent = elektron.agent();
                    if (agent != null)
                    {
                        agents = ArrayUtil.add(agents, agent);
                    }
                }

                final StreamFactory streamFactory = streamFactories.get(routeKind);
                if (streamFactory != null)
                {
                    streamFactoriesByAddressId.put(labelId, streamFactory);
                }
                this.count++;
            }

            return this;
        }

        public ElektronRef unassign(
            RouteKind routeKind,
            int labelId)
        {
            synchronized (this)
            {
                this.count--;

                if (this.count == 0)
                {
                    final StreamFactory streamFactory = streamFactoriesByAddressId.remove(labelId);
                    assert streamFactory == streamFactories.get(routeKind);

                    final Agent agent = elektron.agent();
                    if (agent != null)
                    {
                        // TODO: quiesce streams first
                        agents = ArrayUtil.remove(agents, agent);
                        final Agent closeAgent = new Agent()
                        {

                            @Override
                            public int doWork() throws Exception
                            {
                                quietClose(agent::onClose);
                                agents = ArrayUtil.remove(agents, this);
                                return 1;
                            }

                            @Override
                            public String roleName()
                            {
                                return String.format("%s (deferred close)", agent.roleName());
                            }
                        };
                        agents = ArrayUtil.add(agents, closeAgent);
                    }
                }
            }

            return this;
        }
    }

    private AddressFactory newAddressFactory(
        AddressFactoryBuilder addressFactoryBuilder)
    {
        return addressFactoryBuilder
                .setRouter(resolver)
                .setWriteBuffer(writeBuffer)
                .setTypeIdSupplier(labels::supplyLabelId)
                .setTraceIdSupplier(this::supplyTraceId)
                .setInitialIdSupplier(this::supplyInitialId)
                .setReplyIdSupplier(this::supplyReplyId)
                .build();
    }

    private StreamFactory newStreamFactory(
        final Function<String, LongSupplier> supplyCounter,
        final Function<String, LongConsumer> supplyAccumulator,
        final Supplier<BufferPool> supplyCountingBufferPool,
        final StreamFactoryBuilder streamFactoryBuilder)
    {
        return streamFactoryBuilder
                .setRouteManager(resolver)
                .setExecutor(executor)
                .setSignaler(signaler)
                .setWriteBuffer(writeBuffer)
                .setTypeIdSupplier(labels::supplyLabelId)
                .setInitialIdSupplier(this::supplyInitialId)
                .setReplyIdSupplier(this::supplyReplyId)
                .setTraceIdSupplier(this::supplyTraceId)
                .setBudgetIdSupplier(this::supplyBudgetId)
                .setBudgetCreditor(creditor)
                .setBudgetDebitorSupplier(this::supplyBudgetDebitor)
                .setCounterSupplier(supplyCounter)
                .setAccumulatorSupplier(supplyAccumulator)
                .setBufferPoolSupplier(supplyCountingBufferPool)
                .setDroppedFrameConsumer(this::handleDroppedReadFrame)
                .setRemoteIndexSupplier(StreamId::remoteIndex)
                .setHostResolver(resolveHost)
                .build();
    }

    private StreamFactory supplyStreamFactory(
        int addressId)
    {
        return streamFactoriesByAddressId.get(addressId);
    }

    private MessageConsumer supplyThrottle(
        long streamId)
    {
        final int instanceId = instanceId(streamId);
        final Int2ObjectHashMap<MessageConsumer> dispatcher = throttles[throttleIndex(streamId)];
        return dispatcher.get(instanceId);
    }

    private void removeThrottle(
        long streamId)
    {
        final int instanceId = instanceId(streamId);
        final Int2ObjectHashMap<MessageConsumer> dispatcher = throttles[throttleIndex(streamId)];
        dispatcher.remove(instanceId);
    }

    private long supplyInitialId(
        long routeId)
    {
        final int remoteId = remoteId(routeId);
        final int remoteIndex = resolveRemoteIndex(remoteId);

        streamId += 2L;
        streamId &= mask;

        return (((long)remoteIndex << 48) & 0x00ff_0000_0000_0000L) |
               (streamId & 0xff00_ffff_ffff_ffffL) | 0x0000_0000_0000_0001L;
    }

    private long supplyReplyId(
        long initialId)
    {
        assert isInitial(initialId);
        return initialId & 0xffff_ffff_ffff_fffeL;
    }

    private long supplyBudgetId()
    {
        budgetId++;
        budgetId &= mask;
        return budgetId;
    }

    private BudgetDebitor supplyBudgetDebitor(
        long budgetId)
    {
        final int ownerIndex = (int) ((budgetId >> shift) & 0xFFFF_FFFF);
        return debitorsByIndex.computeIfAbsent(ownerIndex, this::newBudgetDebitor);
    }

    private DefaultBudgetDebitor newBudgetDebitor(
        int ownerIndex)
    {
        final BudgetsLayout layout = new BudgetsLayout.Builder()
                .path(config.directory().resolve(String.format("budgets%d", ownerIndex)))
                .owner(false)
                .build();

        return new DefaultBudgetDebitor(localIndex, ownerIndex, layout);
    }

    private long supplyTraceId()
    {
        traceId++;
        traceId &= mask;
        return traceId;
    }

    private int resolveRemoteIndex(
        int remoteId)
    {
        final Affinity affinity = supplyAffinity(remoteId);
        final BitSet mask = affinity.mask;
        final int remoteIndex = affinity.nextIndex;

        // currently round-robin with prefer-local only
        assert mask.cardinality() != 0;
        if (remoteIndex != localIndex)
        {
            int nextIndex = affinity.mask.nextSetBit(remoteIndex + 1);
            if (nextIndex == -1)
            {
                nextIndex = affinity.mask.nextSetBit(0);
            }
            affinity.nextIndex = nextIndex;
        }

        return remoteIndex;
    }

    private Affinity supplyAffinity(
        int remoteId)
    {
        return affinityByRemoteId.computeIfAbsent(remoteId, resolveAffinity);
    }

    public Affinity resolveAffinity(
        long remoteIdAsLong)
    {
        final int remoteId = (int)(remoteIdAsLong & 0xffff_ffffL);
        String remoteAddress = labels.lookupLabel(remoteId);

        BitSet mask = affinityMask.apply(remoteAddress);

        if (mask.cardinality() == 0)
        {
            throw new IllegalStateException(String.format("affinity mask must specify at least one bit: %s %d",
                    remoteAddress, mask));
        }

        Affinity affinity = new Affinity();
        affinity.mask = mask;
        affinity.nextIndex = mask.get(localIndex) ? localIndex : mask.nextSetBit(0);

        return affinity;
    }

    private static SignalFW.Builder newSignalRW()
    {
        MutableDirectBuffer buffer = new UnsafeBuffer(new byte[512]);
        return new SignalFW.Builder().wrap(buffer, 0, buffer.capacity());
    }

    private Int2ObjectHashMap<MessageConsumer>[] initDispatcher()
    {
        @SuppressWarnings("unchecked")
        Int2ObjectHashMap<MessageConsumer>[] dispatcher = new Int2ObjectHashMap[64];
        for (int i = 0; i < dispatcher.length; i++)
        {
            dispatcher[i] = new Int2ObjectHashMap<>();
        }
        return dispatcher;
    }

    private final class ElektronSignaler implements Signaler
    {
        private final ThreadLocal<SignalFW.Builder> signalRW = withInitial(ElektronAgent::newSignalRW);

        private final ExecutorService executorService;

        private long nextFutureId;

        private ElektronSignaler(
            ExecutorService executorService)
        {
            this.executorService = executorService;
        }

        public void executeTaskAt(
            long timeMillis,
            Runnable task)
        {
            final long timerId = timerWheel.scheduleTimer(timeMillis);
            final Runnable oldTask = tasksByTimerId.put(timerId, task);
            assert oldTask == null;
            assert timerId >= 0L;
        }

        @Override
        public long signalAt(
            long timeMillis,
            int signalId,
            IntConsumer handler)
        {
            final long timerId = timerWheel.scheduleTimer(timeMillis);
            final Runnable task = () -> handler.accept(signalId);
            final Runnable oldTask = tasksByTimerId.put(timerId, task);
            assert oldTask == null;
            assert timerId >= 0L;
            return timerId;
        }

        @Override
        public long signalAt(
            long timeMillis,
            long routeId,
            long streamId,
            int signalId)
        {
            final long timerId = timerWheel.scheduleTimer(timeMillis);
            final Runnable task = () -> signal(routeId, streamId, 0L, 0L, NO_CANCEL_ID, signalId);
            final Runnable oldTask = tasksByTimerId.put(timerId, task);
            assert oldTask == null;
            assert timerId >= 0L;
            return timerId;
        }

        @Override
        public long signalTask(
            Runnable task,
            long routeId,
            long streamId,
            int signalId)
        {
            long cancelId;

            if (executorService != null)
            {
                nextFutureId = (nextFutureId + 1) & 0x7fff_ffff_ffff_ffffL;
                final long newFutureId = (nextFutureId << 1) | 0x8000_0000_0000_0001L;
                assert newFutureId != NO_CANCEL_ID;

                final Future<?> newFuture =
                    executorService.submit(() -> invokeAndSignal(task, routeId, streamId, 0L, 0L, newFutureId, signalId));
                final Future<?> oldFuture = futuresById.put(newFutureId, newFuture);
                assert oldFuture == null;
                cancelId = newFutureId;
            }
            else
            {
                cancelId = NO_CANCEL_ID;
                invokeAndSignal(task, routeId, streamId, 0L, 0L, cancelId, signalId);
            }

            assert cancelId < 0L;

            return cancelId;
        }

        @Override
        public void signalNow(
            long routeId,
            long streamId,
            int signalId)
        {
            signal(routeId, streamId, 0L, 0L, NO_CANCEL_ID, signalId);
        }

        @Override
        public boolean cancel(
            long cancelId)
        {
            boolean cancelled = false;

            if (cancelId > 0L)
            {
                final long timerId = cancelId;
                cancelled = timerWheel.cancelTimer(timerId);
                tasksByTimerId.remove(timerId);
            }
            else if (cancelId != NO_CANCEL_ID)
            {
                final long futureId = cancelId;
                final Future<?> future = futuresById.remove(futureId);
                cancelled = future != null && future.cancel(true);
            }

            return cancelled;
        }

        private void invokeAndSignal(
            Runnable task,
            long routeId,
            long streamId,
            long sequence,
            long acknowledge,
            long cancelId,
            int signalId)
        {
            try
            {
                task.run();
            }
            finally
            {
                signal(routeId, streamId, sequence, acknowledge, cancelId, signalId);
            }
        }

        private void signal(
            long routeId,
            long streamId,
            long sequence,
            long acknowledge,
            long cancelId,
            int signalId)
        {
            final long timestamp = timestamps ? System.nanoTime() : 0L;

            final SignalFW signal = signalRW.get()
                                            .rewrap()
                                            .routeId(routeId)
                                            .streamId(streamId)
                                            .sequence(sequence)
                                            .acknowledge(acknowledge)
                                            .maximum(0)
                                            .timestamp(timestamp)
                                            .traceId(supplyTraceId())
                                            .cancelId(cancelId)
                                            .signalId(signalId)
                                            .build();

            streamsBuffer.write(signal.typeId(), signal.buffer(), signal.offset(), signal.sizeof());
        }
    }

    private final class ElektronExecutor implements SignalingExecutor
    {
        private final ThreadLocal<SignalFW.Builder> signalRW = withInitial(ElektronAgent::newSignalRW);

        private final ExecutorService executorService;

        private ElektronExecutor(
            ExecutorService executorService)
        {
            this.executorService = executorService;
        }

        @Override
        public Future<?> schedule(
            long delay,
            TimeUnit unit,
            long routeId,
            long streamId,
            long signalId)
        {
            final long timerId = timerWheel.scheduleTimer(currentTimeMillis() + unit.toMillis(delay));
            final FutureTask<Object> task = new FutureTask<>(() -> signal(routeId, streamId, signalId), null)
            {
                @Override
                protected void done()
                {
                    if (isCancelled())
                    {
                        tasksByTimerId.remove(timerId);
                        timerWheel.cancelTimer(timerId);
                    }
                }
            };

            tasksByTimerId.put(timerId, task);
            return task;
        }

        @Override
        public Future<?> execute(
            Runnable task,
            long routeId,
            long streamId,
            long signalId)
        {
            if (executorService != null)
            {
                return executorService.submit(() -> invokeAndSignal(task, routeId, streamId, signalId));
            }
            else
            {
                invokeAndSignal(task, routeId, streamId, signalId);
                return new Future<Void>()
                {
                    @Override
                    public boolean cancel(
                        boolean mayInterruptIfRunning)
                    {
                        return false;
                    }

                    @Override
                    public boolean isCancelled()
                    {
                        return false;
                    }

                    @Override
                    public boolean isDone()
                    {
                        return true;
                    }

                    @Override
                    public Void get() throws InterruptedException, ExecutionException
                    {
                        return null;
                    }

                    @Override
                    public Void get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException
                    {
                        return null;
                    }
                };
            }
        }

        private void invokeAndSignal(
            Runnable task,
            long routeId,
            long streamId,
            long signalId)
        {
            try
            {
                task.run();
            }
            finally
            {
                signal(routeId, streamId, signalId);
            }
        }

        private void signal(
            long routeId,
            long streamId,
            long signalId)
        {
            final long timestamp = timestamps ? System.nanoTime() : 0L;

            final SignalFW signal = signalRW.get()
                                            .rewrap()
                                            .routeId(routeId)
                                            .streamId(streamId)
                                            .sequence(0L)
                                            .acknowledge(0L)
                                            .maximum(0)
                                            .timestamp(timestamp)
                                            .traceId(supplyTraceId())
                                            .cancelId(NO_CANCEL_ID)
                                            .signalId((int) signalId)
                                            .build();

            streamsBuffer.write(signal.typeId(), signal.buffer(), signal.offset(), signal.sizeof());
        }
    }

    private static class Affinity
    {
        BitSet mask;
        int nextIndex;
    }
}
