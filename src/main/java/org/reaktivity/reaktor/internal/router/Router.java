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
package org.reaktivity.reaktor.internal.router;

import static java.lang.String.format;
import static org.agrona.CloseHelper.quietClose;
import static org.agrona.LangUtil.rethrowUnchecked;

import java.nio.ByteBuffer;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.agrona.DirectBuffer;
import org.agrona.LangUtil;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.concurrent.MessageHandler;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.ringbuffer.RingBuffer;
import org.agrona.concurrent.status.AtomicCounter;
import org.reaktivity.nukleus.Nukleus;
import org.reaktivity.nukleus.buffer.BufferPool;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.function.MessageFunction;
import org.reaktivity.nukleus.function.MessagePredicate;
import org.reaktivity.nukleus.route.RouteManager;
import org.reaktivity.nukleus.stream.StreamFactory;
import org.reaktivity.nukleus.stream.StreamFactoryBuilder;
import org.reaktivity.reaktor.internal.Context;
import org.reaktivity.reaktor.internal.Counters;
import org.reaktivity.reaktor.internal.State;
import org.reaktivity.reaktor.internal.buffer.CountingBufferPool;
import org.reaktivity.reaktor.internal.conductor.Conductor;
import org.reaktivity.reaktor.internal.layouts.RoutesLayout;
import org.reaktivity.reaktor.internal.layouts.StreamsLayout;
import org.reaktivity.reaktor.internal.types.OctetsFW;
import org.reaktivity.reaktor.internal.types.control.Role;
import org.reaktivity.reaktor.internal.types.control.RouteFW;
import org.reaktivity.reaktor.internal.types.control.UnrouteFW;
import org.reaktivity.reaktor.internal.types.state.RouteEntryFW;
import org.reaktivity.reaktor.internal.types.state.RouteTableFW;
import org.reaktivity.reaktor.internal.types.stream.AbortFW;
import org.reaktivity.reaktor.internal.types.stream.BeginFW;
import org.reaktivity.reaktor.internal.types.stream.DataFW;
import org.reaktivity.reaktor.internal.types.stream.EndFW;
import org.reaktivity.reaktor.internal.types.stream.FrameFW;
import org.reaktivity.reaktor.internal.types.stream.ResetFW;
import org.reaktivity.reaktor.internal.types.stream.WindowFW;

public final class Router implements RouteManager, Nukleus
{
    private static final Pattern ADDRESS_PATTERN = Pattern.compile("^([^#]+)(:?#.*)$");

    private final RouteFW routeRO = new RouteFW();
    private final RouteTableFW routeTableRO = new RouteTableFW();

    private final RouteFW.Builder routeRW = new RouteFW.Builder();
    private final RouteTableFW.Builder routeTableRW = new RouteTableFW.Builder();

    private final FrameFW frameRO = new FrameFW();
    private final BeginFW beginRO = new BeginFW();
    private final AbortFW.Builder abortRW = new AbortFW.Builder();

    private final ResetFW.Builder resetRW = new ResetFW.Builder();

    private final Context context;
    private final State state;
    private final String nukleusName;
    private final Counters counters;
    private final MutableDirectBuffer writeBuffer;
    private final Int2ObjectHashMap<Target> targetsByLabelId;
    private final Map<String, Target> targetsByName;
    private final MutableDirectBuffer routeBuf;
    private final GroupBudgetManager groupBudgetManager;

    private final RoutesLayout routesLayout;
    private final MutableDirectBuffer routesBuffer;
    private final int routesBufferCapacity;

    private final Long2ObjectHashMap<MessageConsumer> streams;
    private final Long2ObjectHashMap<MessageConsumer> throttles;
    private final Long2ObjectHashMap<ReadCounters> countersByRouteId;

    private final Map<Integer, Role> localRoles;

    private final Map<Role, StreamFactory> streamFactories;
    private final StreamsLayout streamsLayout;
    private final int messageCountLimit;
    private final RingBuffer streamsBuffer;
    private final MessageHandler readHandler;

    private Conductor conductor;
    private boolean timestamps;
    private Function<Role, MessagePredicate> supplyRouteHandler;

    public Router(
        Context context,
        State state,
        Function<Role, StreamFactoryBuilder> supplyStreamFactoryBuilder)
    {
        this.context = context;
        this.state = state;
        this.nukleusName = context.name();
        this.counters = context.counters();
        this.writeBuffer = new UnsafeBuffer(new byte[context.maxMessageLength()]);
        this.targetsByLabelId = new Int2ObjectHashMap<>();
        this.targetsByName = new HashMap<>();
        this.routeBuf = new UnsafeBuffer(ByteBuffer.allocateDirect(context.maxControlCommandLength()));
        this.groupBudgetManager = new GroupBudgetManager();
        this.routesLayout = context.routesLayout();
        this.routesBuffer = routesLayout.routesBuffer();
        this.routesBufferCapacity = routesLayout.capacity();
        this.streams = new Long2ObjectHashMap<>();
        this.throttles = new Long2ObjectHashMap<>();
        this.countersByRouteId = new Long2ObjectHashMap<>();
        this.localRoles = new ConcurrentHashMap<>();

        final Map<Role, StreamFactory> streamFactories = new EnumMap<>(Role.class);
        final Function<String, LongSupplier> supplyCounter = name -> () -> context.counters().counter(name).increment() + 1;
        final Function<String, LongConsumer> supplyAccumulator = name -> (i) -> context.counters().counter(name).add(i);
        final AtomicCounter acquires = context.counters().acquires();
        final AtomicCounter releases = context.counters().releases();
        final BufferPool bufferPool = new CountingBufferPool(state.bufferPool(), acquires::increment, releases::increment);
        final Supplier<BufferPool> supplyCountingBufferPool = () -> bufferPool;
        for (Role role : EnumSet.allOf(Role.class))
        {
            final StreamFactoryBuilder streamFactoryBuilder = supplyStreamFactoryBuilder.apply(role);
            if (streamFactoryBuilder != null)
            {
                StreamFactory streamFactory = streamFactoryBuilder
                        .setRouteManager(this)
                        .setWriteBuffer(writeBuffer)
                        .setInitialIdSupplier(state::supplyInitialId)
                        .setReplyIdSupplier(state::supplyReplyId)
                        .setSourceCorrelationIdSupplier(state::supplyCorrelationId)
                        .setTargetCorrelationIdSupplier(state::supplyCorrelationId)
                        .setTraceSupplier(state::supplyTrace)
                        .setGroupIdSupplier(state::supplyGroupId)
                        .setGroupBudgetClaimer(groupBudgetManager::claim)
                        .setGroupBudgetReleaser(groupBudgetManager::release)
                        .setCounterSupplier(supplyCounter)
                        .setAccumulatorSupplier(supplyAccumulator)
                        .setBufferPoolSupplier(supplyCountingBufferPool)
                        .build();
                streamFactories.put(role, streamFactory);
            }
        }
        this.streamFactories = streamFactories;

        final StreamsLayout streamsLayout = new StreamsLayout.Builder()
                .path(context.sourceStreamsPath())
                .streamsCapacity(context.streamsBufferCapacity())
                .readonly(false)
                .build();

        this.streamsLayout = streamsLayout;
        this.messageCountLimit = context.maximumMessagesPerRead();
        this.streamsBuffer = streamsLayout.streamsBuffer();
        this.readHandler = this::handleRead;
    }

    public void setConductor(
        Conductor conductor)
    {
        this.conductor = conductor;
    }

    public void setTimestamps(
        boolean timestamps)
    {
        this.timestamps = timestamps;
    }

    public void setRouteHandlerSupplier(
        Function<Role, MessagePredicate> supplyRouteHandler)
    {
        this.supplyRouteHandler = supplyRouteHandler;
    }

    @Override
    public String name()
    {
        return "router";
    }

    @Override
    public int process()
    {
        return streamsBuffer.read(readHandler, messageCountLimit);
    }

    @Override
    public MessageConsumer supplyReceiver(
        long routeId)
    {
        final int labelId = remoteId(routeId);
        return targetsByLabelId.computeIfAbsent(labelId, this::newTarget).writeHandler();
    }

    @Override
    public MessageConsumer supplySender(
        long routeId)
    {
        final int labelId = localId(routeId);
        return targetsByLabelId.computeIfAbsent(labelId, this::newTarget).writeHandler();
    }

    @Override
    public void setThrottle(
        long streamId,
        MessageConsumer throttle)
    {
        throttles.put(streamId, throttle);
    }

    @Override
    public <R> R resolveExternal(
        long authorization,
        MessagePredicate filter,
        MessageFunction<R> mapper)
    {
        RouteTableFW routeTable = routeTableRO.wrap(routesBuffer, 0, routesBufferCapacity);

        assert routeTable.writeLockReleases() == routeTable.writeLockAcquires();

        RouteEntryFW routeEntry = routeTable.routeEntries().matchFirst(re ->
        {
            final OctetsFW entry = re.route();
            final RouteFW candidate = routeRO.wrap(entry.buffer(), entry.offset(), entry.limit());
            return (authorization & candidate.authorization()) == candidate.authorization() &&
                   filter.test(candidate.typeId(), candidate.buffer(), candidate.offset(), candidate.sizeof());
        });

        R result = null;
        if (routeEntry != null)
        {
            final OctetsFW entry = routeEntry.route();
            final RouteFW route = routeRO.wrap(entry.buffer(), entry.offset(), entry.limit());
            result = mapper.apply(route.typeId(), route.buffer(), route.offset(), route.sizeof());
        }
        return result;
    }

    @Override
    public <R> R resolve(
        long routeId,
        long authorization,
        MessagePredicate filter,
        MessageFunction<R> mapper)
    {
        RouteTableFW routeTable = routeTableRO.wrap(routesBuffer, 0, routesBufferCapacity);

        assert routeTable.writeLockReleases() == routeTable.writeLockAcquires();

        RouteEntryFW routeEntry = routeTable.routeEntries().matchFirst(re ->
        {
            final OctetsFW entry = re.route();
            final RouteFW candidate = routeRO.wrap(entry.buffer(), entry.offset(), entry.limit());
            return remoteId(routeId) == localId(candidate.correlationId()) &&
                   (authorization & candidate.authorization()) == candidate.authorization() &&
                   filter.test(candidate.typeId(), candidate.buffer(), candidate.offset(), candidate.sizeof());
        });

        R result = null;
        if (routeEntry != null)
        {
            final OctetsFW entry = routeEntry.route();
            final RouteFW route = routeRO.wrap(entry.buffer(), entry.offset(), entry.limit());
            result = mapper.apply(route.typeId(), route.buffer(), route.offset(), route.sizeof());
        }
        return result;
    }

    @Override
    public void forEach(
        MessageConsumer consumer)
    {
        RouteTableFW routeTable = routeTableRO.wrap(routesBuffer, 0, routesBufferCapacity);

        assert routeTable.writeLockReleases() == routeTable.writeLockAcquires();

        routeTable.routeEntries().forEach(re ->
        {
            final OctetsFW entry = re.route();
            final RouteFW route = routeRO.wrap(entry.buffer(), entry.offset(), entry.limit());
            consumer.accept(route.typeId(), route.buffer(), route.offset(), route.sizeof());
        });
    }

    @Override
    public void close() throws Exception
    {
        targetsByName.forEach((k, v) -> v.detach());
        targetsByName.forEach((k, v) -> quietClose(v));

        streams.forEach(this::doSyntheticAbort);

        streamsLayout.close();
    }

    public MessageConsumer supplyReplyTo(
        long routeId,
        long streamId)
    {
        if ((streamId & 0x8000_0000_0000_0000L) == 0L)
        {
            return supplySender(routeId);
        }
        else
        {
            return supplyReceiver(routeId);
        }
    }

    public void doRoute(
        RouteFW route)
    {
        final long correlationId = route.correlationId();

        try
        {
            final Role role = route.role().get();

            MessagePredicate routeHandler = supplyRouteHandler.apply(role);
            if (routeHandler == null)
            {
                routeHandler = (t, b, i, l) -> true;
            }

            route = generateRouteId(route);

            if (doRouteInternal(route, routeHandler))
            {
                final long newRouteId = route.correlationId();
                conductor.onRouted(correlationId, newRouteId);
            }
            else
            {
                conductor.onError(correlationId);
            }
        }
        catch (Exception ex)
        {
            conductor.onError(correlationId);
            LangUtil.rethrowUnchecked(ex);
        }
    }

    public void doUnroute(
        UnrouteFW unroute)
    {
        final long correlationId = unroute.correlationId();

        try
        {
            final long routeId = unroute.routeId();
            final Role role = replyRole(routeId);
            MessagePredicate routeHandler = supplyRouteHandler.apply(role);
            if (routeHandler == null)
            {
                routeHandler = (t, b, i, l) -> true;
            }

            if (doUnrouteInternal(unroute, routeHandler))
            {
                conductor.onUnrouted(correlationId);
            }
            else
            {
                conductor.onError(correlationId);
            }
        }
        catch (Exception ex)
        {
            conductor.onError(correlationId);
            LangUtil.rethrowUnchecked(ex);
        }
    }

    private boolean doRouteInternal(
        RouteFW route,
        MessagePredicate routeHandler)
    {
        final RouteTableFW routeTable = routeTableRO.wrap(routesBuffer, 0, routesBufferCapacity);

        assert routeTable.writeLockReleases() == routeTable.writeLockAcquires();

        final boolean routed = routeHandler.test(route.typeId(), route.buffer(), route.offset(), route.sizeof());

        if (routed)
        {
            int readLock = routesLayout.lock();

            routeTableRW
                .wrap(routesBuffer, 0, routesBufferCapacity)
                .writeLockAcquires(readLock)
                .writeLockReleases(readLock - 1)
                .routeEntries(res ->
                {
                    routeTable.routeEntries().forEach(old -> res.item(e -> e.route(old.route())));
                    res.item(re -> re.route(route.buffer(), route.offset(), route.sizeof()));
                })
                .build();

            routesLayout.unlock();
        }

        return routed;
    }


    private boolean doUnrouteInternal(
        UnrouteFW unroute,
        MessagePredicate routeHandler)
    {
        RouteTableFW routeTable = routeTableRO.wrap(routesBuffer, 0, routesBufferCapacity);

        assert routeTable.writeLockReleases() == routeTable.writeLockAcquires();

        int readLock = routesLayout.lock();

        int beforeSize = routeTable.sizeof();

        RouteTableFW newRouteTable = routeTableRW.wrap(routesBuffer, 0, routesBufferCapacity)
            .writeLockAcquires(readLock)
            .writeLockReleases(readLock - 1)
            .routeEntries(res ->
            {
                routeTable.routeEntries().forEach(old ->
                {
                    final OctetsFW entry = old.route();
                    final RouteFW route = routeRO.wrap(entry.buffer(), entry.offset(), entry.limit());
                    if (unroute.routeId() != route.correlationId() ||
                        !routeHandler.test(UnrouteFW.TYPE_ID, unroute.buffer(), unroute.offset(), unroute.sizeof()))
                    {
                        res.item(re -> re.route(route.buffer(), route.offset(), route.sizeof()));
                    }
                });
            })
            .build();

        int afterSize = newRouteTable.sizeof();

        routesLayout.unlock();

        return beforeSize > afterSize;
    }

    private Target newTarget(
        int labelId)
    {
        final String addressName = state.lookupLabel(labelId);
        final Matcher matcher = ADDRESS_PATTERN.matcher(addressName);
        matcher.matches();
        final String targetName = matcher.group(1);

        return targetsByName.computeIfAbsent(targetName, this::newTarget);
    }

    private Target newTarget(
        String targetName)
    {
        StreamsLayout layout = new StreamsLayout.Builder()
                .path(context.targetStreamsPath().apply(targetName))
                .streamsCapacity(context.streamsBufferCapacity())
                .readonly(true)
                .build();

        return new Target(targetName, layout, writeBuffer, counters, timestamps,
                          context.maximumMessagesPerRead(), streams, throttles);
    }

    private RouteFW generateRouteId(
        RouteFW route)
    {
        final Role role = route.role().get();
        final String nukleus = route.nukleus().asString();
        final String localAddress = route.localAddress().asString();
        final String remoteAddress = route.remoteAddress().asString();
        final long authorization = route.authorization();
        final OctetsFW extension = route.extension();

        final int localId = state.supplyLabelId(localAddress);
        final int remoteId = state.supplyLabelId(remoteAddress);

        final Role existingRole = localRoles.putIfAbsent(localId, role);
        if (existingRole != null && existingRole != role)
        {
            throw new IllegalArgumentException("localAddress " + localAddress + " reused with different Role");
        }

        final long newRouteId =
                (long) localId << 48 |
                (long) remoteId << 32 |
                (long) role.ordinal() << 28 |
                (long) (state.supplyRouteId() & 0x0fff_ffff);

        return routeRW.wrap(routeBuf, 0, routeBuf.capacity())
                      .correlationId(newRouteId)
                      .nukleus(nukleus)
                      .role(b -> b.set(role))
                      .authorization(authorization)
                      .localAddress(localAddress)
                      .remoteAddress(remoteAddress)
                      .extension(b -> b.set(extension))
                      .build();
    }

    private Role resolveRole(
        long routeId,
        long streamId)
    {
        return (streamId & 0x8000_0000_0000_0000L) == 0L
                ? localRoles.get(remoteId(routeId))
                : replyRole(routeId);
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

    private Role replyRole(
        long routeId)
    {
        return Role.valueOf((int)(routeId >> 28) & 0x0f);
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

        try
        {
            if ((streamId & 0x8000_0000_0000_0000L) == 0L)
            {
                handleReadInitial(routeId, streamId, msgTypeId, buffer, index, length);
            }
            else
            {
                handleReadReply(routeId, streamId, msgTypeId, buffer, index, length);
            }
        }
        catch (Throwable ex)
        {
            ex.addSuppressed(new Exception(String.format("[%s]\t[0x%016x] %s",
                                                         nukleusName, streamId, streamsLayout)));
            rethrowUnchecked(ex);
        }
    }

    private void handleReadInitial(
        long routeId,
        long streamId,
        int msgTypeId,
        MutableDirectBuffer buffer,
        int index,
        int length)
    {
        if ((msgTypeId & 0x4000_0000) == 0)
        {
            final MessageConsumer handler = streams.get(streamId);
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
                    streams.remove(streamId);
                    break;
                case AbortFW.TYPE_ID:
                    handler.accept(msgTypeId, buffer, index, length);
                    streams.remove(streamId);
                    break;
                default:
                    doReset(routeId, streamId);
                    break;
                }
            }
            else if (msgTypeId == BeginFW.TYPE_ID)
            {
                final MessageConsumer newHandler = handleBegin(msgTypeId, buffer, index, length);
                if (newHandler != null)
                {
                    newHandler.accept(msgTypeId, buffer, index, length);
                }
                else
                {
                    doReset(routeId, streamId);
                }
            }
        }
        else
        {
            final MessageConsumer throttle = throttles.get(streamId);
            if (throttle != null)
            {
                final ReadCounters counters = countersByRouteId.computeIfAbsent(routeId, ReadCounters::new);
                switch (msgTypeId)
                {
                case WindowFW.TYPE_ID:
                    counters.windows.increment();
                    throttle.accept(msgTypeId, buffer, index, length);
                    break;
                case ResetFW.TYPE_ID:
                    counters.resets.increment();
                    throttle.accept(msgTypeId, buffer, index, length);
                    throttles.remove(streamId);
                    break;
                default:
                    break;
                }
            }
        }
    }

    private void handleReadReply(
        long routeId,
        long streamId,
        int msgTypeId,
        MutableDirectBuffer buffer,
        int index,
        int length)
    {
        if ((msgTypeId & 0x4000_0000) == 0)
        {
            final MessageConsumer handler = streams.get(streamId);
            if (handler != null)
            {
                final ReadCounters counters = countersByRouteId.computeIfAbsent(routeId, ReadCounters::new);
                switch (msgTypeId)
                {
                case BeginFW.TYPE_ID:
                    counters.opens.increment();
                    handler.accept(msgTypeId, buffer, index, length);
                    break;
                case DataFW.TYPE_ID:
                    counters.frames.increment();
                    counters.bytes.add(buffer.getInt(index + DataFW.FIELD_OFFSET_LENGTH));
                    handler.accept(msgTypeId, buffer, index, length);
                    break;
                case EndFW.TYPE_ID:
                    counters.closes.increment();
                    handler.accept(msgTypeId, buffer, index, length);
                    streams.remove(streamId);
                    break;
                case AbortFW.TYPE_ID:
                    counters.aborts.increment();
                    handler.accept(msgTypeId, buffer, index, length);
                    streams.remove(streamId);
                    break;
                default:
                    doReset(routeId, streamId);
                    break;
                }
            }
            else if (msgTypeId == BeginFW.TYPE_ID)
            {
                final MessageConsumer newHandler = handleBegin(msgTypeId, buffer, index, length);
                if (newHandler != null)
                {
                    final ReadCounters counters = countersByRouteId.computeIfAbsent(routeId, ReadCounters::new);
                    counters.opens.increment();
                    newHandler.accept(msgTypeId, buffer, index, length);
                }
                else
                {
                    doReset(routeId, streamId);
                }
            }
        }
        else
        {
            final MessageConsumer throttle = throttles.get(streamId);
            if (throttle != null)
            {
                switch (msgTypeId)
                {
                case WindowFW.TYPE_ID:
                    throttle.accept(msgTypeId, buffer, index, length);
                    break;
                case ResetFW.TYPE_ID:
                    throttle.accept(msgTypeId, buffer, index, length);
                    throttles.remove(streamId);
                    break;
                default:
                    break;
                }
            }
        }
    }

    private MessageConsumer handleBegin(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        final BeginFW begin = beginRO.wrap(buffer, index, index + length);
        final long routeId = begin.routeId();
        final long streamId = begin.streamId();
        final Role role = resolveRole(routeId, streamId);

        StreamFactory streamFactory = streamFactories.get(role);
        MessageConsumer newStream = null;
        if (streamFactory != null)
        {
            final MessageConsumer replyTo = supplyReplyTo(routeId, streamId);
            newStream = streamFactory.newStream(msgTypeId, buffer, index, length, replyTo);
            if (newStream != null)
            {
                streams.put(streamId, newStream);
            }
        }

        return newStream;
    }

    private void doReset(
        final long routeId,
        final long streamId)
    {
        final ResetFW reset = resetRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(streamId)
                .build();

        final MessageConsumer replyTo = supplyReplyTo(routeId, streamId);
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
                                     .build();

        stream.accept(abort.typeId(), abort.buffer(), abort.offset(), abort.sizeof());
    }

    final class ReadCounters
    {
        private final AtomicCounter opens;
        private final AtomicCounter closes;
        private final AtomicCounter aborts;
        private final AtomicCounter windows;
        private final AtomicCounter resets;
        private final AtomicCounter bytes;
        private final AtomicCounter frames;

        ReadCounters(
            long routeId)
        {
            this.opens = counters.counter(format("%d.opens.read", routeId));
            this.closes = counters.counter(format("%d.closes.read", routeId));
            this.aborts = counters.counter(format("%d.aborts.read", routeId));
            this.windows = counters.counter(format("%d.windows.read", routeId));
            this.resets = counters.counter(format("%d.resets.read", routeId));
            this.bytes = counters.counter(format("%d.bytes.read", routeId));
            this.frames = counters.counter(format("%d.frames.read", routeId));
        }
    }
}
