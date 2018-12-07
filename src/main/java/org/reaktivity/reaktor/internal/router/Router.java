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

import static org.agrona.CloseHelper.quietClose;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Predicate;

import org.agrona.LangUtil;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.AtomicCounter;
import org.reaktivity.nukleus.Nukleus;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.function.MessageFunction;
import org.reaktivity.nukleus.function.MessagePredicate;
import org.reaktivity.nukleus.route.RouteKind;
import org.reaktivity.nukleus.route.RouteManager;
import org.reaktivity.nukleus.stream.StreamFactoryBuilder;
import org.reaktivity.reaktor.internal.Context;
import org.reaktivity.reaktor.internal.State;
import org.reaktivity.reaktor.internal.conductor.Conductor;
import org.reaktivity.reaktor.internal.layouts.RoutesLayout;
import org.reaktivity.reaktor.internal.layouts.StreamsLayout;
import org.reaktivity.reaktor.internal.types.OctetsFW;
import org.reaktivity.reaktor.internal.types.StringFW;
import org.reaktivity.reaktor.internal.types.control.Role;
import org.reaktivity.reaktor.internal.types.control.RouteFW;
import org.reaktivity.reaktor.internal.types.control.UnrouteFW;
import org.reaktivity.reaktor.internal.types.state.RouteEntryFW;
import org.reaktivity.reaktor.internal.types.state.RouteTableFW;

public final class Router extends Nukleus.Composite implements RouteManager
{
    private final RouteFW routeRO = new RouteFW();
    private final RouteTableFW routeTableRO = new RouteTableFW();

    private final RouteFW.Builder routeRW = new RouteFW.Builder();
    private final RouteTableFW.Builder routeTableRW = new RouteTableFW.Builder();

    private final Context context;
    private final MutableDirectBuffer writeBuffer;
    private final Map<String, Source> sourcesByName;
    private final Map<String, Target> targetsByName;
    private final AtomicCounter routeRefs;
    private final MutableDirectBuffer routeBuf;
    private final AtomicLong correlations;
    private final GroupBudgetManager groupBudgetManager;

    private final RoutesLayout routesLayout;
    private final MutableDirectBuffer routesBuffer;
    private final int routesBufferCapacity;

    private final Long2ObjectHashMap<MessageConsumer> streams;
    private final Long2ObjectHashMap<MessageConsumer> throttles;

    private Conductor conductor;
    private State state;
    private Function<RouteKind, StreamFactoryBuilder> supplyStreamFactoryBuilder;
    private boolean timestamps;
    private Function<Role, MessagePredicate> supplyRouteHandler;
    private Predicate<RouteKind> allowZeroSourceRef;
    private Predicate<RouteKind> allowZeroTargetRef;
    private Predicate<RouteKind> layoutSource;
    private Predicate<RouteKind> layoutTarget;

    public Router(
        Context context)
    {
        this.context = context;
        this.writeBuffer = new UnsafeBuffer(new byte[context.maxMessageLength()]);
        this.routeRefs = context.counters().routes();
        this.sourcesByName = new HashMap<>();
        this.targetsByName = new HashMap<>();
        this.routeBuf = new UnsafeBuffer(ByteBuffer.allocateDirect(context.maxControlCommandLength()));
        this.correlations  = new AtomicLong();
        this.groupBudgetManager = new GroupBudgetManager();
        this.routesLayout = context.routesLayout();
        this.routesBuffer = routesLayout.routesBuffer();
        this.routesBufferCapacity = routesLayout.capacity();
        this.streams = new Long2ObjectHashMap<>();
        this.throttles = new Long2ObjectHashMap<>();

    }

    public void setConductor(
        Conductor conductor)
    {
        this.conductor = conductor;
    }

    public void setState(
        State state)
    {
        this.state = state;
    }

    public void setStreamFactoryBuilderSupplier(
        Function<RouteKind, StreamFactoryBuilder> supplyStreamFactoryBuilder)
    {
        this.supplyStreamFactoryBuilder = supplyStreamFactoryBuilder;
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

    public void setAllowZeroSourceRef(
        Predicate<RouteKind> allowZeroSourceRef)
    {
        this.allowZeroSourceRef = allowZeroSourceRef;
    }

    public void setAllowZeroTargetRef(
        Predicate<RouteKind> allowZeroTargetRef)
    {
        this.allowZeroTargetRef = allowZeroTargetRef;
    }

    public void setLayoutSource(
        Predicate<RouteKind> layoutSource)
    {
        this.layoutSource = layoutSource;
    }

    public void setLayoutTarget(
        Predicate<RouteKind> layoutTarget)
    {
        this.layoutTarget = layoutTarget;
    }

    @Override
    public String name()
    {
        return "router";
    }

    @Override
    public MessageConsumer supplyTarget(
        String targetName)
    {
        return supplyTargetInternal(targetName).writeHandler();
    }

    @Override
    public void setThrottle(
        String targetName,
        long streamId,
        MessageConsumer throttle)
    {
        supplyTargetInternal(targetName).setThrottle(streamId, throttle);
    }

    public void doRoute(
        RouteFW route)
    {
        try
        {
            Role role = route.role().get();
            MessagePredicate routeHandler = supplyRouteHandler.apply(role);

            final boolean requireNonZeroSourceRef = !allowZeroSourceRef.test(RouteKind.valueOf(role.ordinal()));
            final boolean requireNonZeroTargetRef = !allowZeroTargetRef.test(RouteKind.valueOf(role.ordinal()));

            if (requireNonZeroSourceRef || requireNonZeroTargetRef)
            {
                route = generateSourceRefIfNecessary(route, requireNonZeroSourceRef);
                route = generateTargetRefIfNecessary(route, requireNonZeroTargetRef);

                final long sourceRef = route.sourceRef();
                MessagePredicate defaultHandler = (t, b, i, l) -> ReferenceKind.resolve(sourceRef).ordinal() == role.ordinal();
                if (routeHandler == null)
                {
                    routeHandler = defaultHandler;
                }
                else
                {
                    routeHandler = defaultHandler.and(routeHandler);
                }
            }

            if (routeHandler == null)
            {
                routeHandler = (t, b, i, l) -> true;
            }

            if (doRouteInternal(route, routeHandler))
            {
                conductor.onRouted(route.correlationId(), route.sourceRef(), route.targetRef());
            }
            else
            {
                conductor.onError(route.correlationId());
            }
        }
        catch (Exception ex)
        {
            conductor.onError(route.correlationId());
            LangUtil.rethrowUnchecked(ex);
        }
    }

    public void doUnroute(
        UnrouteFW unroute)
    {
        final long correlationId = unroute.correlationId();

        try
        {
            Role role = unroute.role().get();
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

    @Override
    public <R> R resolve(
        final long authorization,
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

            final Role role = route.role().get();
            final RouteKind kind = ReferenceKind.sourceKind(role).toRouteKind();

            if (layoutSource.test(kind))
            {
                String sourceName = route.source().asString();
                supplySource(sourceName);
            }

            if (layoutTarget.test(kind))
            {
                String targetName = route.target().asString();
                supplySource(targetName);
            }

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
                    if (!routeMatchesUnroute(routeHandler, route, unroute))
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

    @Override
    public void close() throws Exception
    {
        sourcesByName.forEach((k, v) -> v.detach());
        targetsByName.forEach((k, v) -> v.detach());

        targetsByName.forEach((k, v) -> quietClose(v));
        super.close();
    }

    private Target supplyTargetInternal(
        String targetName)
    {
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

        return new Target(targetName, layout, writeBuffer, timestamps,
                          context.maximumMessagesPerRead(), streams, throttles);
    }

    private Source supplySource(
        String sourceName)
    {
        return sourcesByName.computeIfAbsent(sourceName, this::newSource);
    }

    private Source newSource(
        String sourceName)
    {
        return include(new Source(
                context,
                writeBuffer,
                this,
                sourceName,
                state,
                groupBudgetManager::claim,
                groupBudgetManager::release,
                supplyStreamFactoryBuilder,
                correlations,
                this::supplyTarget,
                streams,
                throttles));
    }

    private RouteFW generateSourceRefIfNecessary(
        RouteFW route,
        boolean requireNonZeroSourceRef)
    {
        if (requireNonZeroSourceRef && route.sourceRef() == 0L)
        {
            final Role role = route.role().get();
            final ReferenceKind routeKind = ReferenceKind.sourceKind(role);
            final long newSourceRef = routeKind.nextRef(routeRefs);
            final StringFW source = route.source();
            final StringFW target = route.target();
            final long targetRef = route.targetRef();
            final long authorization = route.authorization();
            final OctetsFW extension = route.extension();

            route = routeRW.wrap(routeBuf, 0, routeBuf.capacity())
                           .correlationId(route.correlationId())
                           .role(b -> b.set(role))
                           .source(source)
                           .sourceRef(newSourceRef)
                           .target(target)
                           .targetRef(targetRef)
                           .authorization(authorization)
                           .extension(b -> b.set(extension))
                           .build();
        }

        return route;
    }

    private RouteFW generateTargetRefIfNecessary(
        RouteFW route,
        boolean requireNonZeroTargetRef)
    {
        if (requireNonZeroTargetRef && route.targetRef() == 0L)
        {
            final Role role = route.role().get();
            final ReferenceKind routeKind = ReferenceKind.targetKind(role);
            final long sourceRef = route.sourceRef();
            final StringFW source = route.source();
            final StringFW target = route.target();
            final long newTargetRef = routeKind.nextRef(routeRefs);
            final long authorization = route.authorization();
            final OctetsFW extension = route.extension();

            route = routeRW.wrap(routeBuf, 0, routeBuf.capacity())
                           .correlationId(route.correlationId())
                           .role(b -> b.set(role))
                           .source(source)
                           .sourceRef(sourceRef)
                           .target(target)
                           .targetRef(newTargetRef)
                           .authorization(authorization)
                           .extension(b -> b.set(extension))
                           .build();
        }

        return route;
    }

    private static boolean routeMatchesUnroute(
        MessagePredicate routeHandler,
        RouteFW route,
        UnrouteFW unroute)
    {
        return route.role().get() == unroute.role().get() &&
        unroute.source().asString().equals(route.source().asString()) &&
        unroute.sourceRef() == route.sourceRef() &&
        unroute.target().asString().equals(route.target().asString()) &&
        unroute.targetRef() == route.targetRef() &&
        unroute.authorization() == route.authorization() &&
        unroute.extension().equals(route. extension()) &&
        routeHandler.test(UnrouteFW.TYPE_ID, route.buffer(), route.offset(), route.sizeof());
    }
}
