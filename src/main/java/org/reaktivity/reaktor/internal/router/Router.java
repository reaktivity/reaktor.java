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
package org.reaktivity.reaktor.internal.router;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Predicate;

import org.agrona.DirectBuffer;
import org.agrona.LangUtil;
import org.agrona.MutableDirectBuffer;
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
import org.reaktivity.reaktor.internal.types.ListFW;
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

    private Conductor conductor;
    private State state;
    private Function<RouteKind, StreamFactoryBuilder> supplyStreamFactoryBuilder;
    private boolean timestamps;
    private Function<Role, MessagePredicate> supplyRouteHandler;
    private Predicate<RouteKind> allowZeroRouteRef;
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

    public void setAllowZeroRouteRef(
        Predicate<RouteKind> allowZeroRouteRef)
    {
        this.allowZeroRouteRef = allowZeroRouteRef;
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

            if (!allowZeroRouteRef.test(RouteKind.valueOf(role.ordinal())))
            {
                route = generateSourceRefIfNecessary(route);
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
                conductor.onRouted(route.correlationId(), route.sourceRef());
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
        RouteEntryFW routeEntry = routeTable.routeEntries().matchFirst(re ->
        {
            final RouteFW candidate = wrapRoute(re, routeRO);
            final int typeId = candidate.typeId();
            final DirectBuffer buffer = candidate.buffer();
            final int offset = candidate.offset();
            final int length = candidate.sizeof();
            final long routeAuthorization = candidate.authorization();
            return filter.test(typeId, buffer, offset, offset + length) &&
            (authorization & routeAuthorization) == routeAuthorization;
        });

        R result = null;
        if (routeEntry != null)
        {
            final RouteFW route = wrapRoute(routeEntry, routeRO);
            result = mapper.apply(route.typeId(), route.buffer(), route.offset(), route.sizeof());
        }
        return result;
    }

    @Override
    public void forEach(
        MessageConsumer consumer)
    {
        RouteTableFW routeTable = routeTableRO.wrap(routesBuffer, 0, routesBufferCapacity);
        routeTable.routeEntries().forEach(re ->
        {
            final RouteFW route = wrapRoute(re, routeRO);
            consumer.accept(route.typeId(), route.buffer(), route.offset(), route.sizeof());
        });
    }

    private boolean doRouteInternal(
        RouteFW route,
        MessagePredicate routeHandler)
    {
        final RouteTableFW routeTable = routeTableRO.wrap(routesBuffer, 0, routesBufferCapacity);

        final boolean routed = routeHandler.test(route.typeId(), route.buffer(), route.offset(), route.sizeof());

        if (routed)
        {
            int readLock = routesLayout.lock();

            routeTableRW
                .wrap(routesBuffer, 0, routesBufferCapacity)
                .writeLockAcquires(readLock)
                .writeLockReleases(readLock - 1)
                .routeEntries(b ->
                {
                    final ListFW<RouteEntryFW> existingEntries = routeTable.routeEntries();

                    existingEntries.forEach(
                        e ->
                        {
                            final OctetsFW er = e.route();
                            b.item(b2 -> b2.route(er.buffer(), er.offset(), er.sizeof()));
                        }
                    );
                    b.item(b2 -> b2.route(route.buffer(), route.offset(), route.sizeof()));
                });
            routeTableRW.build();

            final Role role = route.role().get();
            final RouteKind kind = ReferenceKind.valueOf(role).toRouteKind();

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

        int readLock = routesLayout.lock();

        routeTable.wrap(routesBuffer, 0, routesBufferCapacity);
        int beforeSize = routeTableRO.routeEntries().sizeof();
        routeTableRW.wrap(routesBuffer, 0, routesBufferCapacity);

        routeTableRW.writeLockAcquires(readLock)
            .writeLockReleases(readLock - 1)
            .routeEntries(b ->
            {
                routeTableRO.routeEntries().forEach(e ->
                {
                    RouteFW route = wrapRoute(e, routeRO);
                    if (!routeMatchesUnroute(routeHandler, route, unroute))
                    {
                        b.item(ob ->  ob.route(route.buffer(), route.offset(), route.sizeof()));
                    }
                });
            });

        int afterSize = routeTableRW.build().sizeof();
        routesLayout.unlock();
        return beforeSize > afterSize;
    }

    @Override
    public void close() throws Exception
    {
        sourcesByName.forEach((k, v) -> v.detach());
        targetsByName.forEach((k, v) -> v.detach());

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
                .throttleCapacity(context.throttleBufferCapacity())
                .readonly(true)
                .build();

        return include(new Target(context.name(), targetName, layout, writeBuffer, timestamps));
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
                timestamps,
                correlations));
    }

    private RouteFW generateSourceRefIfNecessary(
        RouteFW route)
    {
        if (route.sourceRef() == 0L)
        {
            final Role role = route.role().get();
            final ReferenceKind routeKind = ReferenceKind.valueOf(role);
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
        routeHandler.test(UnrouteFW.TYPE_ID, route.buffer(), route.offset(), route.limit());
    }

    private static RouteFW wrapRoute(
        RouteEntryFW r,
        RouteFW routeRO)
    {
        final OctetsFW route = r.route();
        final DirectBuffer buffer = route.buffer();
        final int offset = route.offset();
        final int routeSize = (int) r.routeSize();
        return routeRO.wrap(buffer, offset, offset + routeSize);
    }
}
