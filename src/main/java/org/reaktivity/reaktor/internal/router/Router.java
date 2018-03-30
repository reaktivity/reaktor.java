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

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.function.MessageFunction;
import org.reaktivity.nukleus.function.MessagePredicate;
import org.reaktivity.reaktor.internal.Context;
import org.reaktivity.reaktor.internal.layouts.RoutesLayout;
import org.reaktivity.reaktor.internal.types.ListFW;
import org.reaktivity.reaktor.internal.types.OctetsFW;
import org.reaktivity.reaktor.internal.types.control.RouteFW;
import org.reaktivity.reaktor.internal.types.control.UnrouteFW;
import org.reaktivity.reaktor.internal.types.state.RouteEntryFW;
import org.reaktivity.reaktor.internal.types.state.RouteTableFW;

public final class Router
{
    private final RoutesLayout routesLayout;
    private final RouteTableFW.Builder routeTableRW;
    private final RouteTableFW routeTableRO;
    private final MutableDirectBuffer routesBuffer;
    private final int routesBufferCapacity;
    private final RouteFW routeRO;

    public Router(
        Context context)
    {
        this.routesLayout = context.routesLayout();
        this.routeTableRW = new RouteTableFW.Builder();
        this.routeTableRO = new RouteTableFW();
        this.routeRO = new RouteFW();
        this.routesBuffer = routesLayout.routesBuffer();
        this.routesBufferCapacity = routesLayout.capacity();
    }

    public boolean doRoute(
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
            routesLayout.unlock();
        }

        return routed;
    }

    public boolean doUnroute(
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

    Object r;
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

    public void foreach(
        MessageConsumer consumer)
    {
        RouteTableFW routeTable = routeTableRO.wrap(routesBuffer, 0, routesBufferCapacity);
        routeTable.routeEntries().forEach(re ->
        {
            final RouteFW route = wrapRoute(re, routeRO);
            consumer.accept(route.typeId(), route.buffer(), route.offset(), route.sizeof());
        });
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
