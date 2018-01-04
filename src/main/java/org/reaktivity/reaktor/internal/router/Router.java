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
import org.reaktivity.nukleus.Nukleus;
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

public final class Router extends Nukleus.Composite
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

    @Override
    public String name()
    {
        return "router";
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
        final boolean unrouted =
            routeHandler.test(unroute.typeId(), unroute.buffer(), unroute.offset(), unroute.sizeof()) &&
            routeTable.routeEntries().anyMatch(r -> routeMatchesUnroute(wrapRoute(r, routeRO), unroute));

        if (unrouted)
        {
            int readLock = routesLayout.lock();

            routeTableRO.wrap(routesBuffer, 0, routesBufferCapacity);
            routeTableRW.wrap(routesBuffer, 0, routesBufferCapacity);

            routeTableRW.writeLockAcquires(readLock)
                        .writeLockReleases(readLock - 1)
                        .routeEntries(b ->
                        {
                            routeTableRO.routeEntries().forEach(e ->
                            {
                                RouteFW route = wrapRoute(e, routeRO);
                                if (!routeMatchesUnroute(route, unroute))
                                {
                                    b.item(ob ->  ob.route(route.buffer(), route.offset(), route.sizeof()));
                                }
                            });
                        });

            routeTableRW.build();
            routesLayout.unlock();
        }
        return unrouted;
    }

    Object r;
    public <R> R resolve(
        final long authorization,
        MessagePredicate filter,
        MessageFunction<R> mapper)
    {
        // TODO do matchFirst when supported by ListFW
        r = null;
        RouteTableFW routeTable = routeTableRO.wrap(routesBuffer, 0, routesBufferCapacity);
        routeTable.routeEntries().forEach(re ->
        {
            if (r == null)
            {
                final RouteFW candidate = wrapRoute(re, routeRO);
                final int typeId = candidate.typeId();
                final DirectBuffer buffer = candidate.buffer();
                final int offset = candidate.offset();
                final int length = candidate.sizeof();
                final long routeAuthorization = candidate.authorization();

                if (filter.test(typeId, buffer, offset, length) &&
                   (authorization & routeAuthorization) == routeAuthorization)
                {
                    r = mapper.apply(typeId, buffer, offset, length);
                }
            }
        });

        System.out.println(authorization + "  " + r);
        return r == null ? null : (R) r;
    }


    private static boolean routeMatchesUnroute(
        RouteFW route,
        UnrouteFW unroute)
    {
        return route.role().get() == unroute.role().get() &&
        unroute.source().asString().equals(route.source().asString()) &&
        unroute.sourceRef() == route.sourceRef() &&
        unroute.target().asString().equals(route.target().asString()) &&
        unroute.targetRef() == route.targetRef() &&
        unroute.authorization() == route.authorization();
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
