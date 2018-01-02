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

import java.util.function.Consumer;

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

    int cnt = 0;
    public boolean doRoute(
        RouteFW route,
        MessagePredicate routeHandler)
    {
        RouteTableFW routeTable = routeTableRO.wrap(routesBuffer, 0, routesBufferCapacity);

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
                            System.out.println("hmm: " + e);
                            b.item(copyRouteEntry(e.routeSize(), e.route()));
                        }
                    );

                    b.item(
                        routeEntryBuilder -> routeEntryBuilder.routeSize(route.sizeof())
                                                              .route(routeBuilder -> copyRoute(routeBuilder, route))
                    );
                });
            RouteTableFW what = routeTableRW.build();
            System.out.println("before");
            final ListFW<RouteEntryFW> routeEntries = what.routeEntries();
            routeEntries.forEach(re -> System.out.println(re));
            System.out.println("after");
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
            routeTable.routeEntries().anyMatch(r -> routeMatchesUnroute(r.route(), unroute));

        if (unrouted)
        {
            routesLayout.lock();

            routeTableRO.wrap(routesBuffer, 0, routesBufferCapacity);
            routeTableRW.wrap(routesBuffer, 0, routesBufferCapacity);

            routeTableRW.routeEntries(b ->
            {
                routeTableRO.routeEntries().forEach(
                    e -> b.item(copyRouteEntry(e.routeSize(), e.route()))
                );
            });

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
        // anyMatch matchFirst firstMatch(
        r = null;
        RouteTableFW routeTable = routeTableRO.wrap(routesBuffer, 0, routesBufferCapacity);
        routeTable.routeEntries().forEach(re ->
        {
            final RouteFW candidate = re.route();
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
        });

        return r == null ? null : (R) r;
    }

    private static Consumer<RouteEntryFW.Builder> copyRouteEntry(
        int size,
        RouteFW route)
    {
        return routeEntryBuilder ->
        {
            routeEntryBuilder.routeSize(size);
            routeEntryBuilder.route(routeBuilder -> copyRoute(routeBuilder, route));
        };
    }

    private static void copyRoute(
        RouteFW.Builder b,
        RouteFW route)
    {
        final OctetsFW extension = route.extension();

        b.correlationId(route.correlationId())
         .role(rb -> rb.set(route.role()))
         .source(route.source())
         .sourceRef(route.sourceRef())
         .target(route.target())
         .targetRef(route.targetRef())
         .authorization(route.authorization())
         .extension(
             extension.buffer(),
             extension.offset(),
             extension.limit());
    }

    private static boolean routeMatchesUnroute(
        RouteFW route,
        UnrouteFW unroute)
    {
        return route.role() == unroute.role() &&
        unroute.source().equals(route.source()) &&
        unroute.sourceRef() == route.sourceRef() &&
        unroute.target().equals(route.target()) &&
        unroute.targetRef() == route.targetRef() &&
        unroute.authorization() == route.authorization();
    }
}
