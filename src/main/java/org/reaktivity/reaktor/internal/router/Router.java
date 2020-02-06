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
package org.reaktivity.reaktor.internal.router;

import static org.reaktivity.reaktor.internal.router.RouteId.routeId;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.function.ToIntFunction;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.reaktivity.nukleus.function.MessagePredicate;
import org.reaktivity.nukleus.route.RouteKind;
import org.reaktivity.reaktor.ReaktorConfiguration;
import org.reaktivity.reaktor.internal.LabelManager;
import org.reaktivity.reaktor.internal.layouts.RoutesLayout;
import org.reaktivity.reaktor.internal.types.OctetsFW;
import org.reaktivity.reaktor.internal.types.control.Role;
import org.reaktivity.reaktor.internal.types.control.RouteFW;
import org.reaktivity.reaktor.internal.types.control.UnrouteFW;
import org.reaktivity.reaktor.internal.types.state.RouteTableFW;

public final class Router
{
    private final RoutesLayout.Builder routesRW = new RoutesLayout.Builder();

    private final RouteFW routeRO = new RouteFW();
    private final RouteTableFW routeTableRO = new RouteTableFW();

    private final RouteFW.Builder routeRW = new RouteFW.Builder();
    private final RouteTableFW.Builder routeTableRW = new RouteTableFW.Builder();

    private final ToIntFunction<String> supplyLabelId;
    private final MutableDirectBuffer routeBuf;

    private final RoutesLayout routes;

    private final Map<String, RouteKind> localAddressKinds;

    private volatile DirectBuffer readonlyRoutesBuffer;
    private int conditionId;

    public Router(
        ReaktorConfiguration config,
        LabelManager labels,
        int maxControlCommandLength)
    {
        this.supplyLabelId = labels::supplyLabelId;
        this.localAddressKinds = new HashMap<>();

        this.routeBuf = new UnsafeBuffer(ByteBuffer.allocateDirect(maxControlCommandLength));

        this.routes = this.routesRW.routesPath(config.directory().resolve("routes"))
            .routesBufferCapacity(config.routesBufferCapacity())
            .readonly(false)
            .build();

        this.readonlyRoutesBuffer = newCopyOfRoutesBuffer();
    }

    public DirectBuffer readonlyRoutesBuffer()
    {
        return readonlyRoutesBuffer;
    }

    public boolean doRoute(
        RouteFW route,
        MessagePredicate routeHandler)
    {
        final RouteTableFW routeTable = routeTableRO.wrap(readonlyRoutesBuffer, 0, readonlyRoutesBuffer.capacity());

        final boolean routed = routeHandler.test(route.typeId(), route.buffer(), route.offset(), route.sizeof());

        if (routed)
        {
            final int modCount = routeTable.modificationCount();

            final MutableDirectBuffer writeableRoutesBuffer = newCopyOfRoutesBuffer();
            routeTableRW
                .wrap(writeableRoutesBuffer, 0, writeableRoutesBuffer.capacity())
                .modificationCount(modCount)
                .entries(es ->
                {
                    routeTable.entries().forEach(old -> es.item(e -> e.route(old.route())));
                    es.item(e -> e.route(route.buffer(), route.offset(), route.sizeof()));
                })
                .build();

            routes.routesBuffer().putBytes(0, writeableRoutesBuffer, 0, writeableRoutesBuffer.capacity());
            routes.routesBuffer().addIntOrdered(RouteTableFW.FIELD_OFFSET_MODIFICATION_COUNT, 1);

            this.readonlyRoutesBuffer = writeableRoutesBuffer;
        }

        return routed;
    }

    public boolean doUnroute(
        UnrouteFW unroute,
        MessagePredicate routeHandler)
    {
        final RouteTableFW routeTable = routeTableRO.wrap(readonlyRoutesBuffer, 0, readonlyRoutesBuffer.capacity());

        final int modCount = routeTable.modificationCount();

        int beforeSize = routeTable.sizeof();

        final MutableDirectBuffer writeableRoutesBuffer = newCopyOfRoutesBuffer();

        RouteTableFW newRouteTable = routeTableRW.wrap(writeableRoutesBuffer, 0, writeableRoutesBuffer.capacity())
            .modificationCount(modCount)
            .entries(res ->
            {
                routeTable.entries().forEach(old ->
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

        this.readonlyRoutesBuffer = writeableRoutesBuffer;

        routes.routesBuffer().putBytes(0, writeableRoutesBuffer, 0, writeableRoutesBuffer.capacity());
        routes.routesBuffer().addIntOrdered(RouteTableFW.FIELD_OFFSET_MODIFICATION_COUNT, 1);

        return beforeSize > afterSize;
    }

    public RouteFW generateRouteId(
        RouteFW route)
    {
        final Role role = route.role().get();
        final String nukleus = route.nukleus().asString();
        final String localAddress = route.localAddress().asString();
        final String remoteAddress = route.remoteAddress().asString();
        final long authorization = route.authorization();
        final OctetsFW extension = route.extension();

        final RouteKind routeKind = RouteKind.valueOf(role.ordinal());
        final RouteKind existingRouteKind = localAddressKinds.putIfAbsent(localAddress, routeKind);
        if (existingRouteKind != null && existingRouteKind != routeKind)
        {
            // TODO: pin localAddress to both route kind and nukleus
            throw new IllegalArgumentException("localAddress " + localAddress + " reused with different Role");
        }

        final int localId = supplyLabelId.applyAsInt(localAddress);
        final int remoteId = supplyLabelId.applyAsInt(remoteAddress);
        final long newRouteId = routeId(localId, remoteId, role, ++conditionId);

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

    private MutableDirectBuffer newCopyOfRoutesBuffer()
    {
        final DirectBuffer oldRoutesBuffer = routes.routesBuffer();
        final MutableDirectBuffer newRoutesBuffer = new UnsafeBuffer(new byte[oldRoutesBuffer.capacity()]);
        newRoutesBuffer.putBytes(0, oldRoutesBuffer, 0, oldRoutesBuffer.capacity());
        return newRoutesBuffer;
    }
}
