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

import static org.reaktivity.reaktor.internal.router.RouteId.localId;
import static org.reaktivity.reaktor.internal.router.RouteId.remoteId;

import java.util.function.LongFunction;
import java.util.function.Supplier;

import org.agrona.DirectBuffer;
import org.agrona.collections.Long2ObjectHashMap;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.function.MessageFunction;
import org.reaktivity.nukleus.function.MessagePredicate;
import org.reaktivity.nukleus.route.RouteManager;
import org.reaktivity.reaktor.internal.Counters;
import org.reaktivity.reaktor.internal.types.OctetsFW;
import org.reaktivity.reaktor.internal.types.control.RouteFW;
import org.reaktivity.reaktor.internal.types.state.RouteEntryFW;
import org.reaktivity.reaktor.internal.types.state.RouteTableFW;

public final class Resolver implements RouteManager
{
    private final ThreadLocal<RouteFW> routeRO = ThreadLocal.withInitial(RouteFW::new);
    private final RouteTableFW routeTableRO = new RouteTableFW();

    private final Counters counters;
    private final Long2ObjectHashMap<MessageConsumer> throttles;
    private final Supplier<DirectBuffer> routesBufferRef;
    private final LongFunction<MessageConsumer> supplyReceiver;

    public Resolver(
        Counters counters,
        Supplier<DirectBuffer> routesBufferRef,
        Long2ObjectHashMap<MessageConsumer> throttles,
        LongFunction<MessageConsumer> supplyReceiver)
    {
        this.counters = counters;
        this.throttles = throttles;
        this.routesBufferRef = routesBufferRef;
        this.supplyReceiver = supplyReceiver;
    }

    public Counters counters()
    {
        return counters;
    }

    @Override
    public MessageConsumer supplyReceiver(
        long routeId)
    {
        return supplyReceiver.apply(routeId);
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
        final DirectBuffer routesBuffer = routesBufferRef.get();
        final RouteTableFW routeTable = routeTableRO.wrap(routesBuffer, 0, routesBuffer.capacity());

        final RouteEntryFW routeEntry = routeTable.entries().matchFirst(re ->
        {
            final OctetsFW entry = re.route();
            final RouteFW candidate = routeRO.get().wrap(entry.buffer(), entry.offset(), entry.limit());
            return (authorization & candidate.authorization()) == candidate.authorization() &&
                   filter.test(candidate.typeId(), candidate.buffer(), candidate.offset(), candidate.sizeof());
        });

        R result = null;
        if (routeEntry != null)
        {
            final OctetsFW entry = routeEntry.route();
            final RouteFW route = routeRO.get().wrap(entry.buffer(), entry.offset(), entry.limit());
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
        final DirectBuffer routesBuffer = routesBufferRef.get();
        RouteTableFW routeTable = routeTableRO.wrap(routesBuffer, 0, routesBuffer.capacity());

        RouteEntryFW routeEntry = routeTable.entries().matchFirst(re ->
        {
            final OctetsFW entry = re.route();
            final RouteFW candidate = routeRO.get().wrap(entry.buffer(), entry.offset(), entry.limit());
            return remoteId(routeId) == localId(candidate.correlationId()) &&
                   (authorization & candidate.authorization()) == candidate.authorization() &&
                   filter.test(candidate.typeId(), candidate.buffer(), candidate.offset(), candidate.sizeof());
        });

        R result = null;
        if (routeEntry != null)
        {
            final OctetsFW entry = routeEntry.route();
            final RouteFW route = routeRO.get().wrap(entry.buffer(), entry.offset(), entry.limit());
            result = mapper.apply(route.typeId(), route.buffer(), route.offset(), route.sizeof());
        }
        return result;
    }

    @Override
    public void forEach(
        MessageConsumer consumer)
    {
        final DirectBuffer routesBuffer = routesBufferRef.get();
        RouteTableFW routeTable = routeTableRO.wrap(routesBuffer, 0, routesBuffer.capacity());

        routeTable.entries().forEach(re ->
        {
            final OctetsFW entry = re.route();
            final RouteFW route = routeRO.get().wrap(entry.buffer(), entry.offset(), entry.limit());
            consumer.accept(route.typeId(), route.buffer(), route.offset(), route.sizeof());
        });
    }
}
