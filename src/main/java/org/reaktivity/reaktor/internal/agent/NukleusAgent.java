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

import static java.nio.ByteBuffer.allocateDirect;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import java.util.function.Supplier;

import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.LangUtil;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.MessageHandler;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.broadcast.BroadcastTransmitter;
import org.agrona.concurrent.ringbuffer.ManyToOneRingBuffer;
import org.agrona.concurrent.ringbuffer.RingBuffer;
import org.reaktivity.nukleus.AgentBuilder;
import org.reaktivity.nukleus.Nukleus;
import org.reaktivity.nukleus.function.CommandHandler;
import org.reaktivity.nukleus.function.MessagePredicate;
import org.reaktivity.nukleus.route.RouteKind;
import org.reaktivity.reaktor.ReaktorConfiguration;
import org.reaktivity.reaktor.internal.LabelManager;
import org.reaktivity.reaktor.internal.layouts.ControlLayout;
import org.reaktivity.reaktor.internal.router.Router;
import org.reaktivity.reaktor.internal.types.OctetsFW;
import org.reaktivity.reaktor.internal.types.control.CommandFW;
import org.reaktivity.reaktor.internal.types.control.ErrorFW;
import org.reaktivity.reaktor.internal.types.control.FreezeFW;
import org.reaktivity.reaktor.internal.types.control.FrozenFW;
import org.reaktivity.reaktor.internal.types.control.Role;
import org.reaktivity.reaktor.internal.types.control.RouteFW;
import org.reaktivity.reaktor.internal.types.control.RoutedFW;
import org.reaktivity.reaktor.internal.types.control.UnrouteFW;
import org.reaktivity.reaktor.internal.types.control.UnroutedFW;

public class NukleusAgent implements Agent
{
    private final CommandFW commandRO = new CommandFW();
    private final RouteFW routeRO = new RouteFW();
    private final UnrouteFW unrouteRO = new UnrouteFW();
    private final FreezeFW freezeRO = new FreezeFW();

    private final ErrorFW.Builder errorRW = new ErrorFW.Builder();
    private final RoutedFW.Builder routedRW = new RoutedFW.Builder();
    private final UnroutedFW.Builder unroutedRW = new UnroutedFW.Builder();
    private final FrozenFW.Builder frozenRW = new FrozenFW.Builder();

    private final ReaktorConfiguration config;
    private final Supplier<AgentBuilder> supplyAgentBuilder;
    private final LabelManager labels;
    private final List<ElektronAgent> elektronAgents;
    private final Map<String, Nukleus> nukleiByName;
    private final Router router;

    private final ControlLayout control;
    private final RingBuffer commandBuffer;
    private final BroadcastTransmitter responseBuffer;
    private final MutableDirectBuffer sendBuffer;
    private final MessageHandler commandHandler;

    public NukleusAgent(
        ReaktorConfiguration config,
        Supplier<AgentBuilder> supplyAgentBuilder)
    {
        this.config = config;
        this.supplyAgentBuilder = supplyAgentBuilder;
        this.labels = new LabelManager(config.directory());
        this.elektronAgents = new ArrayList<>();
        this.nukleiByName = new HashMap<>();

        this.control = new ControlLayout.Builder()
                .controlPath(config.directory().resolve("control"))
                .commandBufferCapacity(config.commandBufferCapacity())
                .responseBufferCapacity(config.responseBufferCapacity())
                .readonly(false)
                .build();

        this.commandBuffer = new ManyToOneRingBuffer(control.commandBuffer());
        this.responseBuffer = new BroadcastTransmitter(control.responseBuffer());
        this.sendBuffer = new UnsafeBuffer(allocateDirect(responseBuffer.maxMsgLength()));

        this.router = new Router(config, labels, commandBuffer.maxMsgLength());

        this.commandHandler = this::handleCommand;
    }

    @Override
    public String roleName()
    {
        return "reaktor/control";
    }

    public int doWork() throws Exception
    {
        return commandBuffer.read(commandHandler);
    }

    public void onClose()
    {
        CloseHelper.quietClose(control);
    }

    public Nukleus nukleus(
        String name)
    {
        return nukleiByName.get(name);
    }

    public <T extends Nukleus> T nukleus(
        Class<T> kind)
    {
        return nukleiByName.values()
            .stream()
            .filter(kind::isInstance)
            .map(kind::cast)
            .findFirst()
            .orElse(null);
    }

    public LabelManager labels()
    {
        return labels;
    }

    public ElektronAgent supplyElektronAgent(
        int index,
        int count,
        ExecutorService executor,
        Function<String, BitSet> affinityMask)
    {
        ElektronAgent newElektronAgent = new ElektronAgent(index, count, config, labels, executor, affinityMask,
                router::readonlyRoutesBuffer, supplyAgentBuilder);
        elektronAgents.add(newElektronAgent);
        return newElektronAgent;
    }

    public void assign(
        Nukleus nukleus)
    {
        nukleiByName.putIfAbsent(nukleus.name(), nukleus);
    }

    public void unassign(
        Nukleus nukleus)
    {
        nukleiByName.remove(nukleus.name());
    }

    public boolean isEmpty()
    {
        return nukleiByName.isEmpty();
    }

    Router router()
    {
        return router;
    }

    public void onRouteable(
        long routeId,
        Nukleus nukleus)
    {
        elektronAgents.forEach(a -> a.onRouteable(routeId, nukleus));
    }

    public void onRouted(
        Nukleus nukleus,
        RouteKind routeKind,
        long routeId,
        OctetsFW extension)
    {
        elektronAgents.forEach(a -> a.onRouted(nukleus, routeKind, routeId, extension));
    }

    void onUnrouted(
        Nukleus nukleus,
        RouteKind routeKind,
        long routeId)
    {
        elektronAgents.forEach(a -> a.onUnrouted(nukleus, routeKind, routeId));
    }


    private void doError(
        long correlationId)
    {
        ErrorFW error = errorRW.wrap(sendBuffer, 0, sendBuffer.capacity())
                .correlationId(correlationId)
                .build();

        responseBuffer.transmit(error.typeId(), error.buffer(), error.offset(), error.sizeof());
    }

    private void doRouted(
        long correlationId,
        long routeId)
    {
        RoutedFW routed = routedRW.wrap(sendBuffer, 0, sendBuffer.capacity())
                .correlationId(correlationId)
                .routeId(routeId)
                .build();

        responseBuffer.transmit(routed.typeId(), routed.buffer(), routed.offset(), routed.sizeof());
    }

    private void doUnrouted(
        long correlationId)
    {
        UnroutedFW unrouted = unroutedRW.wrap(sendBuffer, 0, sendBuffer.capacity())
                .correlationId(correlationId)
                .build();

        responseBuffer.transmit(unrouted.typeId(), unrouted.buffer(), unrouted.offset(), unrouted.sizeof());
    }

    private void doFrozen(
        long correlationId)
    {
        FrozenFW frozen = frozenRW.wrap(sendBuffer, 0, sendBuffer.capacity())
                .correlationId(correlationId)
                .build();

        responseBuffer.transmit(frozen.typeId(), frozen.buffer(), frozen.offset(), frozen.sizeof());
    }

    private void handleCommand(
        int msgTypeId,
        MutableDirectBuffer buffer,
        int index,
        int length)
    {
        switch (msgTypeId)
        {
        case RouteFW.TYPE_ID:
            final RouteFW route = routeRO.wrap(buffer, index, index + length);
            onRoute(route);
            break;
        case UnrouteFW.TYPE_ID:
            final UnrouteFW unroute = unrouteRO.wrap(buffer, index, index + length);
            onUnroute(unroute);
            break;
        case FreezeFW.TYPE_ID:
            final FreezeFW freeze = freezeRO.wrap(buffer, index, index + length);
            onFreeze(freeze);
            break;
        default:
            onUnrecognized(msgTypeId, buffer, index, length);
            break;
        }
    }

    private void onRoute(
        RouteFW route)
    {
        final long correlationId = route.correlationId();
        final String nukleusName = route.nukleus().asString();
        final Nukleus nukleus = nukleus(nukleusName);

        try
        {
            final Role role = route.role().get();
            final RouteKind routeKind = RouteKind.valueOf(role.ordinal());

            MessagePredicate routeHandler = nukleus.routeHandler(routeKind);
            if (routeHandler == null)
            {
                routeHandler = (t, b, i, l) -> true;
            }

            final RouteFW newRoute = router.generateRouteId(route);

            final long newRouteId = newRoute.correlationId();
            this.onRouteable(newRouteId, nukleus);

            if (router.doRoute(newRoute, routeHandler))
            {
                this.onRouted(nukleus, routeKind, newRouteId, route.extension());
                Thread.sleep(config.routedDelayMillis());
                doRouted(correlationId, newRouteId);
            }
            else
            {
                doError(correlationId);
            }
        }
        catch (Exception ex)
        {
            doError(correlationId);
            LangUtil.rethrowUnchecked(ex);
        }
    }

    private void onUnroute(
        UnrouteFW unroute)
    {
        final long correlationId = unroute.correlationId();
        final String nukleusName = unroute.nukleus().asString();
        final Nukleus nukleus = nukleus(nukleusName);

        try
        {
            final long routeId = unroute.routeId();
            final RouteKind routeKind = replyRouteKind(routeId);
            MessagePredicate routeHandler = nukleus.routeHandler(routeKind);
            if (routeHandler == null)
            {
                routeHandler = (t, b, i, l) -> true;
            }

            if (router.doUnroute(unroute, routeHandler))
            {
                this.onUnrouted(nukleus, routeKind, routeId);
                doUnrouted(correlationId);
            }
            else
            {
                doError(correlationId);
            }
        }
        catch (Exception ex)
        {
            doError(correlationId);
            LangUtil.rethrowUnchecked(ex);
        }
    }

    private void onUnrecognized(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        final CommandFW command = commandRO.wrap(buffer, index, index + length);
        final String nukleusName = command.nukleus().asString();
        final Nukleus nukleus = nukleus(nukleusName);

        CommandHandler handler = nukleus.commandHandler(msgTypeId);
        if (handler != null)
        {
            handler.handle(buffer, index, length, responseBuffer::transmit, sendBuffer);
        }
        else
        {
            doError(command.correlationId());
        }
    }

    private void onFreeze(
        FreezeFW freeze)
    {
        final long correlationId = freeze.correlationId();
        final String nukleusName = freeze.nukleus().asString();
        final Nukleus nukleus = nukleus(nukleusName);

        unassign(nukleus);
        doFrozen(correlationId);
    }

    private RouteKind replyRouteKind(
        long routeId)
    {
        return RouteKind.valueOf((int)(routeId >> 28) & 0x0f);
    }
}
