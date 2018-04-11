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
package org.reaktivity.reaktor.internal.acceptor;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Predicate;

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
import org.reaktivity.reaktor.internal.acceptable.Acceptable;
import org.reaktivity.reaktor.internal.acceptable.Target;
import org.reaktivity.reaktor.internal.conductor.Conductor;
import org.reaktivity.reaktor.internal.layouts.StreamsLayout;
import org.reaktivity.reaktor.internal.router.ReferenceKind;
import org.reaktivity.reaktor.internal.router.Router;
import org.reaktivity.reaktor.internal.types.OctetsFW;
import org.reaktivity.reaktor.internal.types.StringFW;
import org.reaktivity.reaktor.internal.types.control.Role;
import org.reaktivity.reaktor.internal.types.control.RouteFW;
import org.reaktivity.reaktor.internal.types.control.UnrouteFW;
import org.reaktivity.reaktor.internal.types.stream.ResetFW;

public final class Acceptor extends Nukleus.Composite implements RouteManager
{
    private final RouteFW.Builder routeRW = new RouteFW.Builder();
    private final ResetFW.Builder resetRW = new ResetFW.Builder();

    private final Context context;
    private final MutableDirectBuffer writeBuffer;
    private final Map<String, Acceptable> acceptables;
    private final Map<String, Target> targetsByName;
    private final AtomicCounter routeRefs;
    private final MutableDirectBuffer routeBuf;
    private final GroupBudgetManager groupBudgetManager;

    private Conductor conductor;
    private Router router;
    private State state;
    private Function<RouteKind, StreamFactoryBuilder> supplyStreamFactoryBuilder;
    private boolean timestamps;
    private Function<Role, MessagePredicate> supplyRouteHandler;
    private Predicate<RouteKind> allowZeroRouteRef;
    private AtomicLong correlations;

    public Acceptor(
        Context context)
    {
        this.context = context;
        this.writeBuffer = new UnsafeBuffer(new byte[context.maxMessageLength()]);
        this.routeRefs = context.counters().routes();
        this.acceptables = new HashMap<>();
        this.targetsByName = new HashMap<>();
        this.routeBuf = new UnsafeBuffer(ByteBuffer.allocateDirect(context.maxControlCommandLength()));
        this.correlations  = new AtomicLong();
        this.groupBudgetManager = new GroupBudgetManager();
    }

    public void setConductor(
        Conductor conductor)
    {
        this.conductor = conductor;
    }

    public void setRouter(
        Router router)
    {
        this.router = router;
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

    @Override
    public String name()
    {
        return "acceptor";
    }

    @Override
    public void close() throws Exception
    {
        targetsByName.forEach(this::doAbort);
        targetsByName.forEach(this::doReset);

        super.close();
    }

    @Override
    public <R> R resolve(
        long authorization,
        MessagePredicate filter,
        MessageFunction<R> mapper)
    {
        return router.resolve(authorization, filter, mapper);
    }

    @Override
    public void forEach(
        MessageConsumer consumer)
    {
        router.forEach(consumer);
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

        return include(new Target(targetName, layout, timestamps));
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

            if (router.doRoute(route, routeHandler))
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

            if (router.doUnroute(unroute, routeHandler))
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

    public Acceptable supplyAcceptable(
        String sourceName)
    {
        return acceptables.computeIfAbsent(sourceName, this::newAcceptable);
    }

    private Acceptable newAcceptable(
        String sourceName)
    {
        return include(new Acceptable(
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

    private void doAbort(
        String targetName,
        Target target)
    {
        target.abort();
    }

    private void doReset(
        String targetName,
        Target target)
    {
        target.reset(this::doReset);
    }

    private void doReset(
        long throttleId,
        MessageConsumer throttle)
    {
        final ResetFW reset = resetRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                                     .streamId(throttleId)
                                     .build();

        throttle.accept(reset.typeId(), reset.buffer(), reset.offset(), reset.sizeof());
    }
}
