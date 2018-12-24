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
package org.reaktivity.reaktor.internal;

import static java.lang.String.format;

import java.util.EnumMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;

import org.agrona.collections.Int2ObjectHashMap;
import org.reaktivity.nukleus.Configuration;
import org.reaktivity.nukleus.Nukleus;
import org.reaktivity.nukleus.NukleusBuilder;
import org.reaktivity.nukleus.function.CommandHandler;
import org.reaktivity.nukleus.function.MessagePredicate;
import org.reaktivity.nukleus.route.RouteKind;
import org.reaktivity.nukleus.stream.StreamFactoryBuilder;
import org.reaktivity.reaktor.internal.conductor.Conductor;
import org.reaktivity.reaktor.internal.router.Router;
import org.reaktivity.reaktor.internal.types.control.Role;
import org.reaktivity.reaktor.internal.types.control.auth.ResolveFW;
import org.reaktivity.reaktor.internal.types.control.auth.UnresolveFW;

public class NukleusBuilderImpl implements NukleusBuilder
{
    private final String name;
    private final State state;
    private final Int2ObjectHashMap<CommandHandler> commandHandlersByTypeId;
    private final Map<Role, MessagePredicate> routeHandlers;
    private final Map<Role, StreamFactoryBuilder> streamFactoryBuilders;
    private final List<Nukleus> components;

    private Configuration config;
    private ExecutorService executor;

    public NukleusBuilderImpl(
        String name,
        State state)
    {
        this.name = name;
        this.state = state;
        this.commandHandlersByTypeId = new Int2ObjectHashMap<>();
        this.routeHandlers = new EnumMap<>(Role.class);
        this.streamFactoryBuilders = new EnumMap<>(Role.class);
        this.components = new LinkedList<>();
    }

    @Override
    public NukleusBuilder configure(
        Configuration config)
    {
        this.config = config;
        return this;
    }

    @Override
    public NukleusBuilder executor(
        ExecutorService executor)
    {
        this.executor = executor;
        return this;
    }

    @Override
    public NukleusBuilder commandHandler(
        int msgTypeId,
        CommandHandler handler)
    {
        switch(msgTypeId)
        {
        case ResolveFW.TYPE_ID:
            commandHandlersByTypeId.put(msgTypeId, handler);
            break;
        case UnresolveFW.TYPE_ID:
            commandHandlersByTypeId.put(msgTypeId, handler);
            break;
        default:
            throw new IllegalArgumentException(format("Unsupported msgTypeId %d", msgTypeId));
        }
        return this;
    }

    @Override
    public NukleusBuilder routeHandler(
        RouteKind kind,
        MessagePredicate handler)
    {
        Objects.requireNonNull(kind, "kind");
        Objects.requireNonNull(handler, "handler");

        switch (kind)
        {
        case CLIENT:
            this.routeHandlers.put(Role.CLIENT, handler);
            break;
        case PROXY:
            this.routeHandlers.put(Role.PROXY, handler);
            break;
        case SERVER:
            this.routeHandlers.put(Role.SERVER, handler);
            break;
        default:
            throw new IllegalStateException("Unrecognized route kind: " + kind);
        }
        return this;
    }

    @Override
    public NukleusBuilder streamFactory(
        RouteKind kind,
        StreamFactoryBuilder builder)
    {
        Objects.requireNonNull(kind, "kind");
        Objects.requireNonNull(builder, "builder");

        final Role role = Role.valueOf(kind.ordinal());
        this.streamFactoryBuilders.put(role, builder);
        return this;
    }

    @Override
    public NukleusBuilder inject(
        Nukleus component)
    {
        components.add(component);
        return this;
    }

    @Override
    public Nukleus build()
    {
        ReaktorConfiguration reaktorConfig = new ReaktorConfiguration(config);
        Context context = new Context();
        context.name(name).executor(executor).conclude(reaktorConfig);

        final boolean timestamps = reaktorConfig.timestamps();

        Conductor conductor = new Conductor(context);
        Router router = new Router(context, state, streamFactoryBuilders::get);

        conductor.setRouter(router);
        conductor.setCommandHandlerSupplier(commandHandlersByTypeId::get);
        router.setConductor(conductor);
        router.setTimestamps(timestamps);
        router.setRouteHandlerSupplier(routeHandlers::get);

        NukleusImpl nukleus = new NukleusImpl(name, config, conductor, router, context, components);

        conductor.freezeHandler(nukleus::freeze);

        return nukleus;
    }

    public static final class NukleusImpl extends Nukleus.Composite
    {
        private final String name;
        private final Configuration config;
        private final Context context;
        private final Runnable handleFreeze;

        NukleusImpl(
            String name,
            Configuration config,
            Conductor conductor,
            Router router,
            Context context,
            List<Nukleus> components)
        {
            super(conductor, router);
            this.name = name;
            this.config = config;
            this.context = context;
            this.handleFreeze = () -> exclude(conductor);

            components.forEach(this::include);
        }

        @Override
        public String name()
        {
            return name;
        }

        @Override
        public Configuration config()
        {
            return config;
        }

        @Override
        public void close() throws Exception
        {
            super.close();
            context.close();
        }

        public long counter(
            String name)
        {
            return context.counters().readonlyCounter(name).getAsLong();
        }

        public void freeze()
        {
            handleFreeze.run();
        }
    }
}
