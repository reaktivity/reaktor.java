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
package org.reaktivity.reaktor.internal;

import java.io.Closeable;
import java.util.EnumMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

import org.reaktivity.nukleus.Nukleus;
import org.reaktivity.nukleus.NukleusBuilder;
import org.reaktivity.nukleus.buffer.BufferPool;
import org.reaktivity.nukleus.function.MessagePredicate;
import org.reaktivity.nukleus.route.RouteKind;
import org.reaktivity.nukleus.stream.StreamFactoryBuilder;
import org.reaktivity.reaktor.internal.acceptor.Acceptor;
import org.reaktivity.reaktor.internal.conductor.Conductor;
import org.reaktivity.reaktor.internal.router.Router;
import org.reaktivity.reaktor.internal.types.control.Role;
import org.reaktivity.reaktor.internal.watcher.Watcher;

public class NukleusBuilderImpl implements NukleusBuilder
{
    private final ReaktorConfiguration config;
    private final String name;
    private final Supplier<BufferPool> supplyBufferPool;
    private final Map<Role, MessagePredicate> routeHandlers;
    private final Map<RouteKind, StreamFactoryBuilder> streamFactoryBuilders;
    private final List<Nukleus> components;

    public NukleusBuilderImpl(
        ReaktorConfiguration config,
        String name,
        Supplier<BufferPool> supplyBufferPool)
    {
        this.config = config;
        this.name = name;
        this.supplyBufferPool = supplyBufferPool;
        this.routeHandlers = new EnumMap<>(Role.class);
        this.streamFactoryBuilders = new EnumMap<>(RouteKind.class);
        this.components = new LinkedList<>();
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

        this.streamFactoryBuilders.put(kind, builder);
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
        Context context = new Context();
        context.name(name).conclude(config);

        final int abortTypeId = config.abortStreamEventTypeId();

        Conductor conductor = new Conductor(context);
        Watcher watcher = new Watcher(context);
        Router router = new Router(context);
        Acceptor acceptor = new Acceptor(context);

        conductor.setAcceptor(acceptor);
        watcher.setAcceptor(acceptor);
        acceptor.setConductor(conductor);
        acceptor.setRouter(router);
        acceptor.setBufferPoolSupplier(supplyBufferPool);
        acceptor.setStreamFactoryBuilderSupplier(streamFactoryBuilders::get);
        acceptor.setAbortTypeId(abortTypeId);
        acceptor.setRouteHandlerSupplier(routeHandlers::get);

        return new NukleusImpl(name, conductor, watcher, router, acceptor, context, components);
    }

    private static final class NukleusImpl extends Nukleus.Composite
    {
        private final String name;
        private final Closeable cleanup;

        NukleusImpl(
            String name,
            Conductor conductor,
            Watcher watcher,
            Router router,
            Acceptor acceptor,
            Closeable cleanup,
            List<Nukleus> components)
        {
            super(conductor, watcher, router, acceptor);
            this.name = name;
            this.cleanup = cleanup;

            components.forEach(this::include);
        }

        @Override
        public String name()
        {
            return name;
        }

        @Override
        public void close() throws Exception
        {
            super.close();
            cleanup.close();
        }
    }
}
