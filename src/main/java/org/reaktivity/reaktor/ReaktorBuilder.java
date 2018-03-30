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
package org.reaktivity.reaktor;

import static java.util.Objects.requireNonNull;

import java.util.HashMap;
import java.util.Map;
import java.util.function.LongSupplier;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.agrona.ErrorHandler;
import org.agrona.collections.ArrayUtil;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.reaktivity.nukleus.Configuration;
import org.reaktivity.nukleus.Controller;
import org.reaktivity.nukleus.ControllerFactory;
import org.reaktivity.nukleus.Nukleus;
import org.reaktivity.nukleus.NukleusBuilder;
import org.reaktivity.nukleus.NukleusFactory;
import org.reaktivity.nukleus.buffer.BufferPool;
import org.reaktivity.reaktor.internal.ControllerBuilderImpl;
import org.reaktivity.reaktor.internal.NukleusBuilderImpl;
import org.reaktivity.reaktor.internal.ReaktorConfiguration;
import org.reaktivity.reaktor.internal.buffer.DefaultBufferPool;

public class ReaktorBuilder
{
    private Configuration config;
    private Predicate<String> nukleusMatcher;
    private Predicate<String> controllerMatcher;
    private IdleStrategy idleStrategy;
    private ErrorHandler errorHandler;
    private Supplier<NukleusFactory> supplyNukleusFactory;
    private String roleName = "reaktor";

    ReaktorBuilder()
    {
        this.nukleusMatcher = x -> false;
        this.controllerMatcher = x -> false;
        this.supplyNukleusFactory = NukleusFactory::instantiate;
    }

    public ReaktorBuilder config(
        Configuration config)
    {
        this.config = requireNonNull(config);
        return this;
    }

    public ReaktorBuilder nukleus(
        Predicate<String> matcher)
    {
        this.nukleusMatcher = requireNonNull(matcher);
        return this;
    }

    public ReaktorBuilder controller(
        Predicate<String> matcher)
    {
        this.controllerMatcher = requireNonNull(matcher);
        return this;
    }

    public ReaktorBuilder idleStrategy(
        IdleStrategy idleStrategy)
    {
        this.idleStrategy = requireNonNull(idleStrategy);
        return this;
    }

    public ReaktorBuilder errorHandler(
        ErrorHandler errorHandler)
    {
        this.errorHandler = requireNonNull(errorHandler);
        return this;
    }

    public ReaktorBuilder loader(
        ClassLoader loader)
    {
        requireNonNull(loader);
        this.supplyNukleusFactory = () -> NukleusFactory.instantiate(loader);
        return this;
    }

    public ReaktorBuilder roleName(
        String roleName)
    {
        this.roleName = requireNonNull(roleName);
        return this;
    }

    public Reaktor build()
    {
        final ReaktorConfiguration config = new ReaktorConfiguration(this.config != null ? this.config : new Configuration());
        final NukleusFactory nukleusFactory = supplyNukleusFactory.get();

        // TODO: bufferPool per thread
        final int bufferPoolCapacity = config.bufferPoolCapacity();
        final int bufferSlotCapacity = config.bufferSlotCapacity();
        final DefaultBufferPool bufferPool = new DefaultBufferPool(bufferPoolCapacity, bufferSlotCapacity);
        final Supplier<BufferPool> supplyBufferPool = () -> bufferPool;
        final long[] streamId = new long[1];
        final long[] groupId = new long[1];
        final long[] traceId = new long[1];
        final LongSupplier supplyStreamId = () -> ++streamId[0];
        final LongSupplier supplyTrace = () -> ++traceId[0];
        final LongSupplier supplyGroupId = () -> ++groupId[0];

        Nukleus[] nuklei = new Nukleus[0];
        for (String name : nukleusFactory.names())
        {
            if (nukleusMatcher.test(name))
            {
                NukleusBuilder builder = new NukleusBuilderImpl(config, name, supplyBufferPool, supplyStreamId,
                        supplyTrace, supplyGroupId);
                Nukleus nukleus = nukleusFactory.create(name, config, builder);
                nuklei = ArrayUtil.add(nuklei, nukleus);
            }
        }

        ControllerFactory controllerFactory = ControllerFactory.instantiate();

        Controller[] controllers = new Controller[0];
        Map<Class<? extends Controller>, Controller> controllersByKind = new HashMap<>();
        for (Class<? extends Controller> kind : controllerFactory.kinds())
        {
            if (controllerMatcher.test(controllerFactory.name(kind)))
            {
                ControllerBuilderImpl<? extends Controller> builder = new ControllerBuilderImpl<>(config, kind);
                Controller controller = controllerFactory.create(config, builder);
                controllersByKind.put(kind, controller);
                controllers = ArrayUtil.add(controllers, controller);
            }
        }

        IdleStrategy idleStrategy = this.idleStrategy;
        if (idleStrategy == null)
        {
            idleStrategy = new BackoffIdleStrategy(
                config.maxSpins(),
                config.maxYields(),
                config.minParkPeriodNanos(),
                config.maxParkPeriodNanos());
        }
        ErrorHandler errorHandler = requireNonNull(this.errorHandler, "errorHandler");

        return new Reaktor(idleStrategy, errorHandler, nuklei, controllers, bufferPool, roleName);
    }
}
