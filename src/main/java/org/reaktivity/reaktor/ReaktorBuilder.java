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
package org.reaktivity.reaktor;

import static java.lang.Integer.bitCount;
import static java.lang.Integer.numberOfTrailingZeros;
import static java.util.Objects.requireNonNull;

import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntFunction;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.function.ToIntFunction;

import org.agrona.ErrorHandler;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.reaktivity.nukleus.Configuration;
import org.reaktivity.nukleus.Controller;
import org.reaktivity.nukleus.ControllerFactory;
import org.reaktivity.nukleus.Nukleus;
import org.reaktivity.nukleus.NukleusFactory;
import org.reaktivity.reaktor.internal.ControllerBuilderImpl;
import org.reaktivity.reaktor.internal.LabelManager;
import org.reaktivity.reaktor.internal.ReaktorConfiguration;
import org.reaktivity.reaktor.internal.StateImpl;
import org.reaktivity.reaktor.internal.agent.NukleusAgent;

public class ReaktorBuilder
{
    private Configuration config;
    private Predicate<String> nukleusMatcher;
    private Predicate<String> controllerMatcher;
    private ToIntFunction<String> affinityMask;
    private IdleStrategy idleStrategy;
    private ErrorHandler errorHandler;
    private Supplier<NukleusFactory> supplyNukleusFactory;

    private String roleName = "reaktor";
    private int threads = 1;

    ReaktorBuilder()
    {
        this.nukleusMatcher = n -> false;
        this.controllerMatcher = c -> false;
        this.affinityMask = n -> 1;
        this.supplyNukleusFactory = NukleusFactory::instantiate;
    }

    public ReaktorBuilder config(
        Configuration config)
    {
        this.config = requireNonNull(config);
        return this;
    }

    public ReaktorBuilder threads(
        int threads)
    {
        this.threads = threads;
        return this;
    }

    public ReaktorBuilder nukleus(
        Predicate<String> matcher)
    {
        requireNonNull(matcher);
        this.nukleusMatcher = n -> matcher.test(n);
        return this;
    }

    public ReaktorBuilder controller(
        Predicate<String> matcher)
    {
        requireNonNull(matcher);
        this.controllerMatcher = c -> matcher.test(c);
        return this;
    }

    public ReaktorBuilder affinityMask(
        ToIntFunction<String> affinityMask)
    {
        this.affinityMask = affinityMask;
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
        final Set<Configuration> configs = new LinkedHashSet<>();

        final ReaktorConfiguration config = new ReaktorConfiguration(this.config != null ? this.config : new Configuration());
        configs.add(config);

        final AtomicInteger routeId = new AtomicInteger();
        final LabelManager labels = new LabelManager(config.directory());

        final StateImpl[] states = new StateImpl[threads];
        for (int thread=0; thread < threads; thread++)
        {
            states[thread] = new StateImpl(thread, threads, routeId, config, labels);
        }

        final NukleusFactory nukleusFactory = supplyNukleusFactory.get();
        for (String name : nukleusFactory.names())
        {
            if (nukleusMatcher.test(name))
            {
                int affinity = supplyAffinity(name);
                StateImpl state = states[affinity];

                Nukleus nukleus = nukleusFactory.create(name, config);

                configs.add(nukleus.config());

                state.assign(new NukleusAgent(nukleus, state));
            }
        }

        ControllerFactory controllerFactory = ControllerFactory.instantiate();
        for (Class<? extends Controller> kind : controllerFactory.kinds())
        {
            final String name = controllerFactory.name(kind);
            if (controllerMatcher.test(name))
            {
                int affinity = supplyAffinity(name);
                StateImpl state = states[affinity];

                ControllerBuilderImpl<? extends Controller> builder = new ControllerBuilderImpl<>(config, kind);
                Controller controller = controllerFactory.create(config, builder);

                state.assign(controller);
            }
        }

        IdleStrategy idleStrategy = this.idleStrategy;
        if (idleStrategy == null)
        {
            idleStrategy = new BackoffIdleStrategy(
                config.maxSpins(),
                config.maxYields(),
                config.minParkNanos(),
                config.maxParkNanos());
        }
        ErrorHandler errorHandler = requireNonNull(this.errorHandler, "errorHandler");
        IntFunction<String> namer = t -> String.format("%s%d", roleName, t);

        return new Reaktor(idleStrategy, errorHandler, configs, states, namer);
    }

    private int supplyAffinity(
        String name)
    {
        int mask = affinityMask.applyAsInt(name);

        if (bitCount(mask) != 1)
        {
            throw new IllegalStateException(String.format("affinity mask must specify exactly one bit: %s %d",
                                                          name, mask));
        }

        return numberOfTrailingZeros(mask);
    }
}
