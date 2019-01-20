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

import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.function.ToLongFunction;

import org.agrona.ErrorHandler;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.reaktivity.nukleus.Configuration;
import org.reaktivity.nukleus.Controller;
import org.reaktivity.nukleus.ControllerFactory;
import org.reaktivity.nukleus.Nukleus;
import org.reaktivity.nukleus.NukleusFactory;
import org.reaktivity.reaktor.internal.ControllerBuilderImpl;
import org.reaktivity.reaktor.internal.ReaktorConfiguration;
import org.reaktivity.reaktor.internal.agent.ControllerAgent;
import org.reaktivity.reaktor.internal.agent.ElektronAgent;
import org.reaktivity.reaktor.internal.agent.NukleusAgent;

public class ReaktorBuilder
{
    private Configuration config;
    private Predicate<String> nukleusMatcher;
    private Predicate<String> controllerMatcher;
    private Map<String, Long> affinityMasks;
    private ToLongFunction<String> affinityMaskDefault;
    private IdleStrategy idleStrategy;
    private ErrorHandler errorHandler;
    private Supplier<NukleusFactory> supplyNukleusFactory;

    private int threads = 1;


    ReaktorBuilder()
    {
        this.nukleusMatcher = n -> false;
        this.controllerMatcher = c -> false;
        this.affinityMasks = new HashMap<>();
        this.affinityMaskDefault = n -> 1L;
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

    public ReaktorBuilder affinityMaskDefault(
        ToLongFunction<String> affinityMaskDefault)
    {
        this.affinityMaskDefault = affinityMaskDefault;
        return this;
    }

    public ReaktorBuilder affinityMask(
        String address,
        long affinityMask)
    {
        this.affinityMasks.put(address, affinityMask);
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

    public Reaktor build()
    {
        final Set<Configuration> configs = new LinkedHashSet<>();

        final ReaktorConfiguration config = new ReaktorConfiguration(this.config != null ? this.config : new Configuration());
        configs.add(config);

        final List<Nukleus> nuklei = new ArrayList<>();
        final NukleusFactory nukleusFactory = supplyNukleusFactory.get();
        for (String name : nukleusFactory.names())
        {
            if (nukleusMatcher.test(name))
            {
                Nukleus nukleus = nukleusFactory.create(name, config);
                configs.add(nukleus.config());
                nuklei.add(nukleus);
            }
        }

        // ensure control file is not created for no nuklei
        NukleusAgent nukleusAgent = null;
        if (!nuklei.isEmpty())
        {
            nukleusAgent = new NukleusAgent(config);
            nuklei.forEach(nukleusAgent::assign);
        }

        final List<Controller> controllers = new ArrayList<>();
        final ControllerFactory controllerFactory = ControllerFactory.instantiate();
        for (Class<? extends Controller> kind : controllerFactory.kinds())
        {
            final String name = controllerFactory.name(kind);
            if (controllerMatcher.test(name))
            {
                ControllerBuilderImpl<? extends Controller> builder = new ControllerBuilderImpl<>(config, kind);
                Controller controller = controllerFactory.create(config, builder);
                controllers.add(controller);
            }
        }

        // TODO: ReaktorConfiguration for executor pool size
        final ExecutorService executor = Executors.newFixedThreadPool(1);

        final int count = threads;
        final ElektronAgent[] elektronAgents = new ElektronAgent[count];

        if (nukleusAgent != null)
        {
            final BiFunction<String, Long, Long> remapper = (k, v) -> v != null ? v : affinityMaskDefault.applyAsLong(k);
            final ToLongFunction<String> affinityMask = n -> affinityMasks.compute(n, remapper);
            for (int index=0; index < count; index++)
            {
                elektronAgents[index] = nukleusAgent.supplyElektronAgent(index, count, executor, affinityMask);
            }
        }

        final ControllerAgent controllerAgent = new ControllerAgent();
        controllers.forEach(controllerAgent::assign);

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

        List<Agent> agents = new ArrayList<>();
        if (nukleusAgent != null)
        {
            for (Agent elektronAgent : elektronAgents)
            {
                agents.add(elektronAgent);
            }
            agents.add(nukleusAgent);
        }
        if (!controllerAgent.isEmpty())
        {
            agents.add(controllerAgent);
        }

        return new Reaktor(idleStrategy, errorHandler, configs, executor, agents.toArray(new Agent[0]));
    }
}
