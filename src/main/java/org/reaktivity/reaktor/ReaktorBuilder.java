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
package org.reaktivity.reaktor;

import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.agrona.ErrorHandler;
import org.agrona.concurrent.Agent;
import org.reaktivity.nukleus.Configuration;
import org.reaktivity.nukleus.Controller;
import org.reaktivity.nukleus.ControllerFactory;
import org.reaktivity.nukleus.Nukleus;
import org.reaktivity.nukleus.NukleusFactory;
import org.reaktivity.reaktor.internal.ControllerBuilderImpl;
import org.reaktivity.reaktor.internal.agent.ControllerAgent;
import org.reaktivity.reaktor.internal.agent.ElektronAgent;
import org.reaktivity.reaktor.internal.agent.NukleusAgent;

public class ReaktorBuilder
{
    private Configuration config;
    private Predicate<String> nukleusMatcher;
    private Predicate<String> controllerMatcher;
    private Map<String, BitSet> affinityMasks;
    private Function<String, BitSet> affinityMaskDefault;
    private ErrorHandler errorHandler;
    private Supplier<NukleusFactory> supplyNukleusFactory;
    private ThreadFactory threadFactory;

    private int threads = 1;
    private BitSet affinityMaskDefaultBits;

    ReaktorBuilder()
    {
        this.nukleusMatcher = n -> false;
        this.controllerMatcher = c -> false;
        this.affinityMasks = new ConcurrentHashMap<>();
        this.affinityMaskDefaultBits = BitSet.valueOf(new long[] { (1L << threads) - 1L });
        this.affinityMaskDefault = n -> affinityMaskDefaultBits;
        this.supplyNukleusFactory = NukleusFactory::instantiate;
        this.threadFactory = Thread::new;
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
        this.affinityMaskDefaultBits = BitSet.valueOf(new long[] { (1L << threads) - 1L });
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
        Function<String, BitSet> affinityMaskDefault)
    {
        this.affinityMaskDefault = affinityMaskDefault;
        return this;
    }

    public ReaktorBuilder affinityMask(
        String address,
        long affinityMask)
    {
        BitSet affinityBits = BitSet.valueOf(new long[] { affinityMask });
        this.affinityMasks.put(address, affinityBits);
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

        final int parallelism = config.taskParallelism();
        final ExecutorService executor = Executors.newFixedThreadPool(parallelism, new ReaktorTaskThreadFactory());

        final int count = threads;
        final ElektronAgent[] elektronAgents = new ElektronAgent[count];

        if (nukleusAgent != null)
        {
            final BiFunction<String, BitSet, BitSet> remapper = (k, v) -> v != null ? v : affinityMaskDefault.apply(k);
            final Function<String, BitSet> affinityMask = n -> affinityMasks.compute(n, remapper);
            for (int index = 0; index < count; index++)
            {
                elektronAgents[index] = nukleusAgent.supplyElektronAgent(index, count, executor, affinityMask);
            }
        }

        final ControllerAgent controllerAgent = new ControllerAgent();
        controllers.forEach(controllerAgent::assign);

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

        return new Reaktor(config, errorHandler, configs, executor, agents.toArray(new Agent[0]), threadFactory);
    }

    private static final class ReaktorTaskThreadFactory implements ThreadFactory
    {
        private final AtomicInteger nextThreadId = new AtomicInteger();
        private final ThreadFactory factory = Executors.defaultThreadFactory();

        @Override
        public Thread newThread(Runnable r)
        {
            Thread t = factory.newThread(r);

            if (t != null)
            {
                t.setName(String.format("reaktor/task#%d", nextThreadId.getAndIncrement()));
            }

            return t;
        }
    }
}
