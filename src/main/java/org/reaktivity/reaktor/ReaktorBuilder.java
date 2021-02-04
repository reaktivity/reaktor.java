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
import static java.util.concurrent.Executors.newFixedThreadPool;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.function.ToLongFunction;

import org.agrona.ErrorHandler;
import org.agrona.concurrent.Agent;
import org.reaktivity.reaktor.internal.LabelManager;
import org.reaktivity.reaktor.internal.ReaktorThreadFactory;
import org.reaktivity.reaktor.internal.context.ConfigureAgent;
import org.reaktivity.reaktor.internal.context.DispatchAgent;
import org.reaktivity.reaktor.nukleus.Configuration;
import org.reaktivity.reaktor.nukleus.Nukleus;
import org.reaktivity.reaktor.nukleus.NukleusFactory;

public class ReaktorBuilder
{
    private Configuration config;
    private ErrorHandler errorHandler;

    private int threads = 1;

    ReaktorBuilder()
    {
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

    public ReaktorBuilder errorHandler(
        ErrorHandler errorHandler)
    {
        this.errorHandler = requireNonNull(errorHandler);
        return this;
    }

    public Reaktor build()
    {
        final ReaktorConfiguration config = new ReaktorConfiguration(this.config != null ? this.config : new Configuration());

        final Set<Nukleus> nuklei = new LinkedHashSet<>();
        final NukleusFactory factory = NukleusFactory.instantiate();
        for (String name : factory.names())
        {
            Nukleus nukleus = factory.create(name, config);
            nuklei.add(nukleus);
        }

        int coreCount = threads;
        ExecutorService executor = newFixedThreadPool(config.taskParallelism(), new ReaktorThreadFactory("task"));
        LabelManager labels = new LabelManager(config.directory());

        // TODO: revisit affinity
        BitSet affinityMask = new BitSet(coreCount);
        affinityMask.set(0, affinityMask.size());

        Set<DispatchAgent> dispatchAgents = new LinkedHashSet<>();
        for (int coreIndex = 0; coreIndex < coreCount; coreIndex++)
        {
            DispatchAgent agent = new DispatchAgent(config, executor, labels, affinityMask, nuklei, coreIndex);
            dispatchAgents.add(agent);
        }

        final ConfigureAgent configureAgent = new ConfigureAgent(dispatchAgents);
        final ErrorHandler errorHandler = requireNonNull(this.errorHandler, "errorHandler");

        List<Agent> agents = new ArrayList<>();
        agents.add(configureAgent);
        agents.addAll(dispatchAgents);

        final ToLongFunction<String> counter =
            name -> dispatchAgents.stream().mapToLong(d -> d.counter(name)).sum();

        return new Reaktor(config, errorHandler, executor, nuklei, agents, counter);
    }
}
