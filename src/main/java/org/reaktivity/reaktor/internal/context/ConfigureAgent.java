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

import java.util.BitSet;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.agrona.concurrent.Agent;
import org.reaktivity.reaktor.ReaktorConfiguration;
import org.reaktivity.reaktor.internal.LabelManager;
import org.reaktivity.reaktor.internal.ReaktorThreadFactory;
import org.reaktivity.reaktor.nukleus.Nukleus;
import org.reaktivity.reaktor.nukleus.NukleusFactory;

public class NukleusAgent implements Agent
{
    private final LabelManager labels;
    private final Set<ElektronAgent> elektronAgents;
    private final Set<Nukleus> nuklei;
    private final ExecutorService executor;

    public NukleusAgent(
        ReaktorConfiguration config,
        int agentCount)
    {
        this.labels = new LabelManager(config.directory());
        this.executor = Executors.newFixedThreadPool(config.taskParallelism(), new ReaktorThreadFactory("task"));

        final Set<Nukleus> nuklei = new LinkedHashSet<>();
        final NukleusFactory factory = NukleusFactory.instantiate();
        for (String name : factory.names())
        {
            Nukleus nukleus = factory.create(name, config);
            nuklei.add(nukleus);
        }
        this.nuklei = nuklei;

        // TODO: revisit affinity
        BitSet affinityMask = new BitSet(agentCount);
        affinityMask.set(0, affinityMask.size());

        Set<ElektronAgent> elektronAgents = new LinkedHashSet<>();
        for (int agentIndex = 0; agentIndex < agentCount; agentIndex++)
        {
            ElektronAgent agent = new ElektronAgent(config, executor, labels, affinityMask, nuklei, agentIndex);
            elektronAgents.add(agent);
        }
        this.elektronAgents = elektronAgents;
    }

    public Set<Nukleus> nuklei()
    {
        return nuklei;
    }

    public Set<ElektronAgent> elektronAgents()
    {
        return elektronAgents;
    }

    @Override
    public String roleName()
    {
        return "reaktor/config";
    }

    @Override
    public int doWork() throws Exception
    {
        // TODO: check config
        return 0;
    }

    @Override
    public void onClose()
    {
        executor.shutdownNow();
    }
}
