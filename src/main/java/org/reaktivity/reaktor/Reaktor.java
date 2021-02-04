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

import static org.agrona.LangUtil.rethrowUnchecked;

import java.util.ArrayList;
import java.util.List;

import org.agrona.CloseHelper;
import org.agrona.ErrorHandler;
import org.agrona.collections.ArrayUtil;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.reaktivity.reaktor.internal.agent.NukleusAgent;

public final class Reaktor implements AutoCloseable
{
    private final AgentRunner[] runners;
    private final NukleusAgent nukleusAgent;

    Reaktor(
        ReaktorConfiguration config,
        ErrorHandler errorHandler,
        NukleusAgent nukleusAgent)
    {
        List<Agent> agents = new ArrayList<>();
        agents.add(nukleusAgent);
        agents.addAll(nukleusAgent.elektronAgents());

        AgentRunner[] runners = new AgentRunner[0];
        for (Agent agent : agents)
        {
            final IdleStrategy idleStrategy = new BackoffIdleStrategy(
                    config.maxSpins(),
                    config.maxYields(),
                    config.minParkNanos(),
                    config.maxParkNanos());
            runners = ArrayUtil.add(runners, new AgentRunner(idleStrategy, errorHandler, null, agent));
        }
        this.runners = runners;

        this.nukleusAgent = nukleusAgent;
    }

    public <T> T nukleus(
        Class<T> kind)
    {
        return nukleusAgent.nuklei()
                .stream()
                .filter(kind::isInstance)
                .map(kind::cast)
                .findFirst()
                .orElse(null);
    }

    public long counter(
        String name)
    {
        return nukleusAgent.elektronAgents().stream().mapToLong(agent -> agent.counter(name)).sum();
    }

    public Reaktor start()
    {
        for (AgentRunner runner : runners)
        {
            AgentRunner.startOnThread(runner, Thread::new);
        }

        return this;
    }

    @Override
    public void close() throws Exception
    {
        final List<Throwable> errors = new ArrayList<>();
        for (AgentRunner runner : runners)
        {
            try
            {
                CloseHelper.close(runner);
            }
            catch (Throwable t)
            {
                errors.add(t);
            }
        }

        if (!errors.isEmpty())
        {
            final Throwable t = errors.get(0);
            errors.stream().filter(x -> x != t).forEach(x -> t.addSuppressed(x));
            rethrowUnchecked(t);
        }
    }

    public static ReaktorBuilder builder()
    {
        return new ReaktorBuilder();
    }
}
