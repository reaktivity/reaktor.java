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

import static org.agrona.LangUtil.rethrowUnchecked;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.agrona.CloseHelper;
import org.agrona.ErrorHandler;
import org.agrona.collections.ArrayUtil;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.IdleStrategy;
import org.reaktivity.nukleus.Configuration;
import org.reaktivity.nukleus.Controller;
import org.reaktivity.reaktor.internal.agent.ControllerAgent;
import org.reaktivity.reaktor.internal.agent.ElektronAgent;

public final class Reaktor implements AutoCloseable
{
    private final Set<Configuration> configs;
    private final ExecutorService executor;
    private final AgentRunner[] runners;
    private final ControllerAgent controllerAgent;
    private final List<ElektronAgent> elektronAgents;

    Reaktor(
        IdleStrategy idleStrategy,
        ErrorHandler errorHandler,
        Set<Configuration> configs,
        ExecutorService executor,
        Agent[] agents)
    {
        this.configs = configs;
        this.executor = executor;

        AgentRunner[] runners = new AgentRunner[0];
        for (Agent agent : agents)
        {
            runners = ArrayUtil.add(runners, new AgentRunner(idleStrategy, errorHandler, null, agent));
        }
        this.runners = runners;

        this.controllerAgent = Arrays.stream(agents)
            .filter(agent -> agent instanceof ControllerAgent)
            .map(ControllerAgent.class::cast)
            .findFirst()
            .orElse(null);

        this.elektronAgents = Arrays.stream(agents)
                .filter(agent -> agent instanceof ElektronAgent)
                .map(ElektronAgent.class::cast)
                .collect(Collectors.toList());
    }

    public void properties(
        BiConsumer<String, Object> valueAction,
        BiConsumer<String, Object> defaultAction)
    {
        configs.forEach(c -> c.properties(valueAction, defaultAction));
    }

    public <T extends Controller> T controller(
        Class<T> kind)
    {
        return controllerAgent != null ? controllerAgent.controller(kind) : null;
    }

    public Stream<Controller> controllers()
    {
        return controllerAgent != null ? controllerAgent.controllers() : null;
    }

    public long counter(
        String name)
    {
        return elektronAgents.stream().mapToLong(agent -> agent.counter(name)).sum();
    }

    public Reaktor start()
    {
        for (AgentRunner runner : runners)
        {
            AgentRunner.startOnThread(runner);
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

        executor.shutdownNow();

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
