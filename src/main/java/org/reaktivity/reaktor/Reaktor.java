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
import static org.agrona.concurrent.AgentRunner.startOnThread;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.function.IntFunction;

import org.agrona.CloseHelper;
import org.agrona.ErrorHandler;
import org.agrona.LangUtil;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.IdleStrategy;
import org.reaktivity.nukleus.Configuration;
import org.reaktivity.nukleus.Controller;
import org.reaktivity.nukleus.buffer.BufferPool;
import org.reaktivity.reaktor.internal.State;
import org.reaktivity.reaktor.internal.agent.NukleusAgent;

public final class Reaktor implements AutoCloseable
{
    private final IdleStrategy idleStrategy;
    private final ErrorHandler errorHandler;
    private final Set<Configuration> configs;
    private final Map<String, NukleusAgent> nukleiByName;
    private final Map<Class<? extends Controller>, Controller> controllersByKind;

    private volatile Core[] cores;

    Reaktor(
        IdleStrategy idleStrategy,
        ErrorHandler errorHandler,
        Set<Configuration> configs,
        State[] states,
        IntFunction<String> namer)
    {
        this.idleStrategy = idleStrategy;
        this.errorHandler = errorHandler;
        this.configs = configs;
        this.nukleiByName = new ConcurrentHashMap<>();
        this.controllersByKind = new ConcurrentHashMap<>();

        Core[] cores = new Core[states.length];
        for (int i=0; i < states.length; i++)
        {
            cores[i] = new Core(namer.apply(i), states[i]);

            for (Agent agent : states[i].agents())
            {
                if (agent instanceof NukleusAgent)
                {
                    NukleusAgent nukleus = (NukleusAgent) agent;
                    nukleiByName.put(nukleus.roleName(), nukleus);
                }
            }

            for (Controller controller : states[i].controllers())
            {
                controllersByKind.put(controller.kind(), controller);
            }
        }
        this.cores = cores;
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
        return kind.cast(controllersByKind.get(kind));
    }

    public Agent nukleus(
        String name)
    {
        return nukleiByName.get(name);
    }

    public Set<Class<? extends Controller>> controllerKinds()
    {
        return controllersByKind.keySet();
    }

    public Reaktor start()
    {
        for (Core core : cores)
        {
            core.start();
        }

        return this;
    }

    @Override
    public void close() throws Exception
    {
        final List<Throwable> errors = new ArrayList<>();
        for (Core core : cores)
        {
            try
            {
                core.stop();
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

    private final class Core implements Agent
    {
        private final String roleName;
        private final BufferPool bufferPool;
        private final Agent[] agents;
        private final Controller[] controllers;

        private volatile AgentRunner runner;

        Core(
            String roleName,
            State state)
        {
            this.agents = state.agents().toArray(new Agent[0]);
            this.roleName = roleName;
            this.controllers = state.controllers().toArray(new Controller[0]);
            this.bufferPool = state.bufferPool();
        }

        public void start()
        {
            this.runner = new AgentRunner(idleStrategy, errorHandler, null, this);

            startOnThread(runner);
        }

        public void stop()
        {
            CloseHelper.close(runner);
        }

        @Override
        public String roleName()
        {
            return roleName;
        }

        @Override
        public int doWork() throws Exception
        {
            int workDone = 0;

            for (final Agent agent : agents)
            {
                workDone += agent.doWork();
            }

            final Controller[] controllers = this.controllers;
            for (int i=0; i < controllers.length; i++)
            {
                workDone += controllers[i].process();
            }

            return workDone;
        }

        @Override
        public void onClose()
        {
            final List<Throwable> errors = new ArrayList<>();
            for (int i=0; i < agents.length; i++)
            {
                try
                {
                    agents[i].onClose();
                }
                catch (Throwable t)
                {
                    errors.add(t);
                }
            }
            for (int i=0; i < controllers.length; i++)
            {
                try
                {
                    controllers[i].close();
                }
                catch (Throwable t)
                {
                    errors.add(t);
                }
            }

            if (bufferPool.acquiredSlots() != 0)
            {
                errors.add(new IllegalStateException("Buffer pool has unreleased slots: " + bufferPool.acquiredSlots()));
            }

            if (!errors.isEmpty())
            {
                final Throwable t = errors.get(0);
                errors.stream().filter(x -> x != t).forEach(x -> t.addSuppressed(x));
                LangUtil.rethrowUnchecked(t);
            }
        }
    }
}
