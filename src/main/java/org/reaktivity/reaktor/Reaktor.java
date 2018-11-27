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
import org.reaktivity.nukleus.Nukleus;
import org.reaktivity.nukleus.buffer.BufferPool;
import org.reaktivity.reaktor.internal.State;

public final class Reaktor implements AutoCloseable
{
    private final IdleStrategy idleStrategy;
    private final ErrorHandler errorHandler;
    private final Set<Configuration> configs;
    private final Map<String, Nukleus> nukleiByName;
    private final Map<Class<? extends Controller>, Controller> controllersByKind;

    private volatile Core[] cores;

    Reaktor(
        IdleStrategy idleStrategy,
        ErrorHandler errorHandler,
        Set<Configuration> configs,
        State[] states,
        IntFunction<String> roleName)
    {
        this.idleStrategy = idleStrategy;
        this.errorHandler = errorHandler;
        this.configs = configs;
        this.nukleiByName = new ConcurrentHashMap<>();
        this.controllersByKind = new ConcurrentHashMap<>();

        Core[] cores = new Core[states.length];
        for (int i=0; i < states.length; i++)
        {
            cores[i] = new Core(roleName.apply(i), states[i]);

            for (Nukleus nukleus : states[i].nuklei())
            {
                nukleiByName.put(nukleus.name(), nukleus);
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

    public <T extends Nukleus> T nukleus(
        String name,
        Class<T> kind)
    {
        return kind.cast(nukleiByName.get(name));
    }

    public Nukleus nukleus(
        String name)
    {
        return nukleiByName.get(name);
    }

    public Set<Class<? extends Controller>> controllerKinds()
    {
        return controllersByKind.keySet();
    }

    public Set<String> nukleusNames()
    {
        return nukleiByName.keySet();
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
        private final Nukleus[] nuklei;
        private final Controller[] controllers;

        private volatile AgentRunner runner;

        Core(
            String roleName,
            State state)
        {
            this.roleName = roleName;
            this.nuklei = state.nuklei().toArray(new Nukleus[0]);
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
            int work = 0;

            final Nukleus[] nuklei = this.nuklei;
            for (int i=0; i < nuklei.length; i++)
            {
                work += nuklei[i].process();
            }

            final Controller[] controllers = this.controllers;
            for (int i=0; i < controllers.length; i++)
            {
                work += controllers[i].process();
            }

            return work;
        }

        @Override
        public void onClose()
        {
            final List<Throwable> errors = new ArrayList<>();
            for (int i=0; i < nuklei.length; i++)
            {
                try
                {
                    nuklei[i].close();
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
