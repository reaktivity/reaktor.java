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

import static org.agrona.LangUtil.rethrowUnchecked;
import static org.agrona.concurrent.AgentRunner.startOnThread;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.IntSupplier;
import java.util.function.Supplier;

import org.agrona.ErrorHandler;
import org.agrona.LangUtil;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.IdleStrategy;
import org.reaktivity.nukleus.Controller;
import org.reaktivity.nukleus.Nukleus;
import org.reaktivity.nukleus.buffer.BufferPool;
import org.reaktivity.reaktor.internal.State;

public final class Reaktor implements AutoCloseable
{
    private final IdleStrategy idleStrategy;
    private final ErrorHandler errorHandler;
    private final Set<State> states;
    private final Map<String, Nukleus> nukleiByName;
    private final Map<Class<? extends Controller>, Controller> controllersByKind;

    private volatile AgentRunner runner;
    private volatile IntSupplier worker;
    private String roleName;

    Reaktor(
        IdleStrategy idleStrategy,
        ErrorHandler errorHandler,
        Nukleus[] nuklei,
        Controller[] controllers,
        Set<State> states,
        String roleName)
    {
        this.idleStrategy = idleStrategy;
        this.errorHandler = errorHandler;
        this.states = states;
        this.nukleiByName = new ConcurrentHashMap<>();
        this.controllersByKind = new ConcurrentHashMap<>();
        this.roleName = roleName;

        for (Nukleus nukleus : nuklei)
        {
            nukleiByName.put(nukleus.name(), nukleus);
        }

        for (Controller controller : controllers)
        {
            controllersByKind.put(controller.kind(), controller);
        }

        this.worker = supplyWorker();
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
        Agent agent = new Agent()
        {
            @Override
            public String roleName()
            {
                return roleName;
            }

            @Override
            public int doWork() throws Exception
            {
                return worker.getAsInt();
            }

            @Override
            public void onClose()
            {
                final Nukleus[] nuklei0 = nukleiByName.values().toArray(new Nukleus[0]);
                final Controller[] controllers0 = controllersByKind.values().toArray(new Controller[0]);
                final List<Throwable> errors = new ArrayList<>();
                for (int i=0; i < nuklei0.length; i++)
                {
                    try
                    {
                        nuklei0[i].close();
                    }
                    catch (Throwable t)
                    {
                        errors.add(t);
                    }
                }
                for (int i=0; i < controllers0.length; i++)
                {
                    try
                    {
                        controllers0[i].close();
                    }
                    catch (Throwable t)
                    {
                        errors.add(t);
                    }
                }

                for (State state : states)
                {
                    final Supplier<BufferPool> supplyBufferPool = state.supplyBufferPool();
                    final BufferPool bufferPool = supplyBufferPool.get();
                    if (bufferPool.acquiredSlots() != 0)
                    {
                        errors.add(new IllegalStateException("Buffer pool has unreleased slots: " + bufferPool.acquiredSlots()));
                    }
                }

                if (!errors.isEmpty())
                {
                    final Throwable t = errors.get(0);
                    errors.stream().filter(x -> x != t).forEach(x -> t.addSuppressed(x));
                    LangUtil.rethrowUnchecked(t);
                }
            }
        };

        this.runner = new AgentRunner(idleStrategy, errorHandler, null, agent);

        startOnThread(runner);

        return this;
    }

    private IntSupplier supplyWorker()
    {
        final Nukleus[] nuklei0 = nukleiByName.values().toArray(new Nukleus[0]);
        final Controller[] controllers0 = controllersByKind.values().toArray(new Controller[0]);

        return () ->
        {
            int work = 0;

            for (int i=0; i < nuklei0.length; i++)
            {
                work += nuklei0[i].process();
            }

            for (int i=0; i < controllers0.length; i++)
            {
                work += controllers0[i].process();
            }

            return work;
        };
    }

    @Override
    public void close() throws Exception
    {
        try
        {
            runner.close();
        }
        catch (final Exception ex)
        {
            rethrowUnchecked(ex);
        }
    }

    public static ReaktorBuilder builder()
    {
        return new ReaktorBuilder();
    }
}
