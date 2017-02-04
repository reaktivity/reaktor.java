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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.IntSupplier;

import org.agrona.ErrorHandler;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.IdleStrategy;
import org.reaktivity.nukleus.Controller;
import org.reaktivity.nukleus.Nukleus;
import org.reaktivity.reaktor.matchers.ControllerMatcher;
import org.reaktivity.reaktor.matchers.NukleusMatcher;

public final class Reaktor implements AutoCloseable
{
    private final IdleStrategy idleStrategy;
    private final ErrorHandler errorHandler;
    private final Map<String, Nukleus> nukleiByName;
    private final Map<Class<? extends Controller>, Controller> controllersByKind;

    private volatile AgentRunner runner;
    private volatile IntSupplier worker;

    Reaktor(
        IdleStrategy idleStrategy,
        ErrorHandler errorHandler,
        Nukleus[] nuklei,
        Controller[] controllers)
    {
        this.idleStrategy = idleStrategy;
        this.errorHandler = errorHandler;
        this.nukleiByName = new ConcurrentHashMap<>();
        this.controllersByKind = new ConcurrentHashMap<>();

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

    public void attach(
        Nukleus nukleus)
    {
        this.nukleiByName.put(nukleus.name(), nukleus);
        this.worker = supplyWorker();
    }

    public void attach(
        Controller controller)
    {
        this.controllersByKind.put(controller.kind(), controller);
        this.worker = supplyWorker();
    }

    public void detach(
        NukleusMatcher matcher)
    {
        if (this.nukleiByName.keySet().removeIf(matcher))
        {
            this.worker = supplyWorker();
        }
    }

    public void detach(
        ControllerMatcher matcher)
    {
        if (this.controllersByKind.keySet().removeIf(matcher))
        {
            this.worker = supplyWorker();
        }
    }

    public Reaktor start()
    {
        Agent agent = new Agent()
        {
            @Override
            public String roleName()
            {
                return "reaktor";
            }

            @Override
            public int doWork() throws Exception
            {
                return worker.getAsInt();
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
