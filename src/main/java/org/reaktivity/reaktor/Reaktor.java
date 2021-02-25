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

import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.ForkJoinPool.commonPool;
import static org.agrona.LangUtil.rethrowUnchecked;

import java.net.URL;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongFunction;
import java.util.function.ToLongFunction;

import org.agrona.CloseHelper;
import org.agrona.ErrorHandler;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.concurrent.AgentRunner;
import org.reaktivity.reaktor.internal.LabelManager;
import org.reaktivity.reaktor.internal.context.ConfigureTask;
import org.reaktivity.reaktor.internal.context.DispatchAgent;
import org.reaktivity.reaktor.internal.stream.NamespacedId;
import org.reaktivity.reaktor.nukleus.Nukleus;

public final class Reaktor implements AutoCloseable
{
    private final Collection<Nukleus> nuklei;
    private final ExecutorService tasks;
    private final Callable<Void> configure;
    private final Collection<AgentRunner> runners;
    private final ToLongFunction<String> counter;

    private final AtomicInteger nextTaskId;
    private final ThreadFactory factory;

    private Future<Void> configureRef;

    Reaktor(
        ReaktorConfiguration config,
        Collection<Nukleus> nuklei,
        ErrorHandler errorHandler,
        URL configURL,
        int coreCount,
        Collection<ReaktorAffinity> affinities)
    {
        this.nextTaskId = new AtomicInteger();
        this.factory = Executors.defaultThreadFactory();

        ExecutorService tasks = null;
        if (config.taskParallelism() > 0)
        {
            tasks = newFixedThreadPool(config.taskParallelism(), this::newTaskThread);
        }
        LabelManager labels = new LabelManager(config.directory());

        Collection<DispatchAgent> dispatchers = new LinkedHashSet<>();
        for (int coreIndex = 0; coreIndex < coreCount; coreIndex++)
        {
            BitSet defaultMask = BitSet.valueOf(new long[] { (1L << coreCount) - 1L });
            Long2ObjectHashMap<BitSet> affinityMasks = new Long2ObjectHashMap<>();
            for (ReaktorAffinity affinity : affinities)
            {
                int namespaceId = labels.supplyLabelId(affinity.namespace);
                int bindingId = labels.supplyLabelId(affinity.binding);
                long routeId = NamespacedId.id(namespaceId, bindingId);
                BitSet mask = BitSet.valueOf(new long[] { affinity.mask });
                affinityMasks.put(routeId, mask);
            }
            LongFunction<BitSet> defaulter = r -> defaultMask;
            LongFunction<BitSet> affinityMask = r -> affinityMasks.computeIfAbsent(r, defaulter);

            DispatchAgent agent =
                    new DispatchAgent(config, configURL, tasks, labels, errorHandler, affinityMask, nuklei, coreIndex);
            dispatchers.add(agent);
        }

        final Callable<Void> configure = new ConfigureTask(configURL, labels::supplyLabelId, dispatchers, errorHandler);

        List<AgentRunner> runners = new ArrayList<>(dispatchers.size());
        dispatchers.forEach(d -> runners.add(d.runner()));

        final ToLongFunction<String> counter =
            name -> dispatchers.stream().mapToLong(d -> d.counter(name)).sum();

        this.nuklei = nuklei;
        this.tasks = tasks;
        this.configure = configure;
        this.runners = runners;
        this.counter = counter;
    }

    public <T> T nukleus(
        Class<T> kind)
    {
        return nuklei.stream()
                .filter(kind::isInstance)
                .map(kind::cast)
                .findFirst()
                .orElse(null);
    }

    public long counter(
        String name)
    {
        return counter.applyAsLong(name);
    }

    public Future<Void> start()
    {
        for (AgentRunner runner : runners)
        {
            AgentRunner.startOnThread(runner, Thread::new);
        }

        this.configureRef = commonPool().submit(configure);

        return configureRef;
    }

    @Override
    public void close() throws Exception
    {
        final List<Throwable> errors = new ArrayList<>();

        configureRef.cancel(true);

        for (AgentRunner runner : runners)
        {
            try
            {
                CloseHelper.close(runner);
            }
            catch (Throwable ex)
            {
                errors.add(ex);
            }
        }

        if (tasks != null)
        {
            tasks.shutdownNow();
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

    private Thread newTaskThread(
        Runnable r)
    {
        Thread t = factory.newThread(r);

        if (t != null)
        {
            t.setName(String.format("reaktor/task#%d", nextTaskId.getAndIncrement()));
        }

        return t;
    }
}
