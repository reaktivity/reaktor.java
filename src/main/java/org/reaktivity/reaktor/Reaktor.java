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
import static java.util.stream.Collectors.toList;
import static org.agrona.LangUtil.rethrowUnchecked;

import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.ServiceLoader;
import java.util.ServiceLoader.Provider;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.ToIntFunction;
import java.util.function.ToLongFunction;

import org.agrona.CloseHelper;
import org.agrona.ErrorHandler;
import org.agrona.concurrent.AgentRunner;
import org.reaktivity.reaktor.ext.ReaktorExtContext;
import org.reaktivity.reaktor.ext.ReaktorExtSpi;
import org.reaktivity.reaktor.internal.Info;
import org.reaktivity.reaktor.internal.LabelManager;
import org.reaktivity.reaktor.internal.Tuning;
import org.reaktivity.reaktor.internal.context.ConfigureTask;
import org.reaktivity.reaktor.internal.context.DispatchAgent;
import org.reaktivity.reaktor.internal.stream.NamespacedId;
import org.reaktivity.reaktor.nukleus.Configuration;
import org.reaktivity.reaktor.nukleus.Nukleus;

public final class Reaktor implements AutoCloseable
{
    private final Collection<Nukleus> nuklei;
    private final ExecutorService tasks;
    private final Callable<Void> configure;
    private final Collection<AgentRunner> runners;
    private final ToLongFunction<String> counter;
    private final Tuning tuning;
    private final List<ReaktorExtSpi> extensions;
    private final ReaktorExtContext context;

    private final AtomicInteger nextTaskId;
    private final ThreadFactory factory;

    private final ToIntFunction<String> supplyLabelId;

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

        Info info = new Info(config.directory(), coreCount);
        info.reset();

        LabelManager labels = new LabelManager(config.directory());

        Tuning tuning = new Tuning(config.directory(), coreCount);
        tuning.reset();
        for (ReaktorAffinity affinity : affinities)
        {
            int namespaceId = labels.supplyLabelId(affinity.namespace);
            int bindingId = labels.supplyLabelId(affinity.binding);
            long routeId = NamespacedId.id(namespaceId, bindingId);
            tuning.affinity(routeId, affinity.mask);
        }
        this.tuning = tuning;

        Collection<DispatchAgent> dispatchers = new LinkedHashSet<>();
        for (int coreIndex = 0; coreIndex < coreCount; coreIndex++)
        {
            DispatchAgent agent =
                new DispatchAgent(config, configURL, tasks, labels, errorHandler, tuning::affinity, nuklei, coreIndex);
            dispatchers.add(agent);
        }

        final Consumer<String> logger = config.verbose() ? System.out::print : m -> {};

        final List<ReaktorExtSpi> extensions = ServiceLoader.load(ReaktorExtSpi.class).stream()
                .map(Provider::get)
                .collect(toList());

        final ContextImpl context = new ContextImpl(config, errorHandler, dispatchers);
        final Callable<Void> configure =
                new ConfigureTask(configURL, labels::supplyLabelId, tuning, dispatchers,
                        errorHandler, logger, context, extensions);

        List<AgentRunner> runners = new ArrayList<>(dispatchers.size());
        dispatchers.forEach(d -> runners.add(d.runner()));

        final ToLongFunction<String> counter =
            name -> dispatchers.stream().mapToLong(d -> d.counter(name)).sum();

        this.supplyLabelId = labels::supplyLabelId;

        this.nuklei = nuklei;
        this.tasks = tasks;
        this.configure = configure;
        this.extensions = extensions;
        this.context = context;
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

    public ReaktorLoad load(
        String namespace,
        String binding)
    {
        return context.load(namespace, binding);
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

        tuning.close();

        extensions.forEach(e -> e.onClosed(context));

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

    private final class ContextImpl implements ReaktorExtContext
    {
        private final Configuration config;
        private final ErrorHandler errorHandler;
        private final Collection<DispatchAgent> dispatchers;

        private ContextImpl(
            Configuration config,
            ErrorHandler errorHandler,
            Collection<DispatchAgent> dispatchers)
        {
            this.config = config;
            this.errorHandler = errorHandler;
            this.dispatchers = dispatchers;
        }

        @Override
        public Configuration config()
        {
            return config;
        }

        @Override
        public ReaktorLoad load(
            String namespace,
            String binding)
        {
            int namespaceId = supplyLabelId.applyAsInt(namespace);
            int bindingId = supplyLabelId.applyAsInt(binding);
            long namespacedId = NamespacedId.id(namespaceId, bindingId);

            return new LoadImpl(namespacedId);
        }

        @Override
        public void onError(
            Exception error)
        {
            errorHandler.onError(error);
        }

        private final class LoadImpl implements ReaktorLoad
        {
            private final ToLongFunction<? super DispatchAgent> initialOpens;
            private final ToLongFunction<? super DispatchAgent> initialCloses;
            private final ToLongFunction<? super DispatchAgent> initialErrors;
            private final ToLongFunction<? super DispatchAgent> initialBytes;

            private final ToLongFunction<? super DispatchAgent> replyOpens;
            private final ToLongFunction<? super DispatchAgent> replyCloses;
            private final ToLongFunction<? super DispatchAgent> replyErrors;
            private final ToLongFunction<? super DispatchAgent> replyBytes;

            private LoadImpl(
                long id)
            {
                this.initialOpens = d -> d.initialOpens(id);
                this.initialCloses = d -> d.initialCloses(id);
                this.initialErrors = d -> d.initialErrors(id);
                this.initialBytes = d -> d.initialBytes(id);
                this.replyOpens = d -> d.replyOpens(id);
                this.replyCloses = d -> d.replyCloses(id);
                this.replyErrors = d -> d.replyErrors(id);
                this.replyBytes = d -> d.replyBytes(id);
            }

            @Override
            public long initialOpens()
            {
                return dispatchers.stream().mapToLong(initialOpens).sum();
            }

            @Override
            public long initialCloses()
            {
                return dispatchers.stream().mapToLong(initialCloses).sum();
            }

            @Override
            public long initialBytes()
            {
                return dispatchers.stream().mapToLong(initialBytes).sum();
            }

            @Override
            public long initialErrors()
            {
                return dispatchers.stream().mapToLong(initialErrors).sum();
            }

            @Override
            public long replyOpens()
            {
                return dispatchers.stream().mapToLong(replyOpens).sum();
            }

            @Override
            public long replyCloses()
            {
                return dispatchers.stream().mapToLong(replyCloses).sum();
            }

            @Override
            public long replyBytes()
            {
                return dispatchers.stream().mapToLong(replyBytes).sum();
            }

            @Override
            public long replyErrors()
            {
                return dispatchers.stream().mapToLong(replyErrors).sum();
            }
        }
    }
}
