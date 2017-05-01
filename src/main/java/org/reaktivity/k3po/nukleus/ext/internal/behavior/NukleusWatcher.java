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
package org.reaktivity.k3po.nukleus.ext.internal.behavior;

import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_DELETE;
import static java.nio.file.StandardWatchEventKinds.OVERFLOW;
import static java.util.Arrays.stream;
import static org.agrona.CloseHelper.quietClose;
import static org.agrona.IoUtil.delete;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.agrona.LangUtil;

import com.sun.nio.file.SensitivityWatchEventModifier;

@SuppressWarnings("restriction")
public final class NukleusWatcher implements AutoCloseable
{
    private final WatchService service;
    private final Path streamsPath;
    private final Set<Path> sourcePaths;
    private final Consumer<WatchEvent<?>> handleEvent;

    private NukleusScope router;
    private WatchKey streamsKey;

    public NukleusWatcher(
        Supplier<WatchService> watchService,
        Path streamsPath)
    {
        this.service = watchService.get();
        this.streamsPath = streamsPath;
        this.sourcePaths = new HashSet<>();

        Map<WatchEvent.Kind<?>, Consumer<WatchEvent<?>>> handlerMap = new HashMap<>();
        handlerMap.put(StandardWatchEventKinds.OVERFLOW, this::handleOverflow);
        handlerMap.put(StandardWatchEventKinds.ENTRY_CREATE, this::handleCreate);
        handlerMap.put(StandardWatchEventKinds.ENTRY_DELETE, this::handleDelete);
        this.handleEvent = e -> handlerMap.getOrDefault(e.kind(), this::handleUnexpected).accept(e);
        registerIfNecessary();
    }

    public void setRouter(
        NukleusScope router)
    {
        this.router = router;
    }

    public int process()
    {
        registerIfNecessary();

        int workCount = 0;

        WatchKey key = service.poll();
        if (key != null && key.isValid())
        {
            List<WatchEvent<?>> events = key.pollEvents();
            workCount += events.size();
            events.forEach(handleEvent);
            key.reset();
        }

        return workCount;
    }

    @Override
    public void close()
    {
        cancelIfNecessary();

        quietClose(service);
    }

    private void handleCreate(
        WatchEvent<?> event)
    {
        Path sourcePath = (Path) event.context();
        handleCreatePath(sourcePath);
    }

    private void handleCreatePath(
        Path sourcePath)
    {
        if (sourcePaths.add(sourcePath))
        {
            router.onReadable(sourcePath);
        }
    }

    private void handleDelete(
        WatchEvent<?> event)
    {
        Path sourcePath = (Path) event.context();
        handleDeletePath(sourcePath);
    }

    private void handleDeletePath(
        Path sourcePath)
    {
        if (sourcePaths.remove(sourcePath))
        {
            router.onExpired(sourcePath);
        }
    }

    private void handleOverflow(
        WatchEvent<?> event)
    {
        syncWithFileSystem();
    }

    private void handleUnexpected(
        WatchEvent<?> event)
    {
        // ignore
    }

    private void registerIfNecessary()
    {
        if (streamsKey == null)
        {
            try
            {
                if (Files.exists(streamsPath))
                {
                    delete(streamsPath.toFile(), false);
                }
                streamsPath.toFile().mkdirs();
                WatchEvent.Kind<?>[] kinds = { ENTRY_CREATE, ENTRY_DELETE, OVERFLOW };
                Path absolutePath = streamsPath.toAbsolutePath();
                streamsKey = absolutePath.register(service, kinds, SensitivityWatchEventModifier.HIGH);
            }
            catch (IOException ex)
            {
                LangUtil.rethrowUnchecked(ex);
            }
        }
    }

    private void cancelIfNecessary()
    {
        if (streamsKey != null)
        {
            streamsKey.cancel();
        }
    }

    private void syncWithFileSystem()
    {
        sourcePaths.stream().filter(p -> !p.toFile().exists()).forEach(this::handleDeletePath);
        stream(streamsPath.toFile().listFiles()).map(f -> f.toPath()).forEach(this::handleCreatePath);
    }
}
