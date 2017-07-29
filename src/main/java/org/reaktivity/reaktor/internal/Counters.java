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
package org.reaktivity.reaktor.internal;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.LongSupplier;

import org.agrona.CloseHelper;
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.status.CountersManager;

public final class Counters implements AutoCloseable
{
    private final CountersManager manager;
    private final ConcurrentMap<String, AtomicCounter> counters;
    private final ConcurrentMap<String, LongSupplier> readonlyCounters;

    Counters(CountersManager manager)
    {
        this.manager = manager;
        counters = new ConcurrentHashMap<>();
        readonlyCounters = new ConcurrentHashMap<>();
    }

    @Override
    public void close() throws Exception
    {
        counters.values().forEach(CloseHelper::quietClose);
    }

    public AtomicCounter routes()
    {
        return counter("routes");
    }

    public AtomicCounter streams()
    {
        return counter("streams");
    }

    public AtomicCounter counter(
        String name)
    {
        return counters.computeIfAbsent(name, manager::newCounter);
    }

    public LongSupplier readonlyCounter(
        String name)
    {
        LongSupplier readonlyCounter = readonlyCounters.get(name);
        if (readonlyCounter == null)
        {
            manager.forEach(this::populateReadonlyCounter);
            readonlyCounter = readonlyCounters.get(name);
        }

        if (readonlyCounter == null)
        {
            readonlyCounter = () -> 0L;
        }

        return readonlyCounter;
    }

    private void populateReadonlyCounter(
        int counterId,
        String name)
    {
        readonlyCounters.computeIfAbsent(name, k -> () -> manager.getCounterValue(counterId));
    }
}
