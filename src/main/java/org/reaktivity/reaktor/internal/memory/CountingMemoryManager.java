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
package org.reaktivity.reaktor.internal.memory;

import java.util.function.LongSupplier;

import org.reaktivity.nukleus.buffer.MemoryManager;

public final class CountingMemoryManager implements MemoryManager
{
    private final MemoryManager memoryManager;
    private final LongSupplier acquires;
    private final LongSupplier releases;

    public CountingMemoryManager(
        MemoryManager memoryManager,
        LongSupplier acquires,
        LongSupplier releases)
    {
        this.memoryManager = memoryManager;
        this.acquires = acquires;
        this.releases = releases;
    }

    @Override
    public long acquire(
        int capacity)
    {
        final long address = memoryManager.acquire(capacity);

        if (address >= 0)
        {
            acquires.getAsLong();
        }

        return address;
    }

    @Override
    public long resolve(
        long address)
    {
        return memoryManager.resolve(address);
    }

    @Override
    public void release(
        long address,
        int capacity)
    {
        memoryManager.release(address, capacity);

        if (address >= 0)
        {
            releases.getAsLong();
        }
    }
}
