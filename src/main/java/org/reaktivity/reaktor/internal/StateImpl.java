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

import java.util.function.LongSupplier;
import java.util.function.Supplier;

import org.reaktivity.nukleus.buffer.BufferPool;
import org.reaktivity.reaktor.internal.buffer.DefaultBufferPool;

public final class StateImpl implements State, Comparable<StateImpl>
{
    private final int index;
    private final LongSupplier supplyStreamId;
    private final Supplier<BufferPool> supplyBufferPool;
    private final LongSupplier supplyTrace;
    private final LongSupplier supplyGroupId;

    public StateImpl(
        ReaktorConfiguration config,
        int index)
    {
        final int bufferPoolCapacity = config.bufferPoolCapacity();
        final int bufferSlotCapacity = config.bufferSlotCapacity();
        final BufferPool bufferPool = new DefaultBufferPool(bufferPoolCapacity, bufferSlotCapacity);

        final long mask = index << 60;
        final long[] streamId = new long[1];
        final long[] groupId = new long[1];
        final long[] traceId = new long[1];

        this.index = index;
        this.supplyBufferPool = () -> bufferPool;
        this.supplyStreamId = () -> ++streamId[0] | mask;
        this.supplyTrace = () -> ++traceId[0] | mask;
        this.supplyGroupId = () -> ++groupId[0] | mask;
    }

    @Override
    public LongSupplier supplyStreamId()
    {
        return supplyStreamId;
    }

    @Override
    public Supplier<BufferPool> supplyBufferPool()
    {
        return supplyBufferPool;
    }

    @Override
    public LongSupplier supplyTrace()
    {
        return supplyTrace;
    }

    @Override
    public LongSupplier supplyGroupId()
    {
        return supplyGroupId;
    }

    @Override
    public int hashCode()
    {
        return index;
    }

    @Override
    public boolean equals(
        Object obj)
    {
        if (this == obj)
        {
            return true;
        }

        if (!(obj instanceof StateImpl))
        {
            return false;
        }

        StateImpl that = (StateImpl)obj;
        return this.index == that.index;
    }

    @Override
    public int compareTo(
        StateImpl that)
    {
        return this.index - that.index;
    }
}
