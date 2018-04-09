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

import static java.lang.Integer.numberOfTrailingZeros;
import static org.agrona.BitUtil.findNextPositivePowerOfTwo;

import java.util.ArrayList;
import java.util.List;

import org.reaktivity.nukleus.Controller;
import org.reaktivity.nukleus.Nukleus;
import org.reaktivity.nukleus.buffer.BufferPool;
import org.reaktivity.reaktor.internal.buffer.DefaultBufferPool;

public final class StateImpl implements State, Comparable<StateImpl>
{
    private final int index;
    private final BufferPool bufferPool;
    private final long mask;
    private final List<Nukleus> nuklei;
    private final List<Controller> controllers;

    private long streamId;
    private long traceId;
    private long groupId;

    public StateImpl(
        int index,
        int count,
        ReaktorConfiguration config)
    {
        final int bufferPoolCapacity = config.bufferPoolCapacity();
        final int bufferSlotCapacity = config.bufferSlotCapacity();
        final BufferPool bufferPool = new DefaultBufferPool(bufferPoolCapacity, bufferSlotCapacity);

        final int reserved = numberOfTrailingZeros(findNextPositivePowerOfTwo(count));
        final int bits = Long.SIZE - reserved;
        final long initial = ((long) index) << bits;
        final long mask = initial | (-1L >>> reserved);

        this.index = index;
        this.mask = mask;
        this.bufferPool = bufferPool;
        this.streamId = initial;
        this.traceId = initial;
        this.groupId = initial;
        this.nuklei = new ArrayList<>();
        this.controllers = new ArrayList<>();
    }

    @Override
    public BufferPool bufferPool()
    {
        return bufferPool;
    }

    @Override
    public long supplyStreamId()
    {
        streamId++;
        streamId &= mask;
        return streamId;
    }

    @Override
    public long supplyTrace()
    {
        traceId++;
        traceId &= mask;
        return traceId;
    }

    @Override
    public long supplyGroupId()
    {
        groupId++;
        groupId &= mask;
        return groupId;
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

    public void assign(
        Nukleus nukleus)
    {
        nuklei.add(nukleus);
    }

    public List<? extends Nukleus> nuklei()
    {
        return nuklei;
    }

    public void assign(
        Controller controller)
    {
        controllers.add(controller);
    }

    public List<? extends Controller> controllers()
    {
        return controllers;
    }
}
