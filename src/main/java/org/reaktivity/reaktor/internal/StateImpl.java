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
package org.reaktivity.reaktor.internal;

import static java.lang.Integer.numberOfTrailingZeros;
import static org.agrona.BitUtil.findNextPositivePowerOfTwo;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.reaktivity.nukleus.Controller;
import org.reaktivity.nukleus.buffer.BufferPool;
import org.reaktivity.reaktor.internal.agent.NukleusAgent;
import org.reaktivity.reaktor.internal.buffer.DefaultBufferPool;

public final class StateImpl implements State, Comparable<StateImpl>
{
    private final int index;
    private final BufferPool bufferPool;
    private final long mask;
    private final AtomicInteger routeId;
    private final LabelManager labels;
    private final List<NukleusAgent> nuklei;
    private final List<Controller> controllers;

    private long streamId;
    private long correlationId;
    private long traceId;
    private long groupId;

    public StateImpl(
        int index,
        int count,
        AtomicInteger routeId,
        ReaktorConfiguration config,
        LabelManager labels)
    {
        final int bufferPoolCapacity = config.bufferPoolCapacity();
        final int bufferSlotCapacity = config.bufferSlotCapacity();
        final BufferPool bufferPool = new DefaultBufferPool(bufferPoolCapacity, bufferSlotCapacity);

        final int reserved = numberOfTrailingZeros(findNextPositivePowerOfTwo(count)) + 1; // reply bit
        final int bits = Long.SIZE - reserved;
        final long initial = ((long) index) << bits;
        final long mask = initial | (-1L >>> reserved);

        assert mask >= 0; // high bit used by reply id

        this.index = index;
        this.mask = mask;
        this.bufferPool = bufferPool;
        this.streamId = initial;
        this.correlationId = initial;
        this.traceId = initial;
        this.groupId = initial;
        this.routeId = routeId;
        this.labels = labels;
        this.nuklei = new ArrayList<>();
        this.controllers = new ArrayList<>();
    }

    @Override
    public BufferPool bufferPool()
    {
        return bufferPool;
    }

    @Override
    public int supplyLabelId(
        String label)
    {
        return labels.supplyLabelId(label);
    }

    @Override
    public String lookupLabel(
        int labelId)
    {
        return labels.lookupLabel(labelId);
    }

    @Override
    public int supplyRouteId()
    {
        return routeId.incrementAndGet();
    }

    @Override
    public long supplyInitialId()
    {
        streamId++;
        streamId &= mask;
        return streamId;
    }

    @Override
    public long supplyCorrelationId()
    {
        correlationId++;
        correlationId &= mask;
        return correlationId;
    }

    @Override
    public long supplyReplyId(
        long initialId)
    {
        assert (initialId & 0x8000_0000_0000_0000L) == 0L;
        return initialId | 0x8000_0000_0000_0000L;
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
        NukleusAgent agent)
    {
        nuklei.add(agent);
    }

    public List<NukleusAgent> agents()
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
