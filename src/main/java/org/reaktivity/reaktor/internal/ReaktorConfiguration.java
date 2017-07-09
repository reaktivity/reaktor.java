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

import static org.agrona.BitUtil.findNextPositivePowerOfTwo;

import org.reaktivity.nukleus.Configuration;
import org.reaktivity.reaktor.internal.types.stream.EndFW;

public class ReaktorConfiguration extends Configuration
{
    public static final String ABORT_STREAM_FRAME_TYPE_ID = "reaktor.abort.stream.frame.type.id";

    public static final String NUKLEUS_BUFFER_POOL_CAPACITY_PROPERTY_FORMAT = "nukleus.%s.buffer.pool.capacity";

    public static final String NUKLEUS_BUFFER_SLOT_CAPACITY_PROPERTY_FORMAT = "nukleus.%s.buffer.slot.capacity";

    public static final int ABORT_STREAM_EVENT_TYPE_ID_DEFAULT = EndFW.TYPE_ID;

    public static final int NUKLEUS_BUFFER_SLOT_CAPACITY_DEFAULT = 65536;

    private final String bufferPoolCapacityPropertyName;
    private final String bufferSlotCapacityPropertyName;

    public ReaktorConfiguration(
        Configuration config,
        String name)
    {
        super(config);

        this.bufferPoolCapacityPropertyName = String.format(NUKLEUS_BUFFER_POOL_CAPACITY_PROPERTY_FORMAT, name);
        this.bufferSlotCapacityPropertyName = String.format(NUKLEUS_BUFFER_SLOT_CAPACITY_PROPERTY_FORMAT, name);
    }

    public int bufferPoolCapacity()
    {
        return getInteger(bufferPoolCapacityPropertyName, this::calculateBufferPoolCapacity);
    }

    public int bufferSlotCapacity()
    {
        return getInteger(bufferSlotCapacityPropertyName, NUKLEUS_BUFFER_SLOT_CAPACITY_DEFAULT);
    }

    public int abortStreamEventTypeId()
    {
        return getInteger(ABORT_STREAM_FRAME_TYPE_ID, ABORT_STREAM_EVENT_TYPE_ID_DEFAULT);
    }

    private int calculateBufferPoolCapacity()
    {
        final int maximumStreamsCount = maximumStreamsCount();
        final int maximumBufferedStreams = maximumStreamsCount < 1024 ? maximumStreamsCount : maximumStreamsCount / 8;
        return findNextPositivePowerOfTwo(bufferSlotCapacity() * maximumBufferedStreams / 2);
    }
}
