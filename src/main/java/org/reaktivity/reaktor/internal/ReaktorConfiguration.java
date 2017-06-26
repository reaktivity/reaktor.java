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

public class ReaktorConfiguration extends Configuration
{
    public static final String NUKLEUS_BUFFER_POOL_SIZE_PROPERTY_FORMAT = "nukleus.%s.buffer.pool.size";

    public static final String NUKLEUS_BUFFER_SLOT_SIZE_PROPERTY_FORMAT = "nukleus.%s.buffer.slot.size";

    public static final int NUKLEUS_BUFFER_SLOT_SIZE_DEFAULT = 65536;

    private final String bufferPoolSizePropertyName;
    private final String bufferSlotSizePropertyName;

    public ReaktorConfiguration(
        Configuration config,
        String name)
    {
        super(config);

        this.bufferPoolSizePropertyName = String.format(NUKLEUS_BUFFER_POOL_SIZE_PROPERTY_FORMAT, name);
        this.bufferSlotSizePropertyName = String.format(NUKLEUS_BUFFER_SLOT_SIZE_PROPERTY_FORMAT, name);
    }

    public int bufferPoolSize()
    {
        return getInteger(bufferPoolSizePropertyName, this::calculateBufferPoolSize);
    }

    public int bufferSlotSize()
    {
        return getInteger(bufferSlotSizePropertyName, NUKLEUS_BUFFER_SLOT_SIZE_DEFAULT);
    }

    private int calculateBufferPoolSize()
    {
        final int maximumStreamsCount = maximumStreamsCount();
        final int maximumBufferedStreams = maximumStreamsCount < 1024 ? maximumStreamsCount : maximumStreamsCount / 8;
        return findNextPositivePowerOfTwo(bufferSlotSize() * maximumBufferedStreams / 2);
    }
}
