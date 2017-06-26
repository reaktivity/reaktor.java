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
package org.reaktivity.reaktor.internal.buffer;

import static org.agrona.BitUtil.isPowerOfTwo;

import java.nio.ByteBuffer;
import java.util.BitSet;

import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Hashing;
import org.agrona.concurrent.UnsafeBuffer;
import org.reaktivity.nukleus.buffer.BufferPool;

/**
 * A chunk of shared memory for temporary storage of data. This is logically segmented into a set of
 * slots of equal size. Methods are provided for acquiring a slot, getting a slabBuffer that can be used
 * to store data in it, and releasing the slot once it is no longer needed.
 * <b>Each instance of this class is assumed to be used by one and only one thread.</b>
 */
public class Slab implements BufferPool
{
    private final MutableDirectBuffer slotBuffer = new UnsafeBuffer(new byte[0]);

    private final int slotCapacity;
    private final int bitsPerSlot;
    private final int mask;
    private final MutableDirectBuffer slabBuffer;
    private final ByteBuffer slotByteBuffer;
    private final BitSet used;
    private final int[] availableSlots;

    public Slab(int totalCapacity, int slotCapacity)
    {
        if (!isPowerOfTwo(totalCapacity))
        {
            throw new IllegalArgumentException("totalCapacity is not a power of 2");
        }
        if (!isPowerOfTwo(slotCapacity))
        {
            throw new IllegalArgumentException("slotCapacity is not a power of 2");
        }
        if (slotCapacity > totalCapacity)
        {
            throw new IllegalArgumentException("slotCapacity exceeds totalCapacity");
        }
        this.slotCapacity = slotCapacity;
        this.bitsPerSlot = Integer.numberOfTrailingZeros(slotCapacity);
        int totalSlots = totalCapacity / slotCapacity;
        this.mask = totalSlots - 1;
        this.slabBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(totalCapacity));
        this.slotByteBuffer = slabBuffer.byteBuffer().duplicate();
        this.used = new BitSet(totalSlots);
        this.availableSlots = new int[] { totalSlots };
    }

    @Override
    public int acquire(long streamId)
    {
        if (availableSlots[0] == 0)
        {
            return NO_SLOT;
        }
        int slot = Hashing.hash(streamId, mask);
        while (used.get(slot))
        {
            slot = ++slot & mask;
        }
        used.set(slot);
        availableSlots[0]--;

        return slot;
    }

    @Override
    public MutableDirectBuffer buffer(int slot)
    {
        assert used.get(slot);
        final long slotAddressOffset = slabBuffer.addressOffset() + (slot << bitsPerSlot);
        slotBuffer.wrap(slotAddressOffset, slotCapacity);
        return slotBuffer;
    }

    @Override
    public ByteBuffer byteBuffer(int slot)
    {
        assert used.get(slot);
        final int slotOffset = slot << bitsPerSlot;
        slotByteBuffer.clear();
        slotByteBuffer.position(slotOffset);
        slotByteBuffer.limit(slotOffset + slotCapacity);
        return slotByteBuffer;
    }

    @Override
    public MutableDirectBuffer buffer(int slot, int offset)
    {
        assert used.get(slot);
        final long slotAddressOffset = slabBuffer.addressOffset() + (slot << bitsPerSlot);
        slotBuffer.wrap(slotAddressOffset + offset, slotCapacity);
        return slotBuffer;
    }

    /**
     * Releases a slot so it may be used by other streams
     * @param slot - Id of a previously acquired slot
     */
    @Override
    public void release(int slot)
    {
        assert used.get(slot);
        used.clear(slot);
        availableSlots[0]++;
    }

    @Override
    public BufferPool duplicate()
    {
        return new Slab(this);
    }

    private Slab(
        Slab that)
    {
        this.availableSlots = that.availableSlots;
        this.bitsPerSlot = that.bitsPerSlot;
        this.mask = that.mask;
        this.slabBuffer = that.slabBuffer;
        this.slotCapacity = that.slotCapacity;
        this.used = that.used;
        this.slotByteBuffer = that.slotByteBuffer.duplicate();
    }
}
