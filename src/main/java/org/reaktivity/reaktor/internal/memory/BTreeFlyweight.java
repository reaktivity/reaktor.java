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

import static java.lang.Long.highestOneBit;
import static java.lang.Long.numberOfTrailingZeros;
import static org.reaktivity.reaktor.internal.memory.DefaultMemoryManager.BITS_PER_ENTRY;
import static org.reaktivity.reaktor.internal.memory.DefaultMemoryManager.BITS_PER_LONG;

import org.agrona.concurrent.UnsafeBuffer;

class BTreeFlyweight
{
    static final long EMPTY = 0x00L;
    static final long FULL = 0x01L;
    static final long SPLIT = 0x02L;
    static final long FULL_AND_SPLIT = 0x03L;
    static final long UNSPLIT_MASH = FULL;

    private final int largestBlock;
    private final int offset;

    private int entryIndex;
    private UnsafeBuffer buffer;

     BTreeFlyweight(
        int largestBlock,
        int smallestBlock,
        int offset) // TODO move offset to wrap?
    {
        this.largestBlock = largestBlock;
        this.offset = offset;
    }

    public BTreeFlyweight wrap(
        UnsafeBuffer buffer,
        int entryIndex)
    {
        this.entryIndex = entryIndex;
        this.buffer = buffer;
        return this;
    }

    long value()
    {
        final int arrayIndex = arrayIndex();
        final long longValue = buffer.getLong(arrayIndex);
        final long value = (longValue >> BITS_PER_LONG - bitOffset() - BITS_PER_ENTRY) & 0x3L;
        return value;
    }

    public BTreeFlyweight walkParent()
    {
        this.wrap(buffer, (entryIndex - 1) >> 1);
        return this;
    }

    public BTreeFlyweight walkLeftChild()
    {
        this.wrap(buffer, 2 * entryIndex + 1);
        return this;
    }

    public BTreeFlyweight walkRightChild()
    {
        this.wrap(buffer, 2 * entryIndex + 2);
        return this;
    }

    public BTreeFlyweight walkToRightSibling()
    {
        this.wrap(buffer, entryIndex + 1);
        return this;
    }

    private int order()
    {
        return numberOfTrailingZeros(highestOneBit(entryIndex + 1));
    }

    private int orderSize()
    {
        return 0x01 << order();
    }

    public int indexInOrder()
    {
        return (index() + 1) % orderSize();
    }

    public boolean isFull()
    {
        return (value() & FULL) > 0;
    }

    public boolean isFree()
    {
        return isEmpty() && !isSplit();
    }

    public boolean isEmpty()
    {
        return (value() & FULL) == 0;
    }

    public boolean isSplit()
    {
        return (value() & SPLIT) > 0;
    }

    public void split()
    {
        long newValue = shiftToEntry(SPLIT);
        buffer.putLong(arrayIndex(), buffer.getLong(arrayIndex()) | newValue);
    }

    public void free()
    {
        long newValue = ~shiftToEntry(FULL);
        buffer.putLong(arrayIndex(), buffer.getLong(arrayIndex()) & newValue);
    }

    // WARNING, this unsplits it AND releases it, not sure if you want it.
    public void empty()
    {
        final long newValueMask = ~(shiftToEntry(FULL_AND_SPLIT));
        long newValue = buffer.getLong(arrayIndex()) & newValueMask;
        buffer.putLong(arrayIndex(), newValue);
    }

    public void splitAndFill()
    {
        long newValue = 0xffffffffffffffffL ^ shiftToEntry(SPLIT | FULL);
        buffer.putLong(arrayIndex(), buffer.getLong(arrayIndex()) | newValue);
    }

    public void fill()
    {
        final long newValueMask = shiftToEntry(FULL);
        final long newValue = buffer.getLong(arrayIndex()) | newValueMask;
        buffer.putLong(arrayIndex(), newValue);
    }

    public boolean isLeftFull()
    {
        this.walkLeftChild();
        boolean result = isFull();
        this.walkParent();
        return result;
    }

    public boolean isRightFull()
    {
        this.walkRightChild();
        boolean result = isFull();
        this.walkParent();
        return result;
    }

    public boolean isLeftFullOrSplit()
    {
        this.walkLeftChild();
        boolean result = isFull() || isSplit();
        this.walkParent();
        return result;
    }

    public boolean isRightFullOrSplit()
    {
        this.walkRightChild();
        boolean result = isFull() || isSplit();
        this.walkParent();
        return result;
    }

    public int index()
    {
        return entryIndex;
    }

    private int arrayIndex()
    {
        return offset + (int) entryIndex / (BITS_PER_LONG >> (BITS_PER_ENTRY - 1));
    }

    private int bitOffset()
    {
        return (entryIndex % (BITS_PER_LONG >> (BITS_PER_ENTRY - 1))) * BITS_PER_ENTRY;
    }

    public int blockSize()
    {
        int index = entryIndex + 1;
        int size = largestBlock;
        while(index != 1)
        {
            size = size >> 1;
            index = index >> 1;
        }
        return size;
    }

    public boolean isRoot()
    {
        return entryIndex == 0;
    }

    public boolean isLeftChild()
    {
        return !isRoot() && entryIndex % 2 == 1;
    }

    public boolean isRightChild()
    {
        return !isRoot() && entryIndex % 2 == 0;
    }

    private long shiftToEntry(long v)
    {
        return v << (BITS_PER_LONG - bitOffset() - BITS_PER_ENTRY);
    }

    @Override
    public String toString()
    {
        return String.format("index=%d, blockSize=%d isFull=%b isSplit=%b", index(), blockSize(), isFull(), isSplit());
    }
}
