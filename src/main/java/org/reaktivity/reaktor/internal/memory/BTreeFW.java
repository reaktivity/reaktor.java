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

import static java.lang.Integer.highestOneBit;
import static java.lang.Integer.numberOfTrailingZeros;
import static org.reaktivity.reaktor.internal.layouts.MemoryLayout.BITS_PER_BTREE_NODE;
import static org.reaktivity.reaktor.internal.layouts.MemoryLayout.BITS_PER_BTREE_NODE_SHIFT;
import static org.reaktivity.reaktor.internal.layouts.MemoryLayout.BITS_PER_BYTE_SHIFT;
import static org.reaktivity.reaktor.internal.layouts.MemoryLayout.MASK_PER_BTREE_NODE;

import org.agrona.MutableDirectBuffer;

class BTreeFW
{
    static final int EMPTY = 0x00;
    static final int FULL = 0x01;
    static final int SPLIT = 0x02;

    private final int maximumNodeOffset;

    private MutableDirectBuffer buffer;
    private int offset;

    private int nodeOffset;

    BTreeFW(
        int maximumOrder)
    {
        this.maximumNodeOffset = (1 << (maximumOrder + 1)) - 1;
    }

    public BTreeFW wrap(
        MutableDirectBuffer buffer,
        int offset,
        int length)
    {
        this.buffer = buffer;
        this.offset = offset;

        this.nodeOffset = 1;

        return this;
    }

    public BTreeFW walk(
        int nodeIndex)
    {
        if (nodeIndex < 0 || nodeIndex + 1 > maximumNodeOffset)
        {
            throw new IllegalArgumentException("node not found");
        }

        nodeOffset = nodeIndex + 1;
        return this;
    }

    public int leftIndex()
    {
        if (nodeOffset == maximumNodeOffset)
        {
            throw new IllegalStateException("already at leaf");
        }

        return (nodeOffset << 1) - 1;
    }

    public int rightIndex()
    {
        if (nodeOffset == maximumNodeOffset)
        {
            throw new IllegalStateException("already at leaf");
        }

        return ((nodeOffset << 1) | 0x01) - 1;
    }

    public int parentIndex()
    {
        if (nodeOffset == 1)
        {
            throw new IllegalStateException("already at root");
        }

        return (nodeOffset >> 1) - 1;
    }

    public int siblingIndex()
    {
        if (nodeOffset == 1)
        {
            throw new IllegalStateException("root has no sibling");
        }

        return nodeOffset + 1 - 1;
    }

    public BTreeFW set(
        int flags)
    {
        if (numberOfTrailingZeros(highestOneBit(flags)) >= BITS_PER_BTREE_NODE)
        {
            throw new IllegalStateException("invalid flags");
        }

        final int bitOffset = nodeOffset << BITS_PER_BTREE_NODE_SHIFT;
        final int byteOffset = bitOffset >> BITS_PER_BYTE_SHIFT;
        final int byteShift = bitOffset & (Byte.SIZE - 1);
        final int byteValue = buffer.getByte(offset + byteOffset) & 0xff;
        final int newMask = (flags & MASK_PER_BTREE_NODE) << byteShift;
        final byte newByteValue = (byte) (byteValue | newMask);
        buffer.putByte(offset + byteOffset, newByteValue);
        return this;
    }

    public BTreeFW clear(
        int flags)
    {
        if (numberOfTrailingZeros(highestOneBit(flags)) >= BITS_PER_BTREE_NODE)
        {
            throw new IllegalStateException("invalid flags");
        }

        final int bitOffset = nodeOffset << BITS_PER_BTREE_NODE_SHIFT;
        final int byteOffset = bitOffset >> BITS_PER_BYTE_SHIFT;
        final int byteShift = bitOffset & (Byte.SIZE - 1);
        final int byteValue = buffer.getByte(offset + byteOffset) & 0xff;
        final int newMask = (flags & MASK_PER_BTREE_NODE) << byteShift;
        final byte newByteValue = (byte) (byteValue & ~newMask);
        buffer.putByte(offset + byteOffset, newByteValue);
        return this;
    }

    public boolean flag(
        int flag)
    {
        return (flag & flags()) == flag;
    }

    public int flags()
    {
        return flags(nodeOffset - 1);
    }

    public int order()
    {
        return numberOfTrailingZeros(highestOneBit(maximumNodeOffset)) - numberOfTrailingZeros(highestOneBit(nodeOffset));
    }

    public int index()
    {
        return nodeOffset - 1;
    }

    public boolean isLeftChild()
    {
        return (nodeOffset & 0x01) == 0x00;
    }

    public boolean isRightChild()
    {
        return (nodeOffset & 0x01) == 0x01 && nodeOffset != 1;
    }

    public boolean flag(
        int nodeIndex,
        int flag)
    {
        return (flags(nodeIndex) & flag) == flag;
    }

    public int flags(
        int nodeIndex)
    {
        final int nodeOffset = nodeIndex + 1;
        final int bitOffset = nodeOffset << BITS_PER_BTREE_NODE_SHIFT;
        final int byteOffset = bitOffset >> BITS_PER_BYTE_SHIFT;
        final int byteShift = bitOffset & (Byte.SIZE - 1);
        final int byteValue = buffer.getByte(offset + byteOffset) & 0xff;
        return (byteValue >> byteShift) & MASK_PER_BTREE_NODE;
    }

    @Override
    public String toString()
    {
        return String.format("index=%d, order=%d flags=%01x", index(), order(), flags());
    }
}
