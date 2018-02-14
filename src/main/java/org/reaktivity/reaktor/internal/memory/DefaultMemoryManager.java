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
import static org.reaktivity.reaktor.internal.layouts.MemoryLayout.BTREE_OFFSET;
import static org.reaktivity.reaktor.internal.memory.BTreeFW.EMPTY;
import static org.reaktivity.reaktor.internal.memory.BTreeFW.FULL;
import static org.reaktivity.reaktor.internal.memory.BTreeFW.SPLIT;

import java.util.Random;

import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.AtomicBuffer;
import org.agrona.hints.ThreadHints;
import org.reaktivity.nukleus.buffer.MemoryManager;
import org.reaktivity.reaktor.internal.layouts.MemoryLayout;

// NOTE, order 0 is largest in terms of size
public class DefaultMemoryManager implements MemoryManager
{
    private final long id = new Random().nextLong();

    private final BTreeFW btreeRO;

    private final int blockSizeShift;
    private final int maximumBlockSize;
    private final int maximumOrder;

    private final MutableDirectBuffer memoryBuffer;
    private final AtomicBuffer metadataBuffer;


    public DefaultMemoryManager(
        MemoryLayout memoryLayout)
    {
        final int minimumBlockSize = memoryLayout.minimumBlockSize();
        final int maximumBlockSize = memoryLayout.maximumBlockSize();

        this.memoryBuffer = memoryLayout.memoryBuffer();
        this.metadataBuffer = memoryLayout.metadataBuffer();
        this.blockSizeShift = numberOfTrailingZeros(minimumBlockSize);
        this.maximumBlockSize = maximumBlockSize;
        this.maximumOrder = numberOfTrailingZeros(maximumBlockSize) - numberOfTrailingZeros(minimumBlockSize);
        this.btreeRO = new BTreeFW().wrap(metadataBuffer, BTREE_OFFSET, metadataBuffer.capacity() - BTREE_OFFSET);
    }

    @Override
    public long resolve(
        long address)
    {
        return memoryBuffer.addressOffset() + address;
    }

    @Override
    public long acquire(
        int capacity)
    {
        lock();
        try
        {
            return acquire0(capacity);
        }
        finally
        {
            unlock();
        }
    }

    @Override
    public void release(
        long address,
        int capacity)
    {
        lock();
        try
        {
            release0(address, capacity);
        }
        finally
        {
            unlock();
        }
    }

    private long acquire0(
        int capacity)
    {
        if (capacity > this.maximumBlockSize)
        {
            return -1;
        }

        int allocationSize = Math.max(capacity >> blockSizeShift, 1) << blockSizeShift;
        int allocationOrder = numberOfTrailingZeros(allocationSize >> blockSizeShift);

        final BTreeFW node = btreeRO.walk(0);
        while (node.order() != allocationOrder || node.flag(SPLIT) || node.flag(FULL))
        {
            if (node.order() < allocationOrder || node.flag(FULL))
            {
                while (node.isRightChild())
                {
                    node.walk(node.parentIndex());
                }

                if (node.isLeftChild())
                {
                    node.walk(node.siblingIndex());
                }
                else
                {
                    break; // root
                }
            }
            else
            {
                node.walk(node.leftIndex());
            }
        }

        if (node.flag(FULL))
        {
            return -1;
        }

        node.set(FULL);

        final int nodeIndex = node.index();
        final int nodeOrder = node.order();

        while (node.order() < maximumOrder)
        {
            node.walk(node.parentIndex());

            if (node.flag(node.leftIndex(), FULL) && node.flag(node.rightIndex(), FULL))
            {
                node.clear(SPLIT);
                node.set(FULL);
            }
            else
            {
                if (node.flag(SPLIT))
                {
                    break;
                }

                node.set(SPLIT);
            }
        }

        return ((nodeIndex + 1) & ~highestOneBit(nodeIndex + 1)) << blockSizeShift << nodeOrder;
    }

    public void release0(
        long offset,
        int capacity)
    {
        final int allocationSize = Math.max(capacity >> blockSizeShift, 1) << blockSizeShift;
        final int nodeOrder = numberOfTrailingZeros(allocationSize >> blockSizeShift);
        final int nodeIndex = (((int) (offset >> nodeOrder >> blockSizeShift)) | (1 << (maximumOrder - nodeOrder))) - 1;

        final BTreeFW node = btreeRO.walk(nodeIndex);
        for (;; node.walk(node.parentIndex()))
        {
            node.clear(FULL);
            if(node.order() == 0 || (node.flags(node.leftIndex()) == EMPTY && node.flags(node.rightIndex()) == EMPTY))
            {
                node.clear(SPLIT);
            }
            else
            {
                node.set(SPLIT);
            }

            if (node.index() == 0)
            {
                 break;
            }
        }
    }

    public boolean released()
    {
        return btreeRO.walk(0).flags() == EMPTY;
    }

    private void lock()
    {
        while(!metadataBuffer.compareAndSetLong(MemoryLayout.LOCK_OFFSET, 0L, this.id))
        {
            ThreadHints.onSpinWait();
        }
    }

    private void unlock()
    {
        metadataBuffer.putLongOrdered(MemoryLayout.LOCK_OFFSET, 0L);
    }
}
