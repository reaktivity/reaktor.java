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

import static java.lang.Integer.numberOfTrailingZeros;
import static java.lang.Math.max;
import static org.agrona.BitUtil.findNextPositivePowerOfTwo;

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

    private final BTreeFlyweight btreeRO;

    private final int minimumBlockSize;
    private final int maximumBlockSize;

    private final MutableDirectBuffer memoryBuffer;
    private final AtomicBuffer metadataBuffer;

    public DefaultMemoryManager(MemoryLayout memoryLayout)
    {
        this.memoryBuffer = memoryLayout.memoryBuffer();
        this.metadataBuffer = memoryLayout.metadataBuffer();
        this.minimumBlockSize = memoryLayout.minimumBlockSize();
        this.maximumBlockSize = memoryLayout.maximumBlockSize();
        this.btreeRO = new BTreeFlyweight(minimumBlockSize, maximumBlockSize, MemoryLayout.BTREE_OFFSET);
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
        long offset,
        int capacity)
    {
        lock();
        try
        {
            release0(offset, capacity);
        }
        finally
        {
            unlock();
        }
    }

    private long acquire0(int capacity)
    {
        if (capacity > this.maximumBlockSize)
        {
            return -1;
        }
        int requestedBlockSize = calculateBlockSize(capacity);

        BTreeFlyweight node = root();
        while (!(requestedBlockSize == node.blockSize() && node.isFree()))
        {
            if (requestedBlockSize > node.blockSize() || node.isFull())
            {
                while(node.isRightChild())
                {
                    node = node.walkParent();
                }
                if(node.isLeftChild())
                {
                    node = node.walkToRightSibling();
                }
                else
                {
                    break; // you are root
                }
            }
            else
            {
                node = node.walkLeftChild();
            }
        }

        if (node.isFull())
        {
            return -1;
        }

        node.fill();

        long addressOffset = node.indexInOrder() * node.blockSize();

        while (!node.isRoot())
        {
            node = node.walkParent();
            if (node.isLeftFull() && node.isRightFull())
            {
                node.fill();
            }
            else if (node.isSplit())
            {
                break;
            }
            node.split();
        }
        return addressOffset;
    }

    public void release0(
        long offset,
        int capacity)
    {
        final int blockSize = calculateBlockSize(capacity);
        final int order = calculateOrder(blockSize);
        final int orderSize = 0x01 << order;
        final int entryIndex = orderSize - 1 + (int) (offset / blockSize);
        BTreeFlyweight node = btreeRO.wrap(metadataBuffer, entryIndex);
        node.empty();
        while (!node.isRoot())
        {
            node = node.walkParent();
            if(!node.isRightFullOrSplit() && !node.isLeftFullOrSplit())
            {
                node.empty();
            }
            else
            {
                node.free();
            }
        }
    }

    private BTreeFlyweight root()
    {
        return btreeRO.wrap(metadataBuffer, 0);
    }

    private int calculateOrder(int blockSize)
    {
        return max(numberOfTrailingZeros(maximumBlockSize) - numberOfTrailingZeros(blockSize), 0);
    }

    private int calculateBlockSize(
        int size)
    {
        return findNextPositivePowerOfTwo(size);
    }

    public boolean released()
    {
        return root().isFree();
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
