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
package org.reaktivity.reaktor.internal.layouts;

import static java.lang.Integer.numberOfTrailingZeros;
import static java.lang.Math.max;
import static org.agrona.BitUtil.CACHE_LINE_LENGTH;
import static org.agrona.BitUtil.align;
import static org.agrona.BitUtil.isPowerOfTwo;
import static org.agrona.IoUtil.createEmptyFile;
import static org.agrona.IoUtil.mapExistingFile;
import static org.agrona.IoUtil.unmap;

import java.io.File;
import java.nio.MappedByteBuffer;
import java.nio.file.Path;

import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.AtomicBuffer;
import org.agrona.concurrent.UnsafeBuffer;

public final class MemoryLayout extends Layout
{
    public static final int BITS_PER_BYTE_SHIFT = numberOfTrailingZeros(Byte.SIZE);

    public static final int BITS_PER_BTREE_NODE_SHIFT = 1;
    public static final int BITS_PER_BTREE_NODE = 1 << BITS_PER_BTREE_NODE_SHIFT;
    public static final int MASK_PER_BTREE_NODE = (1 << BITS_PER_BTREE_NODE) - 1;

    public static final int LOCK_OFFSET = 0;
    public static final int LOCK_SIZE = Long.BYTES;
    public static final int MINIMUM_BLOCK_SIZE_OFFSET = LOCK_OFFSET + LOCK_SIZE;
    public static final int MINIMUM_BLOCK_SIZE_SIZE = Integer.BYTES;
    public static final int MAXIMUM_BLOCK_SIZE_OFFSET = MINIMUM_BLOCK_SIZE_OFFSET + MINIMUM_BLOCK_SIZE_SIZE;
    public static final int MAXIMUM_BLOCK_SIZE_SIZE = Integer.BYTES;
    public static final int BTREE_OFFSET = MAXIMUM_BLOCK_SIZE_OFFSET + MAXIMUM_BLOCK_SIZE_SIZE;

    private final AtomicBuffer metadataBuffer;
    private final MutableDirectBuffer memoryBuffer;

    private MemoryLayout(
        AtomicBuffer metadataBuffer,
        MutableDirectBuffer memoryBuffer)
    {
        this.metadataBuffer = metadataBuffer;
        this.memoryBuffer = memoryBuffer;
    }

    @Override
    public void close()
    {
        unmap(metadataBuffer.byteBuffer());
        unmap(memoryBuffer.byteBuffer());
    }

    public AtomicBuffer metadataBuffer()
    {
        return metadataBuffer;
    }

    public MutableDirectBuffer memoryBuffer()
    {
        return memoryBuffer;
    }

    public int minimumBlockSize()
    {
        return metadataBuffer.getInt(MINIMUM_BLOCK_SIZE_OFFSET);
    }

    public int maximumBlockSize()
    {
        return metadataBuffer.getInt(MAXIMUM_BLOCK_SIZE_OFFSET);
    }

    public int capacity()
    {
        return memoryBuffer.capacity();
    }

    public static final class Builder extends Layout.Builder<MemoryLayout>
    {
        private Path path;
        private int capacity;
        private int minimumBlockSize;
        private int maximumBlockSize;
        private boolean create;

        public Builder path(
            Path path)
        {
            this.path = path;
            return this;
        }

        public Builder create(
            boolean create)
        {
            this.create = create;
            return this;
        }

        public Builder minimumBlockSize(
            int minimumBlockSize)
        {
            if (!isPowerOfTwo(minimumBlockSize))
            {
                throw new IllegalArgumentException("minimum block size MUST be a power of 2");
            }

            this.minimumBlockSize = minimumBlockSize;
            return this;
        }

        public Builder maximumBlockSize(
            int maximumBlockSize)
        {
            if (!isPowerOfTwo(maximumBlockSize))
            {
                throw new IllegalArgumentException("maximum block size MUST be a power of 2");
            }

            this.maximumBlockSize = maximumBlockSize;
            this.capacity = maximumBlockSize;
            return this;
        }

        @Override
        public MemoryLayout build()
        {
            final File memory = path.toFile();

            if (create)
            {
                final int metadataSize = BTREE_OFFSET + sizeofBTree(minimumBlockSize, maximumBlockSize);
                final int metadataSizeAligned = align(metadataSize, CACHE_LINE_LENGTH);
                CloseHelper.close(createEmptyFile(memory, metadataSizeAligned + capacity));

                final MappedByteBuffer mappedMetadata = mapExistingFile(memory, "metadata", 0, metadataSizeAligned);
                final MappedByteBuffer mappedMemory = mapExistingFile(memory, "memory", metadataSizeAligned, capacity);

                final AtomicBuffer metadataBuffer = new UnsafeBuffer(mappedMetadata, 0, metadataSize);
                final MutableDirectBuffer memoryBuffer = new UnsafeBuffer(mappedMemory);

                metadataBuffer.putInt(MINIMUM_BLOCK_SIZE_OFFSET, minimumBlockSize);
                metadataBuffer.putInt(MAXIMUM_BLOCK_SIZE_OFFSET, maximumBlockSize);
                return new MemoryLayout(metadataBuffer, memoryBuffer);
            }
            else
            {
                final MappedByteBuffer mappedBootstrap = mapExistingFile(memory, "bootstrap", 0, BTREE_OFFSET);
                final DirectBuffer bootstrapBuffer = new UnsafeBuffer(mappedBootstrap);
                final int minimumBlockSize = bootstrapBuffer.getInt(MINIMUM_BLOCK_SIZE_OFFSET);
                final int maximumBlockSize = bootstrapBuffer.getInt(MAXIMUM_BLOCK_SIZE_OFFSET);
                unmap(mappedBootstrap);

                final int metadataSize = BTREE_OFFSET + sizeofBTree(minimumBlockSize, maximumBlockSize);
                final int metadataSizeAligned = align(metadataSize, CACHE_LINE_LENGTH);
                final int capacity = (int) memory.length() - metadataSizeAligned;

                final MappedByteBuffer mappedMetadata = mapExistingFile(memory, "metadata", 0, metadataSize);
                final MappedByteBuffer mappedMemory = mapExistingFile(memory, "memory", metadataSizeAligned, capacity);

                final AtomicBuffer metadataBuffer = new UnsafeBuffer(mappedMetadata);
                final MutableDirectBuffer memoryBuffer = new UnsafeBuffer(mappedMemory);

                return new MemoryLayout(metadataBuffer, memoryBuffer);
            }
        }

        private static int sizeofBTree(
            int minimumBlockSize,
            int maximumBlockSize)
        {
            int orderCount = numberOfTrailingZeros(maximumBlockSize) - numberOfTrailingZeros(minimumBlockSize);
            return align(max(2 << orderCount << BITS_PER_BTREE_NODE_SHIFT >> BITS_PER_BYTE_SHIFT, Byte.BYTES), Long.BYTES);
        }
    }
}
