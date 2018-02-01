package org.reaktivity.reaktor.internal.memory;

import static org.agrona.IoUtil.createEmptyFile;
import static org.agrona.IoUtil.mapExistingFile;
import static org.agrona.IoUtil.unmap;

import java.io.File;
import java.nio.MappedByteBuffer;
import java.nio.file.Path;

import org.agrona.BitUtil;
import org.agrona.CloseHelper;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.reaktivity.reaktor.internal.layouts.Layout;

public final class MemoryLayout extends Layout
{

    private final UnsafeBuffer memoryBuffer;
    private final int smallestBlockSize;
    private final int largestBlockSize;
    private final int capacity;

    private MemoryLayout(
        UnsafeBuffer memoryBuffer,
        int smallestBlockSize,
        int largestBlockSize,
        int capacity)
    {
        this.memoryBuffer = memoryBuffer;
        this.smallestBlockSize = smallestBlockSize;
        this.largestBlockSize = largestBlockSize;
        this.capacity = capacity;
    }

    @Override
    public void close()
    {
        unmap(memoryBuffer().byteBuffer());
    }

    public MutableDirectBuffer memoryBuffer()
    {
        return memoryBuffer;
    }

    public int smallestBlock()
    {
        return smallestBlockSize;
    }

    public int capacity()
    {
        return capacity;
    }

    public int largestBlock()
    {
        return largestBlockSize;
    }

    public static final class Builder extends Layout.Builder<MemoryLayout>
    {
        private Path path;
        private int capacity;
        private int smallestBlockSize;
        private boolean readonly;

        public Builder path(Path path)
        {
            this.path = path;
            return this;
        }

        public Builder capacity(
            int capacity)
        {
            this.capacity = capacity;
            return this;
        }

        public Builder readonly(
            boolean readonly)
        {
            this.readonly = readonly;
            return this;
        }

        public Builder smallestBlockSize(
            int blockSize)
        {
            smallestBlockSize = blockSize;
            return this;
        }

        @Override
        public MemoryLayout build()
        {
            final File routes = path.toFile();

            if (!readonly)
            {
                CloseHelper.close(createEmptyFile(routes, capacity));
            }

            long sizeToAllocate = sizeToAllocate(capacity, capacity, smallestBlockSize);
            final MappedByteBuffer mappedMemory = mapExistingFile(routes, "memory", 0, sizeToAllocate);

            final UnsafeBuffer unsafeBuffer = new UnsafeBuffer(mappedMemory);

            return new MemoryLayout(unsafeBuffer, smallestBlockSize, capacity, capacity);
        }

        private static long sizeToAllocate(
            int capacity,
            int largestBlockSize,
            int smallestBlockSize)
        {
            int requiredSize = capacity + DefaultMemoryManager.sizeOfMetaData(
                    capacity,
                    largestBlockSize,
                    smallestBlockSize);
            return ((requiredSize + BitUtil.SIZE_OF_LONG - 1) / BitUtil.SIZE_OF_LONG) * BitUtil.SIZE_OF_LONG;
        }

    }

}
