package org.reaktivity.reaktor.internal.memory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.reaktivity.reaktor.internal.memory.DefaultMemoryManager.sizeOfMetaData;

import java.util.function.LongSupplier;

import org.agrona.collections.LongArrayList;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Rule;
import org.junit.Test;
import org.reaktivity.nukleus.buffer.MemoryManager;
import org.reaktivity.reaktor.test.ConfigureMemoryLayout;
import org.reaktivity.reaktor.test.DefaultMemoryManagerRule;


public class DefaultMemoryManagerTest
{

    private UnsafeBuffer writeBuffer = new UnsafeBuffer(new byte[1]);
    private static final int KB = 1024;
    private static final int BYTES_64 = 64;
    private static final int BYTES_128 = 128;

    @Rule
    public DefaultMemoryManagerRule memoryManagerRule = new DefaultMemoryManagerRule();


    @Test
    @ConfigureMemoryLayout(capacity = KB, smallestBlockSize = BYTES_64)
    public void shouldCalculateMetaDataSize()
    {
        assertEquals(9, sizeOfMetaData(16, 16, 4));
        assertEquals(10, sizeOfMetaData(128, 128, 4));
    }

    @Test
    @ConfigureMemoryLayout(capacity = KB, smallestBlockSize = BYTES_64)
    public void shouldAllocateAndReleaseLargestBlock()
    {
        memoryManagerRule.assertReleased();
        final MemoryManager memoryManager = memoryManagerRule.memoryManager();
        final MemoryLayout layout = memoryManagerRule.layout();
        final long baseAddressOffset = layout.memoryBuffer().addressOffset();

        long addressOffset = memoryManager.acquire(KB);
        assertEquals(baseAddressOffset, addressOffset);
        writeBuffer.wrap(addressOffset, KB);
        long expected = 0xffffffffffffffffL;
        writeBuffer.putLong(0, expected);
        long actual = writeBuffer.getLong(0);
        assertEquals(expected, actual);

        assertEquals(-1, memoryManager.acquire(KB));
        assertEquals(-1, memoryManager.acquire(KB / 2));

        memoryManagerRule.assertNotReleased();

        memoryManager.release(addressOffset, KB);
        memoryManagerRule.assertReleased();

        addressOffset = memoryManager.acquire(KB);
        writeBuffer.wrap(addressOffset, KB);
        expected = 0xffffffffffffffffL;
        writeBuffer.putLong(0, expected);
        actual = writeBuffer.getLong(0);
        assertEquals(expected, actual);
    }

    @Test
    @ConfigureMemoryLayout(capacity = KB, smallestBlockSize = BYTES_64)
    public void shouldAllocateAndReleaseSmallestBlocks()
    {
        final MemoryManager memoryManager = memoryManagerRule.memoryManager();
        final MemoryLayout layout = memoryManagerRule.layout();
        final long baseAddressOffset = layout.memoryBuffer().addressOffset();

        for (int allocateAndReleased = 2; allocateAndReleased != 0; allocateAndReleased--)
        {
            LongArrayList acquiredAddresses = new LongArrayList();
            for (int i = 0; (i * BYTES_64) < KB; i++)
            {
                long addressOffset = memoryManager.acquire(BYTES_64);
                assertEquals(baseAddressOffset + (i * BYTES_64), addressOffset);
                writeBuffer.wrap(addressOffset, BYTES_64);
                writeBuffer.putLong(0, i % BYTES_64);
                acquiredAddresses.add(addressOffset);
                memoryManagerRule.assertNotReleased();
            }
            assertEquals(KB/BYTES_64, acquiredAddresses.size());
            for (int i = 0; (i * BYTES_64) < KB; i++)
            {
                long addressOffset = acquiredAddresses.get(i);
                writeBuffer.wrap(addressOffset, BYTES_64);
                assertEquals(i % BYTES_64, writeBuffer.getLong(0));
            }
            for (int i = 0; (i * BYTES_64) < KB; i++)
            {
                long addressOffset = acquiredAddresses.get(i);
                memoryManager.release(addressOffset, BYTES_64);
            }
            memoryManagerRule.assertReleased();
        }
    }

    @Test
    @ConfigureMemoryLayout(capacity = KB, smallestBlockSize = BYTES_64)
    public void shouldAllocateAndReleaseMediumBlocks()
    {
        final MemoryManager memoryManager = memoryManagerRule.memoryManager();
        final MemoryLayout layout = memoryManagerRule.layout();
        final long baseAddressOffset = layout.memoryBuffer().addressOffset();

        for (int allocateAndReleased = 2; allocateAndReleased != 0; allocateAndReleased--)
        {
            LongArrayList acquiredAddresses = new LongArrayList();
            for (int i = 0; (i * BYTES_128) < KB; i++)
            {
                long addressOffset = memoryManager.acquire(BYTES_128);
                assertEquals(baseAddressOffset + (i * BYTES_128), addressOffset);
                writeBuffer.wrap(addressOffset, BYTES_128);
                writeBuffer.putLong(0, i % BYTES_128);
                acquiredAddresses.add(addressOffset);
                memoryManagerRule.assertNotReleased();
            }
            assertEquals(KB/BYTES_128, acquiredAddresses.size());
            for (int i = 0; (i * BYTES_128) < KB; i++)
            {
                long addressOffset = acquiredAddresses.get(i);
                writeBuffer.wrap(addressOffset, BYTES_128);
                assertEquals(i % BYTES_128, writeBuffer.getLong(0));
            }
            for (int i = 0; (i * BYTES_128) < KB; i++)
            {
                long addressOffset = acquiredAddresses.get(i);
                memoryManager.release(addressOffset, BYTES_128);
            }
            memoryManagerRule.assertReleased();
        }
    }

    @Test
    @ConfigureMemoryLayout(capacity = KB, smallestBlockSize = BYTES_64)
    public void shouldAllocateAndReleaseMixedSizeBlocks()
    {
        final MemoryManager memoryManager = memoryManagerRule.memoryManager();
        final LongArrayList acquired128Blocks = new LongArrayList();
        final LongArrayList acquired64Blocks = new LongArrayList();

        LongSupplier acquire128Address = () ->
        {
            long addressOffset = memoryManager.acquire(BYTES_128);
            if (addressOffset != -1)
            {
                acquired128Blocks.add(addressOffset);
            }
            return addressOffset;
        };

        LongSupplier acquire64Address = () ->
        {
            long addressOffset = memoryManager.acquire(BYTES_64);
            if (addressOffset != -1)
            {
                acquired64Blocks.add(addressOffset);
            }
            return addressOffset;
        };

        for (int i = 0; acquired128Blocks.size() + acquired64Blocks.size() < 12; i++)
        {
            assertNotEquals(-1, acquire128Address.getAsLong());
            assertNotEquals(-1, acquire64Address.getAsLong());
            assertNotEquals(-1, acquire64Address.getAsLong());
            acquired128Blocks.forEach(l ->
            {
                assertFalse(acquired64Blocks.contains(l));
            });
        }
        assertEquals(-1, acquire128Address.getAsLong());
        assertEquals(-1, acquire64Address.getAsLong());

        memoryManager.release(acquired128Blocks.remove(3), BYTES_128);
        assertNotEquals(-1, acquire64Address.getAsLong());
        assertNotEquals(-1, acquire64Address.getAsLong());
        assertEquals(-1, acquire128Address.getAsLong());
        assertEquals(-1, acquire64Address.getAsLong());

        memoryManager.release(acquired64Blocks.remove(3), BYTES_64);
        memoryManager.release(acquired64Blocks.remove(3), BYTES_64);
        assertNotEquals(-1, acquire128Address.getAsLong());
        assertEquals(-1, acquire128Address.getAsLong());
        assertEquals(-1, acquire64Address.getAsLong());
    }
}
