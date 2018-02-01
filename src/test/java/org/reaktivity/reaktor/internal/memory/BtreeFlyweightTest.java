package org.reaktivity.reaktor.internal.memory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;

public class BtreeFlyweightTest
{
    private BtreeFlyweight node = new BtreeFlyweight(1024, 8, 0);

    @Test
    public void shouldWorkOnRoot()
    {
        byte[] buffer = new byte[8];
        UnsafeBuffer unsafeBuffer = new UnsafeBuffer(buffer);
        node.wrap(unsafeBuffer, 0);
        assertEquals(0, node.index());
        assertTrue(node.isEmpty());
        assertFalse(node.isSplit());
        assertFalse(node.isFull());

        node.fill();
        assertFalse(node.isEmpty());
        assertFalse(node.isSplit());
        assertTrue(node.isFull());

        node.empty();
        assertTrue(node.isEmpty());
        assertFalse(node.isSplit());
        assertFalse(node.isFull());

        node.split();
        assertTrue(node.isEmpty());
        assertTrue(node.isSplit());
        assertFalse(node.isFull());

        node.fill();
        assertFalse(node.isEmpty());
        assertTrue(node.isSplit());
        assertTrue(node.isFull());

        node.free();
        assertTrue(node.isEmpty());
        assertTrue(node.isSplit());
        assertFalse(node.isFull());

        node.fill();
        assertFalse(node.isEmpty());
        assertTrue(node.isSplit());
        assertTrue(node.isFull());

        node = node.walkLeftChild();
        assertSame(node, node);
        assertTrue(node.isEmpty());
        assertFalse(node.isSplit());
        assertFalse(node.isFull());
        assertEquals(1, node.index());

        node.walkParent();
        assertFalse(node.isEmpty());
        assertTrue(node.isSplit());
        assertTrue(node.isFull());

        // implicit unsplit if do empty
        node.split();
        node.empty();
        assertTrue(node.isEmpty());
        assertFalse(node.isSplit());
        assertFalse(node.isFull());

        assertEquals(0, node.index());

        assertEquals(1024, node.blockSize());

        node = node.walkLeftChild();
        assertEquals(512, node.blockSize());
    }

    @Test
    public void shouldWorkOnLeaf()
    {
        byte[] buffer = new byte[8];
        UnsafeBuffer unsafeBuffer = new UnsafeBuffer(buffer);
        node.wrap(unsafeBuffer, 0);
        node = node.walkLeftChild();
        assertEquals(1, node.index());
        assertTrue(node.isEmpty());
        assertFalse(node.isSplit());
        assertFalse(node.isFull());

        node.fill();
        assertFalse(node.isEmpty());
        assertFalse(node.isSplit());
        assertTrue(node.isFull());

    }

    @Test
    public void shouldRecognizeRoot()
    {
        byte[] buffer = new byte[8];
        UnsafeBuffer unsafeBuffer = new UnsafeBuffer(buffer);
        node = node.wrap(unsafeBuffer, 0);
        assertTrue(node.isRoot());
        node = node.walkLeftChild();
        assertFalse(node.isRoot());
    }

}
