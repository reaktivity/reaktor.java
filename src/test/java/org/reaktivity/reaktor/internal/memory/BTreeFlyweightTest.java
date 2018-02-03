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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;

public class BTreeFlyweightTest
{
    private BTreeFlyweight node = new BTreeFlyweight(8, 1024, 0);

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
