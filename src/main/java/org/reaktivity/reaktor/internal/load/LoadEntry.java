/**
 * Copyright 2016-2021 The Reaktivity Project
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
package org.reaktivity.reaktor.internal.load;

import static org.agrona.BitUtil.SIZE_OF_INT;
import static org.agrona.BitUtil.SIZE_OF_LONG;

import org.agrona.BitUtil;
import org.agrona.concurrent.AtomicBuffer;
import org.reaktivity.reaktor.internal.stream.NamespacedId;

public final class LoadEntry
{
    private static final int OFFSET_NAMESPACE_ID = 0;
    private static final int SIZE_OF_NAMESPACE_ID = SIZE_OF_INT;

    private static final int OFFSET_ENTRY_ID = OFFSET_NAMESPACE_ID + SIZE_OF_NAMESPACE_ID;
    private static final int SIZE_OF_ENTRY_ID = SIZE_OF_INT;

    private static final int OFFSET_INITIAL_OPENS = OFFSET_ENTRY_ID + SIZE_OF_ENTRY_ID;
    private static final int SIZE_OF_INITIAL_OPENS = SIZE_OF_LONG;

    private static final int OFFSET_INITIAL_CLOSES = OFFSET_INITIAL_OPENS + SIZE_OF_INITIAL_OPENS;
    private static final int SIZE_OF_INITIAL_CLOSES = SIZE_OF_LONG;

    private static final int OFFSET_INITIAL_BYTES = OFFSET_INITIAL_CLOSES + SIZE_OF_INITIAL_CLOSES;
    private static final int SIZE_OF_INITIAL_BYTES = SIZE_OF_LONG;

    private static final int OFFSET_INITIAL_ERRORS = OFFSET_INITIAL_BYTES + SIZE_OF_INITIAL_BYTES;
    private static final int SIZE_OF_INITIAL_ERRORS = SIZE_OF_LONG;

    private static final int OFFSET_REPLY_OPENS = OFFSET_INITIAL_ERRORS + SIZE_OF_INITIAL_ERRORS;
    private static final int SIZE_OF_REPLY_OPENS = SIZE_OF_LONG;

    private static final int OFFSET_REPLY_CLOSES = OFFSET_REPLY_OPENS + SIZE_OF_REPLY_OPENS;
    private static final int SIZE_OF_REPLY_CLOSES = SIZE_OF_LONG;

    private static final int OFFSET_REPLY_BYTES = OFFSET_REPLY_CLOSES + SIZE_OF_REPLY_CLOSES;
    private static final int SIZE_OF_REPLY_BYTES = SIZE_OF_LONG;

    private static final int OFFSET_REPLY_ERRORS = OFFSET_REPLY_BYTES + SIZE_OF_REPLY_BYTES;
    private static final int SIZE_OF_REPLY_ERRORS = SIZE_OF_LONG;

    private static final int SIZE_OF = OFFSET_REPLY_ERRORS + SIZE_OF_REPLY_ERRORS;
    private static final int SIZE_OF_ALIGNED = BitUtil.align(SIZE_OF, 16);

    private final AtomicBuffer buffer;
    private final int offset;

    public LoadEntry(
        AtomicBuffer buffer,
        int offset)
    {
        this.buffer = buffer;
        this.offset = offset;
    }

    public long namespacedId()
    {
        return NamespacedId.id(namespaceId(), entryId());
    }

    public int namespaceId()
    {
        return buffer.getInt(offset + OFFSET_NAMESPACE_ID);
    }

    public int entryId()
    {
        return buffer.getInt(offset + OFFSET_ENTRY_ID);
    }

    public long initialOpens()
    {
        return buffer.getLong(offset + OFFSET_INITIAL_OPENS);
    }

    public long initialCloses()
    {
        return buffer.getLong(offset + OFFSET_INITIAL_CLOSES);
    }

    public LoadEntry initialOpened(
        long count)
    {
        assert count >= 0;
        buffer.getAndAddLong(offset + OFFSET_INITIAL_OPENS, count);
        return this;
    }

    public LoadEntry initialClosed(
        long count)
    {
        assert count >= 0;
        buffer.getAndAddLong(offset + OFFSET_INITIAL_CLOSES, count);
        return this;
    }

    public long initialBytes()
    {
        return buffer.getLong(offset + OFFSET_INITIAL_BYTES);
    }

    public LoadEntry initialBytesRead(
        long count)
    {
        assert count >= 0;
        buffer.getAndAddLong(offset + OFFSET_INITIAL_BYTES, count);
        return this;
    }

    public long initialErrors()
    {
        return buffer.getLong(offset + OFFSET_INITIAL_ERRORS);
    }

    public LoadEntry initialErrored(
        long count)
    {
        assert count >= 0;
        buffer.getAndAddLong(offset + OFFSET_INITIAL_ERRORS, count);
        return this;
    }

    public long replyOpens()
    {
        return buffer.getLong(offset + OFFSET_REPLY_OPENS);
    }

    public long replyCloses()
    {
        return buffer.getLong(offset + OFFSET_REPLY_CLOSES);
    }

    public LoadEntry replyOpened(
        long count)
    {
        assert count >= 0;
        buffer.getAndAddLong(offset + OFFSET_REPLY_OPENS, count);
        return this;
    }

    public LoadEntry replyClosed(
        long count)
    {
        assert count >= 0;
        buffer.getAndAddLong(offset + OFFSET_REPLY_CLOSES, count);
        return this;
    }

    public long replyBytes()
    {
        return buffer.getLong(offset + OFFSET_REPLY_BYTES);
    }

    public LoadEntry replyBytesWritten(
        long count)
    {
        assert count >= 0;
        buffer.getAndAddLong(offset + OFFSET_REPLY_BYTES, count);
        return this;
    }

    public long replyErrors()
    {
        return buffer.getLong(offset + OFFSET_REPLY_ERRORS);
    }

    public LoadEntry replyErrored(
        long count)
    {
        assert count >= 0;
        buffer.getAndAddLong(offset + OFFSET_REPLY_ERRORS, count);
        return this;
    }

    public static int sizeofAligned()
    {
        return SIZE_OF_ALIGNED;
    }

    public void init(
        long namespacedId)
    {
        buffer.setMemory(offset, offset + SIZE_OF_ALIGNED, (byte) 0);
        buffer.putInt(offset + OFFSET_NAMESPACE_ID, NamespacedId.namespaceId(namespacedId));
        buffer.putInt(offset + OFFSET_ENTRY_ID, NamespacedId.localId(namespacedId));
    }
}
