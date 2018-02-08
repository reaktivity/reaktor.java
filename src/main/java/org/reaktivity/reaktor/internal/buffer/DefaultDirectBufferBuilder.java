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
package org.reaktivity.reaktor.internal.buffer;

import static java.nio.ByteBuffer.allocateDirect;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.ArrayUtil;
import org.agrona.concurrent.UnsafeBuffer;
import org.reaktivity.nukleus.buffer.DirectBufferBuilder;

public final class DefaultDirectBufferBuilder implements DirectBufferBuilder
{
    private static final byte[] EMPTY_BYTE_ARRAY = new byte[0];

    private final DirectBuffer singleton = new UnsafeBuffer(EMPTY_BYTE_ARRAY);
    private final MutableDirectBuffer scratch = new UnsafeBuffer(allocateDirect(64 * 1024));

    private DirectBuffer[] buffers = new DirectBuffer[0];
    private int limit;

    @Override
    public DirectBufferBuilder wrap(
        long address,
        int length)
    {
        if (limit == buffers.length)
        {
            buffers = ArrayUtil.add(buffers, new UnsafeBuffer(EMPTY_BYTE_ARRAY));
        }

        buffers[limit++].wrap(address, length);

        return this;
    }

    @Override
    public DirectBufferBuilder wrap(
        DirectBuffer buffer)
    {
        if (limit == buffers.length)
        {
            buffers = ArrayUtil.add(buffers, new UnsafeBuffer(EMPTY_BYTE_ARRAY));
        }

        buffers[limit++].wrap(buffer);

        return this;
    }

    @Override
    public DirectBufferBuilder wrap(
        DirectBuffer buffer,
        int offset,
        int length)
    {
        if (limit == buffers.length)
        {
            buffers = ArrayUtil.add(buffers, new UnsafeBuffer(EMPTY_BYTE_ARRAY));
        }

        buffers[limit++].wrap(buffer, offset, length);

        return this;
    }

    @Override
    public DirectBuffer build()
    {
        if (limit == 0)
        {
            singleton.wrap(EMPTY_BYTE_ARRAY);
        }
        else
        {
            if (limit == 1)
            {
                singleton.wrap(buffers[0]);
            }
            else
            {
                int position = 0;
                for(int i=0; i < limit; i++)
                {
                    scratch.putBytes(position, buffers[i], 0, buffers[i].capacity());
                    position += buffers[i].capacity();
                }

                singleton.wrap(scratch, 0, position);
            }

            while (limit > 0)
            {
                buffers[--limit].wrap(EMPTY_BYTE_ARRAY);
            }
        }

        assert limit == 0;

        return singleton;
    }
}
