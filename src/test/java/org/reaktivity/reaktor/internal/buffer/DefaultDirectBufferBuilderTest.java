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

import static java.nio.ByteBuffer.allocate;
import static java.nio.ByteBuffer.allocateDirect;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.FromDataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;
import org.reaktivity.nukleus.buffer.DirectBufferBuilder;

@RunWith(Theories.class)
public class DefaultDirectBufferBuilderTest
{
    @DataPoints("buffers")
    public static final DirectBuffer[] BUFFERS = new DirectBuffer[]
    {
        new UnsafeBuffer(allocateDirect(128)),
        new UnsafeBuffer(allocate(128)),
        new UnsafeBuffer(new byte[128])
    };

    @DataPoints("addresses")
    public static final DirectBuffer[] ADDRESSES = new DirectBuffer[]
    {
        new UnsafeBuffer(allocateDirect(128))
    };

    @Theory
    public void shouldBuildEmpty()
    {
        DirectBufferBuilder builder = new DefaultDirectBufferBuilder();

        DirectBuffer target = builder.build();

        assertEquals(0, target.capacity());
    }

    @Theory
    public void shouldBuildOneAddress(
        @FromDataPoints("addresses") DirectBuffer source)
    {
        DirectBufferBuilder builder = new DefaultDirectBufferBuilder();

        DirectBuffer target = builder.wrap(source.addressOffset(), source.capacity())
                                     .build();

        assertEquals(source, target);
        assertEquals(source.addressOffset(), target.addressOffset());
    }

    @Theory
    public void shouldBuildTwoAddresses(
        @FromDataPoints("addresses") DirectBuffer source1,
        @FromDataPoints("addresses") DirectBuffer source2)
    {
        DirectBufferBuilder builder = new DefaultDirectBufferBuilder();

        DirectBuffer target = builder.wrap(source1.addressOffset(), source1.capacity())
                                     .wrap(source2.addressOffset(), source2.capacity())
                                     .build();

        assertEquals(source1.capacity() + source2.capacity(), target.capacity());
        assertEquals(source1, new UnsafeBuffer(target, 0, source1.capacity()));
        assertEquals(source2, new UnsafeBuffer(target, source1.capacity(), source2.capacity()));
    }

    @Theory
    public void shouldBuildThreeAddresses(
        @FromDataPoints("addresses") DirectBuffer source1,
        @FromDataPoints("addresses") DirectBuffer source2,
        @FromDataPoints("addresses") DirectBuffer source3)
    {
        DirectBufferBuilder builder = new DefaultDirectBufferBuilder();

        DirectBuffer target = builder.wrap(source1.addressOffset(), source1.capacity())
                                     .wrap(source2.addressOffset(), source2.capacity())
                                     .wrap(source3.addressOffset(), source3.capacity())
                                     .build();

        assertEquals(source1.capacity() + source2.capacity() + source3.capacity(), target.capacity());
        assertEquals(source1, new UnsafeBuffer(target, 0, source1.capacity()));
        assertEquals(source2, new UnsafeBuffer(target, source1.capacity(), source2.capacity()));
        assertEquals(source3, new UnsafeBuffer(target, source1.capacity() + source2.capacity(), source3.capacity()));
    }

    @Theory
    public void shouldBuildOneDirectBuffer(
        @FromDataPoints("buffers") DirectBuffer source)
    {
        DirectBufferBuilder builder = new DefaultDirectBufferBuilder();

        DirectBuffer target = builder.wrap(source)
                                     .build();

        assertEquals(source, target);
        assertSame(source.byteBuffer(), target.byteBuffer());
        assertSame(source.byteArray(), target.byteArray());
        assertEquals(source.addressOffset(), target.addressOffset());
    }

    @Theory
    public void shouldBuildTwoDirectBuffers(
        @FromDataPoints("buffers") DirectBuffer source1,
        @FromDataPoints("buffers") DirectBuffer source2)
    {
        DirectBufferBuilder builder = new DefaultDirectBufferBuilder();

        DirectBuffer target = builder.wrap(source1)
                                     .wrap(source2)
                                     .build();

        assertEquals(source1.capacity() + source2.capacity(), target.capacity());
        assertEquals(source1, new UnsafeBuffer(target, 0, source1.capacity()));
        assertEquals(source2, new UnsafeBuffer(target, source1.capacity(), source2.capacity()));
    }

    @Theory
    public void shouldBuildThreeDirectBuffers(
        @FromDataPoints("buffers") DirectBuffer source1,
        @FromDataPoints("buffers") DirectBuffer source2,
        @FromDataPoints("buffers") DirectBuffer source3)
    {
        DirectBufferBuilder builder = new DefaultDirectBufferBuilder();

        DirectBuffer target = builder.wrap(source1)
                                     .wrap(source2)
                                     .wrap(source3)
                                     .build();

        assertEquals(source1.capacity() + source2.capacity() + source3.capacity(), target.capacity());
        assertEquals(source1, new UnsafeBuffer(target, 0, source1.capacity()));
        assertEquals(source2, new UnsafeBuffer(target, source1.capacity(), source2.capacity()));
        assertEquals(source3, new UnsafeBuffer(target, source1.capacity() + source2.capacity(), source3.capacity()));
    }
}
