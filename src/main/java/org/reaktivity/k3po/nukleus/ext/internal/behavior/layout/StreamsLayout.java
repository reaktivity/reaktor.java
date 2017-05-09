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
package org.reaktivity.k3po.nukleus.ext.internal.behavior.layout;

import static org.agrona.IoUtil.createEmptyFile;
import static org.agrona.IoUtil.mapExistingFile;
import static org.agrona.IoUtil.unmap;

import java.io.File;
import java.nio.MappedByteBuffer;
import java.nio.file.Path;

import org.agrona.CloseHelper;
import org.agrona.concurrent.AtomicBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.ringbuffer.OneToOneRingBuffer;
import org.agrona.concurrent.ringbuffer.RingBuffer;
import org.agrona.concurrent.ringbuffer.RingBufferDescriptor;

public final class StreamsLayout extends Layout
{
    private final RingBuffer streamsBuffer;
    private final RingBuffer throttleBuffer;

    private StreamsLayout(
        RingBuffer streamsBuffer,
        RingBuffer throttleBuffer)
    {
        this.streamsBuffer = streamsBuffer;
        this.throttleBuffer = throttleBuffer;
    }

    public RingBuffer streamsBuffer()
    {
        return streamsBuffer;
    }

    public RingBuffer throttleBuffer()
    {
        return throttleBuffer;
    }

    @Override
    public void close()
    {
        unmap(streamsBuffer.buffer().byteBuffer());
        unmap(throttleBuffer.buffer().byteBuffer());
    }

    public static final class Builder extends Layout.Builder<StreamsLayout>
    {
        private long streamsCapacity;
        private long throttleCapacity;
        private Path path;
        private boolean readonly;

        public Builder streamsCapacity(
            long streamsCapacity)
        {
            this.streamsCapacity = streamsCapacity;
            return this;
        }

        public Builder throttleCapacity(
            long throttleCapacity)
        {
            this.throttleCapacity = throttleCapacity;
            return this;
        }

        public Builder path(
            Path path)
        {
            this.path = path;
            return this;
        }

        public Builder readonly(
            boolean readonly)
        {
            this.readonly = readonly;
            return this;
        }

        @Override
        public StreamsLayout build()
        {
            final File streams = path.toFile();
            final long streamsSize = streamsCapacity + RingBufferDescriptor.TRAILER_LENGTH;
            final long throttleSize = throttleCapacity + RingBufferDescriptor.TRAILER_LENGTH;

            if (!readonly)
            {
                CloseHelper.close(createEmptyFile(streams, streamsSize + throttleSize));
            }

            final MappedByteBuffer mappedStreams = mapExistingFile(streams, "streams", 0, streamsSize);
            final MappedByteBuffer mappedThrottle = mapExistingFile(streams, "throttle", streamsSize, throttleSize);

            final AtomicBuffer atomicStreams = new UnsafeBuffer(mappedStreams);
            final AtomicBuffer atomicThrottle = new UnsafeBuffer(mappedThrottle);

            return new StreamsLayout(new OneToOneRingBuffer(atomicStreams),
                                     new OneToOneRingBuffer(atomicThrottle));
        }
    }
}
