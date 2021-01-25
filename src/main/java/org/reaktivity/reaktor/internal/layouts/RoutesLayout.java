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
package org.reaktivity.reaktor.internal.layouts;

import static org.agrona.IoUtil.createEmptyFile;
import static org.agrona.IoUtil.mapExistingFile;
import static org.agrona.IoUtil.unmap;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;

import org.agrona.LangUtil;
import org.agrona.concurrent.AtomicBuffer;
import org.agrona.concurrent.UnsafeBuffer;

public final class RoutesLayout extends Layout
{
    private final AtomicBuffer routesBuffer;

    private RoutesLayout(
        AtomicBuffer routesBuffer)
    {
        this.routesBuffer = routesBuffer;
    }

    @Override
    public void close()
    {
        unmap(routesBuffer().byteBuffer());
    }

    public AtomicBuffer routesBuffer()
    {
        return routesBuffer;
    }

    public static final class Builder extends Layout.Builder<RoutesLayout>
    {

        private Path path;
        private int capacity;
        private boolean readonly;

        public Builder routesPath(
            Path path)
        {
            this.path = path;
            return this;
        }

        public Builder routesBufferCapacity(
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

        @Override
        public RoutesLayout build()
        {
            final File routes = path.toFile();

            if (!readonly)
            {
                try (FileChannel channel = createEmptyFile(routes, capacity))
                {
                    ByteBuffer empty = ByteBuffer.allocate(3 * Integer.BYTES).order(ByteOrder.nativeOrder());
                    empty.putInt(0);
                    empty.putInt(Integer.BYTES);
                    empty.putInt(0);
                    empty.flip();
                    channel.position(0);
                    channel.write(empty);
                }
                catch (IOException ex)
                {
                    LangUtil.rethrowUnchecked(ex);
                }
            }

            final MappedByteBuffer mappedRoutes = mapExistingFile(routes, "routes");

            final AtomicBuffer routesBuffer = new UnsafeBuffer(mappedRoutes);

            return new RoutesLayout(routesBuffer);
        }
    }
}
