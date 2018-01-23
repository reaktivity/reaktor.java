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

import static org.agrona.IoUtil.createEmptyFile;
import static org.agrona.IoUtil.mapExistingFile;
import static org.agrona.IoUtil.unmap;
import static org.reaktivity.reaktor.internal.types.state.RouteTableFW.FIELD_OFFSET_WRITE_LOCK_ACQUIRES;
import static org.reaktivity.reaktor.internal.types.state.RouteTableFW.FIELD_OFFSET_WRITE_LOCK_RELEASES;

import java.io.File;
import java.nio.MappedByteBuffer;
import java.nio.file.Path;

import org.agrona.CloseHelper;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

public final class RoutesLayout extends Layout
{

    private final UnsafeBuffer routesBuffer;
    private final int routesBufferCapacity;

    private RoutesLayout(
            UnsafeBuffer routesBuffer,
        int routesBufferCapacity)
    {
        this.routesBuffer = routesBuffer;
        this.routesBufferCapacity = routesBufferCapacity;
    }

    @Override
    public void close()
    {
        unmap(routesBuffer().byteBuffer());
    }

    public MutableDirectBuffer routesBuffer()
    {
        return routesBuffer;
    }

    public int capacity()
    {
        return routesBufferCapacity;
    }

    public int lock()
    {
        assert routesBuffer.getInt(FIELD_OFFSET_WRITE_LOCK_ACQUIRES) ==  routesBuffer.getInt(FIELD_OFFSET_WRITE_LOCK_RELEASES);
        return  routesBuffer.addIntOrdered(FIELD_OFFSET_WRITE_LOCK_ACQUIRES, 1) + 1;
    }

    public int unlock()
    {
        return routesBuffer.addIntOrdered(FIELD_OFFSET_WRITE_LOCK_RELEASES, 1) + 1;
    }

    public static final class Builder extends Layout.Builder<RoutesLayout>
    {

        private Path path;
        private int routesBufferCapacity;
        private boolean readonly;

        public Builder routesPath(Path path)
        {
            this.path = path;
            return this;
        }

        public Builder routesBufferCapacity(
            int routesBufferCapacity)
        {
            this.routesBufferCapacity = routesBufferCapacity;
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
                CloseHelper.close(createEmptyFile(routes, routesBufferCapacity));
            }

            final MappedByteBuffer mappedRoutes = mapExistingFile(routes, "routes", 0, routesBufferCapacity);

            final UnsafeBuffer mutableRoutesBuffer = new UnsafeBuffer(mappedRoutes);

            return new RoutesLayout(mutableRoutesBuffer, routesBufferCapacity);
        }
    }

}
