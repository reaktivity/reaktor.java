/**
 * Copyright 2016-2019 The Reaktivity Project
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

import static org.agrona.BitUtil.align;
import static org.agrona.IoUtil.createEmptyFile;
import static org.agrona.IoUtil.mapExistingFile;
import static org.agrona.IoUtil.unmap;

import java.io.File;
import java.nio.MappedByteBuffer;
import java.nio.file.Path;

import org.agrona.BitUtil;
import org.agrona.CloseHelper;
import org.agrona.concurrent.AtomicBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.broadcast.BroadcastBufferDescriptor;
import org.agrona.concurrent.ringbuffer.RingBufferDescriptor;

public final class ControlLayout extends Layout
{
    private static final int CONTROL_VERSION = 1;

    private static final int FIELD_OFFSET_VERSION = 0;
    private static final int FIELD_SIZE_VERSION = BitUtil.SIZE_OF_INT;

    private static final int FIELD_OFFSET_COMMAND_BUFFER_LENGTH = FIELD_OFFSET_VERSION + FIELD_SIZE_VERSION;
    private static final int FIELD_SIZE_COMMAND_BUFFER_LENGTH = BitUtil.SIZE_OF_INT;

    private static final int FIELD_OFFSET_RESPONSE_BUFFER_LENGTH =
            FIELD_OFFSET_COMMAND_BUFFER_LENGTH + FIELD_SIZE_COMMAND_BUFFER_LENGTH;
    private static final int FIELD_SIZE_RESPONSE_BUFFER_LENGTH = BitUtil.SIZE_OF_INT;

    private static final int END_OF_META_DATA_OFFSET = align(
            FIELD_OFFSET_RESPONSE_BUFFER_LENGTH + FIELD_SIZE_RESPONSE_BUFFER_LENGTH, BitUtil.CACHE_LINE_LENGTH);

    private final AtomicBuffer commandBuffer = new UnsafeBuffer(new byte[0]);
    private final AtomicBuffer responseBuffer = new UnsafeBuffer(new byte[0]);


    public AtomicBuffer commandBuffer()
    {
        return commandBuffer;
    }

    public AtomicBuffer responseBuffer()
    {
        return responseBuffer;
    }

    @Override
    public void close()
    {
        unmap(commandBuffer.byteBuffer());
        unmap(responseBuffer.byteBuffer());
    }

    public static final class Builder extends Layout.Builder<ControlLayout>
    {
        private final ControlLayout layout;

        private Path controlPath;
        private int commandBufferCapacity;
        private int responseBufferCapacity;

        private boolean readonly;

        public Builder()
        {
            this.layout = new ControlLayout();
        }

        public Builder controlPath(Path controlPath)
        {
            this.controlPath = controlPath;
            return this;
        }

        public Path controlPath()
        {
            return controlPath;
        }

        public Builder commandBufferCapacity(int commandBufferCapacity)
        {
            this.commandBufferCapacity = commandBufferCapacity;
            return this;
        }

        public Builder responseBufferCapacity(int responseBufferCapacity)
        {
            this.responseBufferCapacity = responseBufferCapacity;
            return this;
        }

        public Builder readonly(boolean readonly)
        {
            this.readonly = readonly;
            return this;
        }

        @Override
        public ControlLayout build()
        {
            File controlFile = controlPath.toFile();
            if (!readonly)
            {
                int commandBufferLength = commandBufferCapacity + RingBufferDescriptor.TRAILER_LENGTH;
                int responseBufferLength = responseBufferCapacity + BroadcastBufferDescriptor.TRAILER_LENGTH;

                CloseHelper.close(createEmptyFile(controlFile, END_OF_META_DATA_OFFSET +
                        commandBufferLength + responseBufferLength));

                MappedByteBuffer metadata = mapExistingFile(controlFile, "metadata", 0, END_OF_META_DATA_OFFSET);
                metadata.putInt(FIELD_OFFSET_VERSION, CONTROL_VERSION);
                metadata.putInt(FIELD_OFFSET_COMMAND_BUFFER_LENGTH, commandBufferCapacity);
                metadata.putInt(FIELD_OFFSET_RESPONSE_BUFFER_LENGTH, responseBufferCapacity);
                unmap(metadata);
            }
            else
            {
                MappedByteBuffer metadata = mapExistingFile(controlFile, "metadata", 0, END_OF_META_DATA_OFFSET);
                assert CONTROL_VERSION == metadata.getInt(FIELD_OFFSET_VERSION);
                commandBufferCapacity = metadata.getInt(FIELD_OFFSET_COMMAND_BUFFER_LENGTH);
                responseBufferCapacity = metadata.getInt(FIELD_OFFSET_RESPONSE_BUFFER_LENGTH);
                unmap(metadata);
            }

            int commandBufferLength = commandBufferCapacity + RingBufferDescriptor.TRAILER_LENGTH;
            int responseBufferLength = responseBufferCapacity + BroadcastBufferDescriptor.TRAILER_LENGTH;

            int commandBufferOffset = END_OF_META_DATA_OFFSET;
            layout.commandBuffer.wrap(mapExistingFile(controlFile, "commands", commandBufferOffset, commandBufferLength));

            int responseBufferOffset = commandBufferOffset + commandBufferLength;
            layout.responseBuffer.wrap(mapExistingFile(controlFile, "responses", responseBufferOffset, responseBufferLength));

            return layout;
        }
    }
}
