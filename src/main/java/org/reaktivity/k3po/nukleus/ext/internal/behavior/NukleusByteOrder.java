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
package org.reaktivity.k3po.nukleus.ext.internal.behavior;

import static java.nio.ByteOrder.BIG_ENDIAN;
import static java.nio.ByteOrder.nativeOrder;

import java.util.Objects;

import org.jboss.netty.buffer.ChannelBufferFactory;
import org.jboss.netty.buffer.HeapChannelBufferFactory;

public enum NukleusByteOrder
{
    NETWORK(HeapChannelBufferFactory.getInstance(BIG_ENDIAN)),
    NATIVE(HeapChannelBufferFactory.getInstance(nativeOrder()));

    private final ChannelBufferFactory bufferFactory;

    NukleusByteOrder(
        ChannelBufferFactory bufferFactory)
    {
        this.bufferFactory = bufferFactory;
    }

    public ChannelBufferFactory toBufferFactory()
    {
        return bufferFactory;
    }

    public static NukleusByteOrder decode(
        String value)
    {
        Objects.requireNonNull(value);

        switch (value)
        {
        case "network":
            return NETWORK;
        case "native":
            return NATIVE;
        default:
            throw new IllegalArgumentException(value);
        }
    }
}
