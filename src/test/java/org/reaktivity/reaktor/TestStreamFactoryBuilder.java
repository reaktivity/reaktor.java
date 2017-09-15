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
package org.reaktivity.reaktor;

import java.util.function.LongSupplier;
import java.util.function.Supplier;

import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Long2ObjectHashMap;
import org.reaktivity.nukleus.buffer.BufferPool;
import org.reaktivity.nukleus.route.RouteManager;
import org.reaktivity.nukleus.stream.StreamFactory;
import org.reaktivity.nukleus.stream.StreamFactoryBuilder;

public class TestStreamFactoryBuilder implements StreamFactoryBuilder
{
    LongSupplier incrementOverflow;
    RouteManager router;
    LongSupplier supplyStreamId;
    Supplier<BufferPool> supplyBufferPool;
    LongSupplier supplyCorrelationId;
    MutableDirectBuffer writeBuffer;
    final Long2ObjectHashMap<Accepted> correlations = new Long2ObjectHashMap<>();

    @Override
    public StreamFactoryBuilder setRouteManager(
        RouteManager router)
    {
        this.router = router;
        return this;
    }

    @Override
    public StreamFactoryBuilder setStreamIdSupplier(
        LongSupplier supplyStreamId)
    {
        this.supplyStreamId = supplyStreamId;
        return this;
    }

    @Override
    public StreamFactoryBuilder setCorrelationIdSupplier(
        LongSupplier supplyCorrelationId)
    {
        this.supplyCorrelationId = supplyCorrelationId;
        return this;
    }

    @Override
    public StreamFactoryBuilder setWriteBuffer(
        MutableDirectBuffer writeBuffer)
    {
        this.writeBuffer = writeBuffer;
        return this;
    }

    @Override
    public StreamFactoryBuilder setBufferPoolSupplier(
        Supplier<BufferPool> supplyBufferPool)
    {
        this.supplyBufferPool = supplyBufferPool;
        return this;
    }

    @Override
    public StreamFactory build()
    {
        return new TestStreamFactory(this);
    }

    static class Accepted
    {
        long correlationId;
        String acceptName;

        Accepted(long correlationId, String acceptName)
        {
            this.correlationId = correlationId;
            this.acceptName = acceptName;
        }
    }

}