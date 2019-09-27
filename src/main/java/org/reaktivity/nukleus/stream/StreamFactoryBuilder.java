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
package org.reaktivity.nukleus.stream;

import java.util.function.Function;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;
import java.util.function.LongUnaryOperator;
import java.util.function.Supplier;
import java.util.function.ToIntFunction;

import org.agrona.MutableDirectBuffer;
import org.reaktivity.nukleus.buffer.BufferPool;
import org.reaktivity.nukleus.concurrent.SignalingExecutor;
import org.reaktivity.nukleus.route.RouteManager;

public interface StreamFactoryBuilder
{
    StreamFactoryBuilder setRouteManager(
        RouteManager router);

    default StreamFactoryBuilder setExecutor(
        SignalingExecutor executor)
    {
        return this;
    }

    default StreamFactoryBuilder setTypeIdSupplier(
        ToIntFunction<String> supplyTypeId)
    {
        return this;
    }

    StreamFactoryBuilder setInitialIdSupplier(
        LongUnaryOperator supplyInitialId);

    StreamFactoryBuilder setReplyIdSupplier(
        LongUnaryOperator supplyReplyId);

    default StreamFactoryBuilder setGroupIdSupplier(
        LongSupplier supplyGroupId)
    {
        return this;
    }

    default StreamFactoryBuilder setTraceSupplier(
        LongSupplier supplyTraceId)
    {
        return this;
    }

    StreamFactoryBuilder setWriteBuffer(
        MutableDirectBuffer writeBuffer);

    StreamFactoryBuilder setBufferPoolSupplier(
        Supplier<BufferPool> supplyBufferPool);

    default StreamFactoryBuilder setCounterSupplier(
        Function<String, LongSupplier> supplyCounter)
    {
        return this;
    }

    default StreamFactoryBuilder setAccumulatorSupplier(
        Function<String, LongConsumer> supplyAccumulator)
    {
        return this;
    }

    StreamFactory build();

}
