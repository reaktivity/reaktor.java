/**
 * Copyright 2016-2020 The Reaktivity Project
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
import java.util.function.LongFunction;
import java.util.function.LongSupplier;
import java.util.function.LongUnaryOperator;
import java.util.function.Supplier;
import java.util.function.ToIntFunction;

import org.agrona.MutableDirectBuffer;
import org.reaktivity.nukleus.budget.BudgetCreditor;
import org.reaktivity.nukleus.budget.BudgetDebitor;
import org.reaktivity.nukleus.buffer.BufferPool;
import org.reaktivity.nukleus.concurrent.Signaler;
import org.reaktivity.nukleus.concurrent.SignalingExecutor;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.route.RouteManager;

public interface StreamFactoryBuilder
{
    default StreamFactoryBuilder setRouteManager(
        RouteManager router)
    {
        return this;
    }

    @Deprecated
    default StreamFactoryBuilder setExecutor(
        SignalingExecutor executor)
    {
        return this;
    }

    default StreamFactoryBuilder setSignaler(
        Signaler signaler)
    {
        return this;
    }

    default StreamFactoryBuilder setTypeIdSupplier(
        ToIntFunction<String> supplyTypeId)
    {
        return this;
    }

    default StreamFactoryBuilder setInitialIdSupplier(
        LongUnaryOperator supplyInitialId)
    {
        return this;
    }

    default StreamFactoryBuilder setReplyIdSupplier(
        LongUnaryOperator supplyReplyId)
    {
        return this;
    }

    default StreamFactoryBuilder setBudgetIdSupplier(
        LongSupplier supplyBudgetId)
    {
        return this;
    }

    default StreamFactoryBuilder setBudgetCreditor(
        BudgetCreditor creditor)
    {
        return this;
    }

    default StreamFactoryBuilder setBudgetDebitorSupplier(
        LongFunction<BudgetDebitor> supplyDebitor)
    {
        return this;
    }

    default StreamFactoryBuilder setTraceIdSupplier(
        LongSupplier supplyTraceId)
    {
        return this;
    }

    default StreamFactoryBuilder setWriteBuffer(
        MutableDirectBuffer writeBuffer)
    {
        return this;
    }

    default StreamFactoryBuilder setBufferPoolSupplier(
        Supplier<BufferPool> supplyBufferPool)
    {
        return this;
    }

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

    default StreamFactoryBuilder setDroppedFrameConsumer(
        MessageConsumer dropFrame)
    {
        return this;
    }

    StreamFactory build();
}
