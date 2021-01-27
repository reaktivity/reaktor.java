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
package org.reaktivity.nukleus;

import java.util.function.IntFunction;
import java.util.function.LongConsumer;
import java.util.function.LongFunction;
import java.util.function.LongSupplier;
import java.util.function.LongUnaryOperator;
import java.util.function.ToIntFunction;

import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.Agent;
import org.reaktivity.nukleus.buffer.BufferPool;
import org.reaktivity.nukleus.concurrent.SignalingExecutor;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.route.RouteManager;
import org.reaktivity.nukleus.stream.StreamFactory;

public interface AgentBuilder
{
    default AgentBuilder setRouteManager(
        RouteManager router)
    {
        return this;
    }

    default AgentBuilder setExecutor(
        SignalingExecutor executor)
    {
        return this;
    }

    default AgentBuilder setAddressIdSupplier(
        ToIntFunction<String> supplyAddressId)
    {
        return this;
    }

    default AgentBuilder setStreamFactorySupplier(
        IntFunction<StreamFactory> supplyStreamFactory)
    {
        return this;
    }

    default AgentBuilder setThrottleSupplier(
        LongFunction<MessageConsumer> supplyThrottle)
    {
        return this;
    }

    default AgentBuilder setThrottleRemover(
        LongConsumer removeThrottle)
    {
        return this;
    }

    default AgentBuilder setInitialIdSupplier(
        LongUnaryOperator supplyInitialId)
    {
        return this;
    }

    default AgentBuilder setReplyIdSupplier(
        LongUnaryOperator supplyReplyId)
    {
        return this;
    }

    default AgentBuilder setGroupIdSupplier(
        LongSupplier supplyGroupId)
    {
        return this;
    }

    default AgentBuilder setTraceIdSupplier(
        LongSupplier supplyTraceId)
    {
        return this;
    }

    default AgentBuilder setWriteBuffer(
        MutableDirectBuffer writeBuffer)
    {
        return this;
    }

    default AgentBuilder setBufferPool(
        BufferPool bufferPool)
    {
        return this;
    }

    Agent build();
}
