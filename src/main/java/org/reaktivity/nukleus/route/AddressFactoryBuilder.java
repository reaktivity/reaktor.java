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
package org.reaktivity.nukleus.route;

import java.util.function.LongSupplier;
import java.util.function.LongUnaryOperator;
import java.util.function.ToIntFunction;

import org.agrona.MutableDirectBuffer;

public interface AddressFactoryBuilder
{
    default AddressFactoryBuilder setRouter(
        RouteManager router)
    {
        return this;
    }

    default AddressFactoryBuilder setWriteBuffer(
        MutableDirectBuffer writeBuffer)
    {
        return this;
    }

    default AddressFactoryBuilder setTypeIdSupplier(
        ToIntFunction<String> supplyTypeId)
    {
        return this;
    }

    default AddressFactoryBuilder setTraceIdSupplier(
        LongSupplier supplyTraceId)
    {
        return this;
    }

    default AddressFactoryBuilder setInitialIdSupplier(
        LongUnaryOperator supplyInitialId)
    {
        return this;
    }

    default AddressFactoryBuilder setReplyIdSupplier(
        LongUnaryOperator supplyReplyId)
    {
        return this;
    }

    AddressFactory build();
}
