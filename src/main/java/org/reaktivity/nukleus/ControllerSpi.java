/**
 * Copyright 2016-2018 The Reaktivity Project
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

import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.ToIntFunction;

import org.agrona.DirectBuffer;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.function.MessagePredicate;

public interface ControllerSpi
{
    long nextCorrelationId();

    int doProcess();

    void doClose();

    CompletableFuture<Long> doResolve(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length);

    CompletableFuture<Void> doUnresolve(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length);

    CompletableFuture<Long> doRoute(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length);

    CompletableFuture<Void> doUnroute(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length);

    CompletableFuture<Void> doFreeze(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length);

    <R> R doSupplyTarget(
        String target,
        BiFunction<ToIntFunction<MessageConsumer>, MessagePredicate, R> factory);

    long doCount(String name);

}
