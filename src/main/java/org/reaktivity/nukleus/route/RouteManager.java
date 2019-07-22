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
package org.reaktivity.nukleus.route;

import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.function.MessageFunction;
import org.reaktivity.nukleus.function.MessagePredicate;

public interface RouteManager
{
    <R> R resolveExternal(
        long authorization,
        MessagePredicate filter,
        MessageFunction<R> mapper);

    <R> R resolve(
        long routeId,
        long authorization,
        MessagePredicate filter,
        MessageFunction<R> mapper);

    void forEach(
        MessageConsumer consumer);

    MessageConsumer supplyReceiver(
        long streamId);

    void setThrottle(
        long streamId,
        MessageConsumer throttle);

    void clearThrottle(
        long streamId);
}
