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

import java.nio.channels.SelectableChannel;
import java.util.function.Function;

import org.reaktivity.nukleus.route.AddressFactoryBuilder;
import org.reaktivity.nukleus.route.RouteKind;
import org.reaktivity.nukleus.stream.StreamFactoryBuilder;
import org.reaktivity.reaktor.poller.PollerKey;

public interface Elektron
{
    default void setPollerKeySupplier(
        Function<SelectableChannel, PollerKey> supplyPollerKey)
    {
    }

    default StreamFactoryBuilder streamFactoryBuilder(
        RouteKind kind)
    {
        return null;
    }

    default AddressFactoryBuilder addressFactoryBuilder(
        RouteKind kind)
    {
        return null;
    }
}
