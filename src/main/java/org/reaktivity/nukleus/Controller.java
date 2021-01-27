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

import java.util.concurrent.CompletableFuture;

import org.reaktivity.nukleus.route.RouteKind;

@FunctionalInterface
public interface Controller extends AutoCloseable
{
    int process();

    default CompletableFuture<Long> route(
        RouteKind kind,
        String localAddress,
        String remoteAddress,
        String extension)
    {
        throw new UnsupportedOperationException("route");
    }

    @Override
    default void close() throws Exception
    {
    }

    default Class<? extends Controller> kind()
    {
        return getClass();
    }

    default String name()
    {
        return null;
    }
}
