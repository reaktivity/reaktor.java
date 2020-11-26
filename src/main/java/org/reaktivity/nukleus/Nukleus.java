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
package org.reaktivity.nukleus;

import org.reaktivity.nukleus.function.CommandHandler;
import org.reaktivity.nukleus.function.MessagePredicate;
import org.reaktivity.nukleus.route.RouteKind;

public interface Nukleus
{
    String name();
    Configuration config();

    @Deprecated
    default Elektron supplyElektron()
    {
        return null;
    }

    default Elektron supplyElektron(
        int index)
    {
        return supplyElektron();
    }

    default CommandHandler commandHandler(
        int msgTypeId)
    {
        return null;
    }

    default MessagePredicate routeHandler(
        RouteKind kind)
    {
        return null;
    }
}
