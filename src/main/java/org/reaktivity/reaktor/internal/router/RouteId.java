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
package org.reaktivity.reaktor.internal.router;

import org.reaktivity.reaktor.internal.types.control.Role;

public final class RouteId
{
    public static int localId(
        long routeId)
    {
        return (int)(routeId >> 48) & 0xffff;
    }

    public static int remoteId(
        long routeId)
    {
        return (int)(routeId >> 32) & 0xffff;
    }

    public static long routeId(
        final int localId,
        final int remoteId,
        final Role role,
        final int conditionId)
    {
        return (long) localId << 48 |
               (long) remoteId << 32 |
               (long) role.ordinal() << 28 |
               (long) (conditionId & 0x0fff_ffff);
    }

    private RouteId()
    {
    }
}
