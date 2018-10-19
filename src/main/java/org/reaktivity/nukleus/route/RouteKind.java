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
package org.reaktivity.nukleus.route;

public enum RouteKind
{
    SERVER,
    CLIENT,
    PROXY,
    SERVER_REVERSE,
    CLIENT_REVERSE;

    public static RouteKind valueOf(
        int ordinal)
    {
        switch (ordinal)
        {
        case 0:
            return SERVER;
        case 1:
            return CLIENT;
        case 2:
            return PROXY;
        case 3:
            return SERVER_REVERSE;
        case 4:
            return CLIENT_REVERSE;
        default:
            throw new IllegalArgumentException(String.format("Unrecognized value: %d", ordinal));
        }
    }
}
