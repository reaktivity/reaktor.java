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
package org.reaktivity.reaktor.internal.router;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

import org.agrona.concurrent.status.AtomicCounter;
import org.reaktivity.nukleus.route.RouteKind;
import org.reaktivity.reaktor.internal.types.control.Role;

public enum ReferenceKind
{
    SERVER
    {
        @Override
        public RouteKind toRouteKind()
        {
            return RouteKind.SERVER;
        }

        @Override
        protected long nextRef(
            LongSupplier getAndIncrement,
            LongSupplier get)
        {
            // positive, modulo 4 = 0, non-zero
            getAndIncrement.getAsLong();
            return get.getAsLong() << 2L;
        }
    },

    CLIENT
    {
        @Override
        public RouteKind toRouteKind()
        {
            return RouteKind.CLIENT;
        }

        @Override
        protected long nextRef(
            LongSupplier getAndIncrement,
            LongSupplier get)
        {
            // positive, modulo 4 = 1
            return (getAndIncrement.getAsLong() << 2L) | 1L;
        }
    },

    PROXY
    {
        @Override
        public RouteKind toRouteKind()
        {
            return RouteKind.PROXY;
        }

        @Override
        protected long nextRef(
            LongSupplier getAndIncrement,
            LongSupplier get)
        {
            // negative, modulo 4 = 2, non-zero
            getAndIncrement.getAsLong();
            return 0x80000000_00000000L | (get.getAsLong() << 2L);
        }
    },

    SERVER_REVERSE
    {
        @Override
        public RouteKind toRouteKind()
        {
            return RouteKind.SERVER_REVERSE;
        }

        @Override
        protected long nextRef(
            LongSupplier getAndIncrement,
            LongSupplier get)
        {
            // positive, modulo 4 = 2
            return (getAndIncrement.getAsLong() << 2L) | 2L;
        }
    },

    CLIENT_REVERSE
    {
        @Override
        public RouteKind toRouteKind()
        {
            return RouteKind.CLIENT_REVERSE;
        }

        @Override
        protected long nextRef(
            LongSupplier getAndIncrement,
            LongSupplier get)
        {
            // positive, modulo 4 = 3
            return (getAndIncrement.getAsLong() << 2L) | 3L;
        }
    };

    public final long nextRef(
        AtomicCounter counter)
    {
        return nextRef(counter::increment, counter::get);
    }

    public final long nextRef(
        AtomicLong counter)
    {
        return nextRef(counter::getAndIncrement, counter::get);
    }

    public abstract RouteKind toRouteKind();

    protected abstract long nextRef(
        LongSupplier getAndIncrement,
        LongSupplier get);

    public static RouteKind resolve(
        long resolveId)
    {
        switch ((int)resolveId & 0x03 | (int)(resolveId >> 32) & 0x80000000)
        {
        case 0x00000000:
            return RouteKind.SERVER;
        case 0x00000001:
            return RouteKind.CLIENT;
        case 0x00000002:
            return RouteKind.SERVER_REVERSE;
        case 0x00000003:
            return RouteKind.CLIENT_REVERSE;
        case 0x80000000:
            return RouteKind.PROXY;
        default:
            throw new IllegalArgumentException();
        }
    }

    public static boolean valid(
        long referenceId)
    {
        switch ((int)referenceId & 0x03 | (int)(referenceId >> 32) & 0x80000000)
        {
        case 0x00000000:
        case 0x00000001:
        case 0x00000002:
        case 0x00000003:
        case 0x80000000:
            return true;
        default:
            return false;
        }
    }

    public static ReferenceKind sourceKind(
        Role role)
    {
        switch (role)
        {
        case SERVER:
            return SERVER;
        case CLIENT:
            return CLIENT;
        case PROXY:
            return PROXY;
        case SERVER_REVERSE:
            return SERVER_REVERSE;
        case CLIENT_REVERSE:
            return CLIENT_REVERSE;
        }

        throw new IllegalArgumentException("Unexpected role");
    }

    public static ReferenceKind targetKind(
        Role role)
    {
        switch (role)
        {
        case SERVER:
            return SERVER;
        case CLIENT:
            return CLIENT;
        case PROXY:
            return PROXY;
        case SERVER_REVERSE:
            return CLIENT_REVERSE;
        case CLIENT_REVERSE:
            return SERVER_REVERSE;
        }

        throw new IllegalArgumentException("Unexpected role");
    }

    public static ReferenceKind sourceKind(
        RouteKind kind)
    {
        switch (kind)
        {
        case SERVER:
            return SERVER;
        case CLIENT:
            return CLIENT;
        case PROXY:
            return PROXY;
        case SERVER_REVERSE:
            return SERVER_REVERSE;
        case CLIENT_REVERSE:
            return CLIENT_REVERSE;
        }

        throw new IllegalArgumentException("Unexpected route kind");
    }

    public static ReferenceKind targetKind(
        RouteKind kind)
    {
        switch (kind)
        {
        case SERVER:
            return SERVER;
        case CLIENT:
            return CLIENT;
        case PROXY:
            return PROXY;
        case SERVER_REVERSE:
            return CLIENT_REVERSE;
        case CLIENT_REVERSE:
            return SERVER_REVERSE;
        }

        throw new IllegalArgumentException("Unexpected route kind");
    }
}
