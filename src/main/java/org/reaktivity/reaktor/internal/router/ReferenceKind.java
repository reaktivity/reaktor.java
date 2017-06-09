/**
 * Copyright 2016-2017 The Reaktivity Project
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
        protected long nextRef(
            LongSupplier getAndIncrement,
            LongSupplier get)
        {
            // positive, even, non-zero
            getAndIncrement.getAsLong();
            return get.getAsLong() << 1L;
        }
    },

    CLIENT
    {
        @Override
        protected long nextRef(
            LongSupplier getAndIncrement,
            LongSupplier get)
        {
            // positive, odd
            return (getAndIncrement.getAsLong() << 1L) | 1L;
        }
    },

    PROXY
    {
        @Override
        protected long nextRef(
            LongSupplier getAndIncrement,
            LongSupplier get)
        {
            // negative, even, non-zero
            getAndIncrement.getAsLong();
            return 0x80000000_00000000L | (get.getAsLong() << 1L);
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

    protected abstract long nextRef(
        LongSupplier getAndIncrement,
        LongSupplier get);

    public static RouteKind resolve(
        long resolveId)
    {
        switch ((int)resolveId & 0x01 | (int)(resolveId >> 32) & 0x80000000)
        {
        case 0x00000001:
            return RouteKind.CLIENT;
        case 0x00000000:
            return RouteKind.SERVER;
        case 0x80000000:
            return RouteKind.PROXY;
        default:
            throw new IllegalArgumentException();
        }
    }

    public static boolean valid(
        long referenceId)
    {
        switch ((int)referenceId & 0x01 | (int)(referenceId >> 32) & 0x80000000)
        {
        case 0x00000001:
        case 0x00000000:
        case 0x80000000:
            return true;
        default:
            return false;
        }
    }

    public static ReferenceKind valueOf(
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
        }

        throw new IllegalArgumentException("Unexpected role");
    }

    public static ReferenceKind valueOf(
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
        }

        throw new IllegalArgumentException("Unexpected route kind");
    }
}
