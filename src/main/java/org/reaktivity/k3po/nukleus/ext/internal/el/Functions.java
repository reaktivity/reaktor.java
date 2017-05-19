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
package org.reaktivity.k3po.nukleus.ext.internal.el;

import static java.lang.Long.MAX_VALUE;
import static java.lang.Long.MIN_VALUE;

import java.util.concurrent.ThreadLocalRandom;

import org.kaazing.k3po.lang.el.Function;
import org.kaazing.k3po.lang.el.spi.FunctionMapperSpi;

public final class Functions
{
    public static final class Mapper extends FunctionMapperSpi.Reflective
    {
        public Mapper()
        {
            super(Functions.class);
        }

        @Override
        public String getPrefixName()
        {
            return "nukleus";
        }
    }

    @Function
    public static long newRouteRef()
    {
        return nextLongNonZero();
    }

    @Function
    public static long newCorrelationId()
    {
        final ThreadLocalRandom random = ThreadLocalRandom.current();
        return random.nextLong();
    }

    private static long nextLongNonZero()
    {
        final ThreadLocalRandom random = ThreadLocalRandom.current();
        return random.nextLong(MIN_VALUE, 0) | random.nextLong(MAX_VALUE);
    }

    private Functions()
    {
        // utility
    }
}
