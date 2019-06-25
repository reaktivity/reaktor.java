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
package org.reaktivity.k3po.nukleus.ext.internal.el;

import static java.lang.Long.MAX_VALUE;
import static java.lang.Long.MIN_VALUE;
import static java.lang.ThreadLocal.withInitial;

import java.nio.file.Path;
import java.util.concurrent.ThreadLocalRandom;

import org.kaazing.k3po.lang.el.Function;
import org.kaazing.k3po.lang.el.spi.FunctionMapperSpi;
import org.reaktivity.k3po.nukleus.ext.internal.NukleusExtConfiguration;
import org.reaktivity.k3po.nukleus.ext.internal.behavior.LabelManager;
import org.reaktivity.k3po.nukleus.ext.internal.behavior.types.control.Role;

public final class Functions
{
    private static final ThreadLocal<LabelManager> LABELS = withInitial(Functions::newLabelManager);

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
    public static int id(
        String nukleus)
    {
        final LabelManager labels = LABELS.get();
        return labels.supplyLabelId(nukleus);
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

    @Function
    public static Long newClientRouteId(
        String senderAddress,
        String receiverAddress)
    {
        return newRouteId(Role.CLIENT, receiverAddress, senderAddress);
    }

    @Function
    public static Long newServerRouteId(
        String senderAddress,
        String receiverAddress)
    {
        return newRouteId(Role.SERVER, receiverAddress, senderAddress);
    }

    @Function
    public static Long newClientReverseRouteId(
        String senderAddress,
        String receiverAddress)
    {
        return newRouteId(Role.CLIENT_REVERSE, receiverAddress, senderAddress);
    }

    @Function
    public static Long newServerReverseRouteId(
        String senderAddress,
        String receiverAddress)
    {
        return newRouteId(Role.SERVER_REVERSE, receiverAddress, senderAddress);
    }

    @Function
    public static Long newProxyRouteId(
        String senderAddress,
        String receiverAddress)
    {
        return newRouteId(Role.PROXY, receiverAddress, senderAddress);
    }

    private static long nextLongNonZero()
    {
        final ThreadLocalRandom random = ThreadLocalRandom.current();
        return random.nextLong(MIN_VALUE, 0) | random.nextLong(MAX_VALUE);
    }

    private static Long newRouteId(
        Role role,
        String localAddress,
        String remoteAddress)
    {
        final LabelManager labels = LABELS.get();
        long localId = labels.supplyLabelId(localAddress);
        long remoteId = labels.supplyLabelId(remoteAddress);
        long condition = ThreadLocalRandom.current().nextInt(1 << 28);

        return localId << 48 | remoteId << 32 | role.ordinal() << 28 | condition;
    }

    private Functions()
    {
        // utility
    }

    private static LabelManager newLabelManager()
    {
        final NukleusExtConfiguration config = new NukleusExtConfiguration();
        final Path directory = config.directory();
        return new LabelManager(directory);
    }
}
