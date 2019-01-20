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
package org.reaktivity.k3po.nukleus.ext.internal.behavior;

import static java.util.Objects.requireNonNull;

import java.net.URI;

import org.kaazing.k3po.driver.internal.netty.channel.ChannelAddress;

public final class NukleusChannelAddress extends ChannelAddress
{
    private static final long serialVersionUID = 1L;

    private final long authorization;
    private final String senderAddress;
    private final String receiverAddress;

    public NukleusChannelAddress(
        URI location,
        long authorization,
        String senderAddress)
    {
        this(location, authorization, senderAddress, receiverAddress(location));
    }

    private NukleusChannelAddress(
        URI location,
        long authorization,
        String senderAddress,
        String receiverAddress)
    {
        super(location);

        this.authorization = authorization;
        this.senderAddress = requireNonNull(senderAddress);
        this.receiverAddress = requireNonNull(receiverAddress);
    }

    private NukleusChannelAddress(
        URI location,
        ChannelAddress transport,
        boolean ephemeral,
        long authorization,
        String senderAddress,
        String receiverAddress)
    {
        super(location, transport, ephemeral);

        this.authorization = authorization;
        this.senderAddress = requireNonNull(senderAddress);
        this.receiverAddress = requireNonNull(receiverAddress);
    }

    public long getAuthorization()
    {
        return authorization;
    }

    public String getSenderAddress()
    {
        return senderAddress;
    }

    public String getReceiverAddress()
    {
        return receiverAddress;
    }

    @Override
    public NukleusChannelAddress newEphemeralAddress()
    {
        return super.createEphemeralAddress(this::newEphemeralAddress);
    }

    public NukleusChannelAddress newReplyToAddress(
        String replyAddress)
    {
        URI location = getLocation();
        return new NukleusChannelAddress(location, authorization, receiverAddress, replyAddress);
    }

    private NukleusChannelAddress newEphemeralAddress(
        URI location,
        ChannelAddress transport)
    {
        return new NukleusChannelAddress(location, transport, true, authorization, senderAddress, receiverAddress);
    }

    private static String receiverAddress(
        URI location)
    {
        final String fragment = location.getFragment();
        final String path = location.getPath().substring(1);
        return fragment != null ? String.format("%s#%s", path, fragment) : path;
    }
}
