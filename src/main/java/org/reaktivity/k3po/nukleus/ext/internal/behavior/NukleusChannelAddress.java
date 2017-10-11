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
package org.reaktivity.k3po.nukleus.ext.internal.behavior;

import java.net.URI;

import org.kaazing.k3po.driver.internal.netty.channel.ChannelAddress;

public final class NukleusChannelAddress extends ChannelAddress
{
    private static final long serialVersionUID = 1L;

    private final long route;
    private final long authorization;
    private final String replyTo;

    public NukleusChannelAddress(
        URI location,
        long route,
        long authorization,
        String replyTo)
    {
        super(location);

        this.route = route;
        this.authorization = authorization;
        this.replyTo = replyTo;
    }

    private NukleusChannelAddress(
        URI location,
        ChannelAddress transport,
        boolean ephemeral,
        long route,
        long authorization,
        String replyTo)
    {
        super(location, transport, ephemeral);

        this.route = route;
        this.authorization = authorization;
        this.replyTo = replyTo;
    }

    public long getRoute()
    {
        return route;
    }

    public long getAuthorization()
    {
        return authorization;
    }

    public String getReplyTo()
    {
        return replyTo;
    }

    public String getSenderName()
    {
        return senderName(this.getLocation());
    }

    public String getSenderPartition()
    {
        return senderPartition(this.getLocation());
    }

    public String getReceiverName()
    {
        return receiverName(getLocation());
    }

    @Override
    public NukleusChannelAddress newEphemeralAddress()
    {
        return super.createEphemeralAddress(this::newEphemeralAddress);
    }

    public NukleusChannelAddress newReplyToAddress()
    {
        URI location = getLocation();
        String oldReceiver = receiverName(location);
        String oldSender = senderName(location);
        String newSender = oldReceiver;
        String newReceiver = oldSender;
        String newReplyTo = oldReceiver;
        URI newLocation = URI.create(String.format("nukleus://%s/streams/%s", newReceiver, newSender));
        return new NukleusChannelAddress(newLocation, 0L, authorization, newReplyTo);
    }

    private NukleusChannelAddress newEphemeralAddress(
        URI location,
        ChannelAddress transport)
    {
        return new NukleusChannelAddress(location, transport, true, route, authorization, replyTo);
    }

    private static String senderName(
        URI location)
    {
        String path = location.getPath();
        assert path.startsWith("/streams/");
        return path.substring("/streams/".length());
    }

    private static String senderPartition(
        URI location)
    {
        String senderPart = location.getFragment();
        String senderName = senderName(location);
        return senderPart == null ? senderName : String.format("%s#%s", senderName, senderPart);
    }

    private String receiverName(
        URI location)
    {
        return location.getHost();
    }
}
