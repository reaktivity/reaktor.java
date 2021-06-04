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
package org.reaktivity.reaktor.test.internal.k3po.ext.behavior;

import static java.util.Objects.requireNonNull;

import java.net.URI;

import org.kaazing.k3po.driver.internal.netty.channel.ChannelAddress;

public final class NukleusChannelAddress extends ChannelAddress
{
    private static final long serialVersionUID = 1L;

    private final long authorization;
    private final String namespace;
    private final String binding;

    public NukleusChannelAddress(
        URI location,
        long authorization,
        String namespace)
    {
        this(location, authorization, namespace, bindingName(location));
    }

    private NukleusChannelAddress(
        URI location,
        long authorization,
        String namespace,
        String binding)
    {
        super(location);

        this.authorization = authorization;
        this.namespace = requireNonNull(namespace);
        this.binding = requireNonNull(binding);
    }

    private NukleusChannelAddress(
        URI location,
        ChannelAddress transport,
        boolean ephemeral,
        long authorization,
        String namespace,
        String binding)
    {
        super(location, transport, ephemeral);

        this.authorization = authorization;
        this.namespace = requireNonNull(namespace);
        this.binding = requireNonNull(binding);
    }

    public long getAuthorization()
    {
        return authorization;
    }

    public String getNamespace()
    {
        return namespace;
    }

    public String getBinding()
    {
        return binding;
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
        return new NukleusChannelAddress(location, authorization, namespace, replyAddress);
    }

    private NukleusChannelAddress newEphemeralAddress(
        URI location,
        ChannelAddress transport)
    {
        return new NukleusChannelAddress(location, transport, true, authorization, namespace, binding);
    }

    private static String bindingName(
        URI location)
    {
        final String fragment = location.getFragment();
        final String path = location.getPath().substring(1);
        return fragment != null ? String.format("%s#%s", path, fragment) : path;
    }
}
