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

import static java.util.Arrays.asList;
import static org.reaktivity.k3po.nukleus.ext.internal.types.NukleusTypeSystem.OPTION_AUTHORIZATION;
import static org.reaktivity.k3po.nukleus.ext.internal.types.NukleusTypeSystem.OPTION_BYTE_ORDER;
import static org.reaktivity.k3po.nukleus.ext.internal.types.NukleusTypeSystem.OPTION_CORRELATION;
import static org.reaktivity.k3po.nukleus.ext.internal.types.NukleusTypeSystem.OPTION_PADDING;
import static org.reaktivity.k3po.nukleus.ext.internal.types.NukleusTypeSystem.OPTION_PARTITION;
import static org.reaktivity.k3po.nukleus.ext.internal.types.NukleusTypeSystem.OPTION_REPLY_TO;
import static org.reaktivity.k3po.nukleus.ext.internal.types.NukleusTypeSystem.OPTION_ROUTE;
import static org.reaktivity.k3po.nukleus.ext.internal.types.NukleusTypeSystem.OPTION_THROTTLE;
import static org.reaktivity.k3po.nukleus.ext.internal.types.NukleusTypeSystem.OPTION_TRANSMISSION;
import static org.reaktivity.k3po.nukleus.ext.internal.types.NukleusTypeSystem.OPTION_UPDATE;
import static org.reaktivity.k3po.nukleus.ext.internal.types.NukleusTypeSystem.OPTION_WINDOW;

import java.net.URI;
import java.util.Collection;
import java.util.Map;

import org.jboss.netty.channel.ChannelException;
import org.kaazing.k3po.driver.internal.netty.channel.ChannelAddress;
import org.kaazing.k3po.driver.internal.netty.channel.ChannelAddressFactorySpi;
import org.kaazing.k3po.lang.types.TypeInfo;

public class NukleusChannelAddressFactory extends ChannelAddressFactorySpi
{
    @Override
    public String getSchemeName()
    {
        return "nukleus";
    }

    @Override
    protected ChannelAddress newChannelAddress0(
        URI location,
        ChannelAddress transport,
        Map<String, Object> options)
    {
        String host = location.getHost();
        int port = location.getPort();
        String path = location.getPath();

        if (host == null)
        {
            throw new ChannelException(String.format("%s host missing", getSchemeName()));
        }

        if (port != -1)
        {
            throw new ChannelException(String.format("%s port unexpected", getSchemeName()));
        }

        if (path == null || path.isEmpty())
        {
            throw new ChannelException(String.format("%s path missing", getSchemeName()));
        }

        if (!path.startsWith("/streams/"))
        {
            throw new ChannelException(String.format("%s path does not match '/streams/*'", getSchemeName()));
        }

        Collection<TypeInfo<?>> requiredTypes = asList(OPTION_ROUTE, OPTION_WINDOW);
        for (TypeInfo<?> requiredType : requiredTypes)
        {
            if (options == null || !options.containsKey(requiredType.getName()))
            {
                throw new ChannelException(String.format("%s %s option missing", getSchemeName(), requiredType.getName()));
            }
        }

        Collection<TypeInfo<?>> allOptionTypes = asList(OPTION_ROUTE, OPTION_REPLY_TO, OPTION_WINDOW, OPTION_PADDING,
                OPTION_UPDATE, OPTION_PARTITION, OPTION_AUTHORIZATION, OPTION_CORRELATION, OPTION_THROTTLE, OPTION_TRANSMISSION,
                OPTION_BYTE_ORDER);
        for (TypeInfo<?> optionType : allOptionTypes)
        {
            if (options != null && options.containsKey(optionType.getName()))
            {
                Object value = options.get(optionType.getName());
                if (!optionType.getType().isInstance(value))
                {
                    throw new ChannelException(
                            String.format("%s %s option incorrect type", getSchemeName(), optionType.getName()));
                }
            }
        }

        final long route = (Long) options.get(OPTION_ROUTE.getName());
        final long authorization = (Long) options.getOrDefault(OPTION_AUTHORIZATION.getName(), 0L);
        final String replyTo = (String) options.get(OPTION_REPLY_TO.getName());

        return new NukleusChannelAddress(location, route, authorization, replyTo);
    }
}
