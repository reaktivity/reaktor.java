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
package org.reaktivity.k3po.nukleus.ext.internal.behavior.config;

import static java.util.Objects.requireNonNull;

import java.util.List;

import org.jboss.netty.buffer.ChannelBufferFactory;
import org.jboss.netty.channel.Channel;
import org.kaazing.k3po.driver.internal.behavior.handler.codec.ConfigEncoder;
import org.kaazing.k3po.driver.internal.behavior.handler.codec.MessageEncoder;
import org.kaazing.k3po.lang.types.StructuredTypeInfo;
import org.reaktivity.k3po.nukleus.ext.internal.behavior.NukleusChannel;
import org.reaktivity.k3po.nukleus.ext.internal.behavior.NukleusChannelConfig;
import org.reaktivity.k3po.nukleus.ext.internal.behavior.NukleusExtensionKind;

public final class NukleusExtensionEncoder implements ConfigEncoder
{
    private final NukleusExtensionKind writeExtKind;
    private final StructuredTypeInfo type;
    private final List<MessageEncoder> encoders;

    public NukleusExtensionEncoder(
        NukleusExtensionKind writeExtKind,
        StructuredTypeInfo type,
        List<MessageEncoder> encoders)
    {
        this.writeExtKind = writeExtKind;
        this.type = type;
        this.encoders = requireNonNull(encoders);
    }

    @Override
    public void encode(
        Channel channel) throws Exception
    {
        encode((NukleusChannel) channel);
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append(type.getQualifiedName()).append(' ');
        for (MessageEncoder encoder : encoders)
        {
            sb.append(encoder).append(' ');
        }
        sb.setLength(sb.length() - 1);
        return sb.toString();
    }

    private void encode(
        NukleusChannel channel)
    {
        NukleusChannelConfig config = channel.getConfig();
        ChannelBufferFactory bufferFactory = config.getBufferFactory();
        for (MessageEncoder encoder : encoders)
        {
            channel.writeExtBuffer(writeExtKind, false).writeBytes(encoder.encode(bufferFactory));
        }
    }
}
