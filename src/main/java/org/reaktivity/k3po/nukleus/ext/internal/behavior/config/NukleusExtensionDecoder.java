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
package org.reaktivity.k3po.nukleus.ext.internal.behavior.config;

import static java.util.Objects.requireNonNull;

import java.util.List;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.kaazing.k3po.driver.internal.behavior.ScriptProgressException;
import org.kaazing.k3po.driver.internal.behavior.handler.codec.ConfigDecoder;
import org.kaazing.k3po.driver.internal.behavior.handler.codec.MessageDecoder;
import org.kaazing.k3po.lang.types.StructuredTypeInfo;
import org.reaktivity.k3po.nukleus.ext.internal.behavior.NukleusChannel;

public final class NukleusExtensionDecoder implements ConfigDecoder
{
    private final StructuredTypeInfo type;
    private final List<MessageDecoder> decoders;

    public NukleusExtensionDecoder(
        StructuredTypeInfo type,
        List<MessageDecoder> decoders)
    {
        this.type = type;
        this.decoders = requireNonNull(decoders);
    }

    @Override
    public void decode(
        Channel channel) throws Exception
    {
        decode((NukleusChannel) channel);
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("nukleus:").append(type.getName()).append(' ');
        for (MessageDecoder decoder : decoders)
        {
            sb.append(decoder).append(' ');
        }
        sb.setLength(sb.length() - 1);
        return sb.toString();
    }

    private void decode(
        NukleusChannel channel) throws Exception
    {
        final ChannelBuffer readExtBuffer = channel.readExtBuffer();

        final int lastIndex = decoders.size() - 1;
        for (int index=0; index < decoders.size(); index++)
        {
            MessageDecoder decoder = decoders.get(index);

            ChannelBuffer remainingExtBuffer;

            if (index == lastIndex)
            {
                remainingExtBuffer = decoder.decodeLast(readExtBuffer);
            }
            else
            {
                remainingExtBuffer = decoder.decode(readExtBuffer);
            }

            if (remainingExtBuffer == null)
            {
                throw new ScriptProgressException(decoder.getRegionInfo(), "[]");
            }

            // rewind as needed
            final int remainingExtBytes = remainingExtBuffer.readableBytes();
            if (remainingExtBytes > 0)
            {
                readExtBuffer.skipBytes(-remainingExtBytes);
            }
        }
    }
}
