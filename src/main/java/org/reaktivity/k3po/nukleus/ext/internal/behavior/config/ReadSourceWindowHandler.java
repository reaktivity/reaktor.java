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

import static java.lang.String.format;
import static org.jboss.netty.buffer.ChannelBuffers.buffer;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.kaazing.k3po.driver.internal.behavior.ScriptProgressException;
import org.kaazing.k3po.driver.internal.behavior.handler.codec.MessageDecoder;
import org.kaazing.k3po.driver.internal.behavior.handler.command.AbstractCommandHandler;
import org.reaktivity.k3po.nukleus.ext.internal.behavior.NukleusChannel;

public class ReadSourceWindowHandler extends AbstractCommandHandler
{
    private final MessageDecoder decoder;

    public ReadSourceWindowHandler(
        MessageDecoder decoder)
    {
        this.decoder = decoder;
    }

    @Override
    protected void invokeCommand(
        ChannelHandlerContext ctx) throws Exception
    {
        try
        {
            NukleusChannel channel = (NukleusChannel) ctx.getChannel();
            int window = channel.sourceWindow();

            ChannelBuffer buffer = buffer(Integer.BYTES);
            buffer.writeInt(window);

            if (decoder.decodeLast(buffer) == null)
            {
                throw new ScriptProgressException(decoder.getRegionInfo(), Integer.toString(window));
            }

            getHandlerFuture().setSuccess();
        }
        catch (Exception ex)
        {
            getHandlerFuture().setFailure(ex);
        }
    }

    @Override
    protected StringBuilder describe(StringBuilder sb)
    {
        return sb.append(format("read nukleus:window %s", decoder));
    }

}
