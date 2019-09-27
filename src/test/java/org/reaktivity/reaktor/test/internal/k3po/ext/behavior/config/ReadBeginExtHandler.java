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
package org.reaktivity.reaktor.test.internal.k3po.ext.behavior.config;

import static org.kaazing.k3po.driver.internal.behavior.handler.event.AbstractEventHandler.ChannelEventKind.BOUND;
import static org.kaazing.k3po.driver.internal.behavior.handler.event.AbstractEventHandler.ChannelEventKind.CONNECTED;
import static org.kaazing.k3po.driver.internal.behavior.handler.event.AbstractEventHandler.ChannelEventKind.INTEREST_OPS;
import static org.reaktivity.reaktor.test.internal.k3po.ext.behavior.NukleusTransmission.HALF_DUPLEX;

import java.util.EnumSet;

import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.kaazing.k3po.driver.internal.behavior.handler.codec.ConfigDecoder;
import org.reaktivity.reaktor.test.internal.k3po.ext.behavior.NukleusChannel;

public class ReadBeginExtHandler extends AbstractReadExtHandler
{
    public ReadBeginExtHandler(
        ConfigDecoder decoder)
    {
        super(EnumSet.of(BOUND, CONNECTED, INTEREST_OPS),
                EnumSet.of(BOUND, CONNECTED, INTEREST_OPS),
                decoder);
    }

    @Override
    public void channelBound(
        ChannelHandlerContext ctx,
        ChannelStateEvent e) throws Exception
    {
        // eager server
        if (ctx.getChannel().getParent() != null)
        {
            doReadExtension(ctx);
        }

        super.channelBound(ctx, e);
    }

    @Override
    public void channelConnected(
        ChannelHandlerContext ctx,
        ChannelStateEvent e) throws Exception
    {
        // lazy client (not HALF_DUPLEX)
        NukleusChannel channel = (NukleusChannel) ctx.getChannel();
        if (channel.getParent() == null &&
            channel.getConfig().getTransmission() != HALF_DUPLEX)
        {
            doReadExtension(ctx);
        }

        super.channelConnected(ctx, e);
    }

    @Override
    public void channelInterestChanged(
        ChannelHandlerContext ctx,
        ChannelStateEvent e) throws Exception
    {
        // lazy client (HALF_DUPLEX)
        NukleusChannel channel = (NukleusChannel) ctx.getChannel();
        if (channel.getParent() == null &&
            channel.getConfig().getTransmission() == HALF_DUPLEX)
        {
            doReadExtension(ctx);
        }

        super.channelInterestChanged(ctx, e);
    }
}
