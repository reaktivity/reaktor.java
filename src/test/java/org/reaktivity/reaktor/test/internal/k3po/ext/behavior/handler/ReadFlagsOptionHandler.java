/**
 * Copyright 2016-2020 The Reaktivity Project
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
package org.reaktivity.reaktor.test.internal.k3po.ext.behavior.handler;

import java.util.EnumSet;

import org.jboss.netty.channel.ChannelException;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.MessageEvent;
import org.kaazing.k3po.driver.internal.behavior.handler.event.AbstractEventHandler;
import org.reaktivity.reaktor.test.internal.k3po.ext.behavior.NukleusChannel;

public class ReadFlagsOptionHandler extends AbstractEventHandler
{
    private int flags;

    public ReadFlagsOptionHandler(
        int flags)
    {
        super(EnumSet.of(ChannelEventKind.MESSAGE));
        this.flags = flags != -1 ? flags : 3;
    }

    @Override
    public void messageReceived(
        ChannelHandlerContext ctx,
        MessageEvent e) throws Exception
    {
        ChannelFuture handlerFuture = getHandlerFuture();
        NukleusChannel channel = (NukleusChannel) ctx.getChannel();
        try
        {
            if (!handlerFuture.isDone() && channel.readFlags() == this.flags)
            {
                handlerFuture.setSuccess();
            }
            else
            {
                handlerFuture.setFailure(new ChannelException("flags do not match"));
            }
        }
        catch (Exception ex)
        {
            handlerFuture.setFailure(ex);
        }
        super.messageReceived(ctx, e);
    }
}
