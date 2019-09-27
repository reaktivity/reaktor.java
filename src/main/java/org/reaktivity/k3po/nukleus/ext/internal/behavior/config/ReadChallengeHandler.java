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

import static org.reaktivity.k3po.nukleus.ext.internal.behavior.NullChannelBuffer.CHALLENGE_BUFFER;

import java.util.EnumSet;

import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.MessageEvent;
import org.kaazing.k3po.driver.internal.behavior.handler.codec.ConfigDecoder;

public class ReadChallengeHandler extends AbstractReadExtHandler
{
    public ReadChallengeHandler(
        ConfigDecoder decoder)
    {
        super(EnumSet.of(ChannelEventKind.MESSAGE), decoder);
    }

    @Override
    public void messageReceived(
        ChannelHandlerContext ctx,
        MessageEvent e) throws Exception
    {
        ChannelFuture handlerFuture = getHandlerFuture();

        try
        {
            if (!handlerFuture.isDone() &&
                e.getMessage() == CHALLENGE_BUFFER)
            {
                doReadExtension(ctx);
                handlerFuture.setSuccess();
            }
            else
            {
                super.messageReceived(ctx, e);
            }
        }
        catch (Exception ex)
        {
            handlerFuture.setFailure(ex);
        }
    }
}
