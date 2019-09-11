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

import static java.lang.String.format;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;
import static org.jboss.netty.channel.Channels.future;
import static org.jboss.netty.channel.Channels.write;
import static org.reaktivity.k3po.nukleus.ext.internal.behavior.NullChannelBuffer.CHALLENGE_BUFFER;

import java.util.List;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.kaazing.k3po.driver.internal.behavior.ScriptProgressException;
import org.kaazing.k3po.driver.internal.behavior.handler.codec.ConfigEncoder;
import org.kaazing.k3po.driver.internal.behavior.handler.command.AbstractCommandHandler;

public class WriteChallengeHandler extends AbstractCommandHandler
{
    private final List<ConfigEncoder> encoders;

    public WriteChallengeHandler(
        ConfigEncoder encoder)
    {
        this(singletonList(encoder));
    }

    public WriteChallengeHandler(
        List<ConfigEncoder> encoders)
    {
        this.encoders = requireNonNull(encoders, "encoders");
    }

    @Override
    protected void invokeCommand(
        ChannelHandlerContext ctx) throws Exception
    {
        Channel channel = ctx.getChannel();
        for (ConfigEncoder encoder : encoders)
        {
            encoder.encode(channel);
        }

        final ChannelFuture handlerFuture = getHandlerFuture();
        ChannelFuture challengeFuture = future(ctx.getChannel());
        write(ctx, challengeFuture, CHALLENGE_BUFFER);
        challengeFuture.addListener(new ChannelFutureListener()
        {
            @Override
            public void operationComplete(
                ChannelFuture future) throws Exception
            {
                if (future.isSuccess())
                {
                    handlerFuture.setSuccess();
                }
                else
                {
                    handlerFuture.setFailure(new ScriptProgressException(getRegionInfo(), future.getCause().getMessage()));
                }
            }
        });
    }

    @Override
    protected StringBuilder describe(
        StringBuilder sb)
    {
        return sb.append(format("write %s", encoders));
    }
}
