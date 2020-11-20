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

import org.jboss.netty.channel.ChannelHandlerContext;
import org.kaazing.k3po.driver.internal.behavior.handler.command.AbstractCommandHandler;
import org.reaktivity.reaktor.test.internal.k3po.ext.behavior.NukleusChannel;

public class WriteFlagsOptionHandler extends AbstractCommandHandler
{
    private int flags;

    public WriteFlagsOptionHandler(
        int flags)
    {
        this.flags = flags;
    }

    @Override
    protected void invokeCommand(
        ChannelHandlerContext ctx) throws Exception
    {
        NukleusChannel channel = (NukleusChannel) ctx.getChannel();
        channel.flags(flags);
        getHandlerFuture().setSuccess();
    }
}
