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
package org.reaktivity.k3po.nukleus.ext.internal.behavior.option;

import static java.lang.String.format;

import java.util.function.Supplier;

import org.jboss.netty.channel.ChannelHandlerContext;
import org.kaazing.k3po.driver.internal.behavior.handler.command.AbstractCommandHandler;
import org.reaktivity.k3po.nukleus.ext.internal.behavior.NukleusChannel;
import org.reaktivity.k3po.nukleus.ext.internal.behavior.NukleusChannelConfig;

public class ReadPartitionHandler extends AbstractCommandHandler
{
    private final Supplier<String> partition;

    public ReadPartitionHandler(
        Supplier<String> partition)
    {
        this.partition = partition;
    }

    @Override
    protected void invokeCommand(
        ChannelHandlerContext ctx) throws Exception
    {
        NukleusChannel channel = (NukleusChannel) ctx.getChannel();
        NukleusChannelConfig channelConfig = channel.getConfig();
        channelConfig.setReadPartition(partition.get());
    }

    @Override
    protected StringBuilder describe(StringBuilder sb)
    {
        return sb.append(format("read option nukleus:partition %s", partition.get()));
    }

}
