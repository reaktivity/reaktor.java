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
package org.reaktivity.reaktor.test.internal.k3po.ext.behavior;

import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelStateEvent;
import org.kaazing.k3po.driver.internal.netty.bootstrap.channel.AbstractServerChannelSink;

public class NukleusServerChannelSink extends AbstractServerChannelSink<NukleusServerChannel>
{
    @Override
    protected void bindRequested(
        ChannelPipeline pipeline,
        ChannelStateEvent evt) throws Exception
    {
        NukleusServerChannel serverChannel = (NukleusServerChannel) evt.getChannel();
        NukleusChannelAddress localAddress = (NukleusChannelAddress) evt.getValue();
        ChannelFuture bindFuture = evt.getFuture();

        serverChannel.reaktor.bind(serverChannel, localAddress, bindFuture);
    }

    @Override
    protected void unbindRequested(
        ChannelPipeline pipeline,
        ChannelStateEvent evt)
            throws Exception
    {
        final NukleusServerChannel serverChannel = (NukleusServerChannel) evt.getChannel();
        final ChannelFuture unbindFuture = evt.getFuture();

        serverChannel.reaktor.unbind(serverChannel, unbindFuture);
    }

    @Override
    protected void closeRequested(
        ChannelPipeline pipeline,
        ChannelStateEvent evt)
            throws Exception
    {
        final NukleusServerChannel serverChannel = (NukleusServerChannel) evt.getChannel();

        serverChannel.reaktor.close(serverChannel);
    }

}
