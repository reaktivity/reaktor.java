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
package org.reaktivity.k3po.nukleus.ext.internal.behavior;

import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ServerChannel;
import org.jboss.netty.channel.ServerChannelFactory;

public class NukleusServerChannelFactory implements ServerChannelFactory
{
    private final NukleusServerChannelSink channelSink;
    private final NukleusReaktorPool reaktorPool;

    public NukleusServerChannelFactory(
        NukleusReaktorPool reaktorPool)
    {
        this.channelSink = new NukleusServerChannelSink();
        this.reaktorPool = reaktorPool;
    }

    @Override
    public ServerChannel newChannel(
        ChannelPipeline pipeline)
    {
        return new NukleusServerChannel(this, pipeline, channelSink, reaktorPool.nextReaktor());
    }

    @Override
    public void shutdown()
    {
    }

    @Override
    public void releaseExternalResources()
    {
        shutdown();
    }
}
