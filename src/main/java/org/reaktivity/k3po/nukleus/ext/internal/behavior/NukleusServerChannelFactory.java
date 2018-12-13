/**
 * Copyright 2016-2018 The Reaktivity Project
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

import java.util.concurrent.atomic.AtomicInteger;

import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ServerChannel;
import org.jboss.netty.channel.ServerChannelFactory;
import org.reaktivity.k3po.nukleus.ext.internal.behavior.types.control.Role;

public class NukleusServerChannelFactory implements ServerChannelFactory
{
    private final NukleusServerChannelSink channelSink;
    private final NukleusReaktorPool reaktorPool;
    private final AtomicInteger initialId;

    public NukleusServerChannelFactory(
        NukleusReaktorPool reaktorPool)
    {
        this.channelSink = new NukleusServerChannelSink();
        this.reaktorPool = reaktorPool;
        this.initialId = new AtomicInteger();
    }

    @Override
    public ServerChannel newChannel(
        ChannelPipeline pipeline)
    {
        final long routeId = 0xffff_ffffL << 8 | Role.SERVER.ordinal() << 7 | (initialId.incrementAndGet() & 0x00ff_ffffL);

        return new NukleusServerChannel(this, pipeline, channelSink, reaktorPool.nextReaktor(), routeId);
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
