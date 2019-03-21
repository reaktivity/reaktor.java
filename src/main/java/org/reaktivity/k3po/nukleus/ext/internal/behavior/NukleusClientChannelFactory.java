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
package org.reaktivity.k3po.nukleus.ext.internal.behavior;

import java.util.concurrent.atomic.AtomicLong;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelPipeline;

public class NukleusClientChannelFactory implements ChannelFactory
{
    private final NukleusClientChannelSink channelSink;
    private final NukleusReaktorPool reaktorPool;
    private final AtomicLong initialId;

    public NukleusClientChannelFactory(
        NukleusReaktorPool reaktorPool)
    {
        this.channelSink = new NukleusClientChannelSink();
        this.reaktorPool = reaktorPool;
        this.initialId = new AtomicLong(((long)(Long.SIZE - 1) << 56) | 0x0000_0000_0000_0001L);
    }

    @Override
    public Channel newChannel(
        ChannelPipeline pipeline)
    {
        final long targetId = initialId.addAndGet(2L);
        return new NukleusClientChannel(this, pipeline, channelSink, reaktorPool.nextReaktor(), targetId);
    }

    @Override
    public void shutdown()
    {
    }

    @Override
    public void releaseExternalResources()
    {
    }
}
