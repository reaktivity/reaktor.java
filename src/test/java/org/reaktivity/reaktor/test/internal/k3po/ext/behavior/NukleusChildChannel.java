/**
 * Copyright 2016-2021 The Reaktivity Project
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

import static org.jboss.netty.channel.Channels.fireChannelOpen;

import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelSink;

public final class NukleusChildChannel extends NukleusChannel
{
    private final int remoteScope;

    NukleusChildChannel(
        NukleusServerChannel parent,
        ChannelFactory factory,
        ChannelPipeline pipeline,
        ChannelSink sink,
        long sourceId,
        long targetId)
    {
        super(parent, factory, pipeline, sink, parent.reaktor, targetId);
        this.getConfig().setUpdate(parent.getConfig().getUpdate());

        setConnected();

        fireChannelOpen(this);

        this.remoteScope = (int)(sourceId >> 56) & 0x7f;
    }

    @Override
    public NukleusServerChannel getParent()
    {
        return NukleusServerChannel.class.cast(super.getParent());
    }

    @Override
    public int getRemoteScope()
    {
        return remoteScope;
    }
}
