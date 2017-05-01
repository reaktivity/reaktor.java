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

import static org.jboss.netty.buffer.ChannelBuffers.dynamicBuffer;

import java.util.Deque;
import java.util.LinkedList;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelSink;
import org.jboss.netty.channel.MessageEvent;
import org.kaazing.k3po.driver.internal.netty.bootstrap.channel.AbstractChannel;
import org.kaazing.k3po.driver.internal.netty.channel.ChannelAddress;

public abstract class NukleusChannel extends AbstractChannel<NukleusChannelConfig>
{
    private int sourceWindow;
    private int targetWindow;
    private long sourceId;
    private long targetId;

    final NukleusReaktor reaktor;
    final Deque<MessageEvent> writeRequests;

    private ChannelBuffer readExtBuffer;
    private ChannelBuffer writeExtBuffer;

    NukleusChannel(
        NukleusServerChannel parent,
        ChannelFactory factory,
        ChannelPipeline pipeline,
        ChannelSink sink,
        NukleusReaktor reaktor)
    {
        super(parent, factory, pipeline, sink, new DefaultNukleusChannelConfig());

        this.reaktor = reaktor;
        this.writeRequests = new LinkedList<>();
        this.readExtBuffer = dynamicBuffer(8192);
        this.writeExtBuffer = dynamicBuffer(8192);
        this.targetId = getId();
    }

    @Override
    public NukleusChannelAddress getLocalAddress()
    {
        return (NukleusChannelAddress) super.getLocalAddress();
    }

    @Override
    public NukleusChannelAddress getRemoteAddress()
    {
        return (NukleusChannelAddress) super.getRemoteAddress();
    }

    @Override
    protected void setBound()
    {
        super.setBound();
    }

    @Override
    protected void setConnected()
    {
        super.setConnected();
    }

    @Override
    protected boolean isReadClosed()
    {
        return super.isReadClosed();
    }

    @Override
    protected boolean isWriteClosed()
    {
        return super.isWriteClosed();
    }

    @Override
    protected boolean setReadClosed()
    {
        return super.setReadClosed();
    }

    @Override
    protected boolean setWriteClosed()
    {
        return super.setWriteClosed();
    }

    @Override
    protected boolean setClosed()
    {
        return super.setClosed();
    }

    @Override
    protected void setRemoteAddress(ChannelAddress remoteAddress)
    {
        super.setRemoteAddress(remoteAddress);
    }

    @Override
    protected void setLocalAddress(ChannelAddress localAddress)
    {
        super.setLocalAddress(localAddress);
    }

    @Override
    public String toString()
    {
        ChannelAddress localAddress = this.getLocalAddress();
        String description = localAddress != null ? localAddress.toString() : super.toString();
        return String.format("%s [sourceId=%d, targetId=%d]", description, sourceId, targetId);
    }

    public int sourceWindow(int update)
    {
        sourceWindow += update;
        assert sourceWindow >= 0;
        return sourceWindow;
    }

    public int sourceWindow()
    {
        return sourceWindow;
    }

    public void sourceId(
        long sourceId)
    {
        this.sourceId = sourceId;
    }

    public long sourceId()
    {
        return sourceId;
    }

    public long targetId()
    {
        return targetId;
    }

    public boolean targetWritable()
    {
        return targetWindow > 0 || !getConfig().hasThrottle();
    }

    public int targetWriteableBytes(
        int writableBytes)
    {
        return getConfig().hasThrottle() ? Math.min(targetWindow, writableBytes) : writableBytes;
    }

    public void targetWrittenBytes(
        int writtenBytes)
    {
        if (getConfig().hasThrottle())
        {
            targetWindow -= writtenBytes;
            assert targetWindow >= 0;
        }
    }

    public void targetWindowUpdate(
        int update)
    {
        if (getConfig().hasThrottle())
        {
            targetWindow += update;
        }
    }

    public ChannelBuffer writeExtBuffer()
    {
        return writeExtBuffer;
    }

    public ChannelBuffer readExtBuffer()
    {
        return readExtBuffer;
    }
}
