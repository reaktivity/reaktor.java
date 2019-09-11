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

import static org.reaktivity.k3po.nukleus.ext.internal.behavior.NukleusThrottleMode.MESSAGE;

import java.util.Deque;
import java.util.LinkedList;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferFactory;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelSink;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.MessageEvent;
import org.kaazing.k3po.driver.internal.netty.bootstrap.channel.AbstractChannel;
import org.kaazing.k3po.driver.internal.netty.channel.ChannelAddress;
import org.reaktivity.k3po.nukleus.ext.internal.behavior.types.control.Capability;

public abstract class NukleusChannel extends AbstractChannel<NukleusChannelConfig>
{
    static final ChannelBufferFactory NATIVE_BUFFER_FACTORY = NukleusByteOrder.NATIVE.toBufferFactory();

    private long routeId;

    private int readableBudget;
    private int writableBudget;
    int writablePadding;

    private int writtenBytes;
    private int acknowledgedBytes;

    private long sourceId;
    private long sourceAuth;
    private long targetId;
    private long targetAuth;

    private int acknowlegedBytesCheckpoint = -1;

    final NukleusReaktor reaktor;
    final Deque<MessageEvent> writeRequests;

    private NukleusExtensionKind readExtKind;
    private ChannelBuffer readExtBuffer;

    private NukleusExtensionKind writeExtKind;
    private ChannelBuffer writeExtBuffer;

    private boolean targetWriteRequestInProgress;

    private ChannelFuture beginOutputFuture;
    private ChannelFuture beginInputFuture;

    private int capabilities;

    NukleusChannel(
        NukleusServerChannel parent,
        ChannelFactory factory,
        ChannelPipeline pipeline,
        ChannelSink sink,
        NukleusReaktor reaktor,
        long targetId)
    {
        super(parent, factory, pipeline, sink, new DefaultNukleusChannelConfig());

        this.reaktor = reaktor;
        this.writeRequests = new LinkedList<>();
        this.routeId = -1L;
        this.targetId = targetId;
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

    public int getLocalScope()
    {
        return Long.SIZE - 1;
    }

    public abstract int getRemoteScope();

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
    protected boolean isReadAborted()
    {
        return super.isReadAborted();
    }

    @Override
    protected boolean isWriteAborted()
    {
        return super.isWriteAborted();
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
    protected boolean setReadAborted()
    {
        return super.setReadAborted();
    }

    @Override
    protected boolean setWriteAborted()
    {
        return super.setWriteAborted();
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

    public void readableBytes(
        int credit)
    {
        readableBudget += credit;
        assert readableBudget >= 0;
    }

    public int readableBytes()
    {
        return Math.max(readableBudget - getConfig().getPadding(), 0);
    }

    public void routeId(
        long routeId)
    {
        this.routeId = routeId;
    }

    public long routeId()
    {
        return routeId;
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

    public void sourceAuth(
        long sourceAuth)
    {
        this.sourceAuth = sourceAuth;
    }

    public long sourceAuth()
    {
        return sourceAuth;
    }

    public void targetAuth(long targetAuth)
    {
        this.targetAuth = targetAuth;
    }

    public long targetAuth()
    {
        return targetAuth;
    }

    public ChannelFuture beginOutputFuture()
    {
        if (beginOutputFuture == null)
        {
            beginOutputFuture = Channels.future(this);
        }

        return beginOutputFuture;
    }

    public ChannelFuture beginInputFuture()
    {
        if (beginInputFuture == null)
        {
            beginInputFuture = Channels.future(this);
        }

        return beginInputFuture;
    }

    public int writableBytes()
    {
        return Math.max(writableBudget - writablePadding, 0);
    }

    public boolean writable()
    {
        return writableBudget > writablePadding || !getConfig().hasThrottle();
    }

    public int writableBytes(
        int writableBytes)
    {
        return getConfig().hasThrottle() ? Math.min(writableBytes(), writableBytes) : writableBytes;
    }

    public void writtenBytes(
        int writtenBytes)
    {
        this.writtenBytes += writtenBytes;
        writableBudget -= writtenBytes + writablePadding;
        assert writablePadding >= 0 && writableBudget >= 0;
    }

    public void writableWindow(
        int credit,
        int padding)
    {
        writableBudget += credit;
        writablePadding = padding;

        // approximation for window acknowledgment
        // does not account for any change to total available window after initial window
        if (writtenBytes > 0)
        {
            acknowledgedBytes += credit;
        }

        if (getConfig().getThrottle() == MESSAGE && targetWriteRequestInProgress)
        {
            if (acknowledgedBytes >= acknowlegedBytesCheckpoint)
            {
                completeWriteRequestIfFullyWritten();
            }
        }
    }

    public void capabilities(
        int capabilities)
    {
        this.capabilities = capabilities;
    }

    public boolean hasCapability(
        Capability capability)
    {
        return (capabilities & (1 << capability.ordinal())) != 0;
    }

    public void targetWriteRequestProgressing()
    {
        if (getConfig().getThrottle() == MESSAGE)
        {
            final MessageEvent writeRequest = writeRequests.peekFirst();
            final ChannelBuffer message = (ChannelBuffer) writeRequest.getMessage();
            acknowlegedBytesCheckpoint = writtenBytes + message.readableBytes();
            targetWriteRequestInProgress = true;
        }
    }

    public ChannelBuffer writeExtBuffer(
        NukleusExtensionKind writeExtKind,
        boolean readonly)
    {
        if (this.writeExtKind != writeExtKind)
        {
            if (readonly)
            {
                return ChannelBuffers.EMPTY_BUFFER;
            }
            else
            {
                if (writeExtBuffer == null)
                {
                    writeExtBuffer = getConfig().getBufferFactory().getBuffer(8192);
                }
                else
                {
                    writeExtBuffer.clear();
                }
                this.writeExtKind = writeExtKind;
            }
        }

        return writeExtBuffer;
    }

    public ChannelBuffer readExtBuffer(
        NukleusExtensionKind readExtKind)
    {
        if (this.readExtKind != readExtKind)
        {
            if (readExtBuffer == null)
            {
                readExtBuffer = getConfig().getBufferFactory().getBuffer(8192);
            }
            else
            {
                readExtBuffer.clear();
            }
            this.readExtKind = readExtKind;
        }

        return readExtBuffer;
    }

    public void targetWriteRequestProgress()
    {
        switch (getConfig().getThrottle())
        {
        case MESSAGE:
            if (targetWriteRequestInProgress && acknowledgedBytes >= acknowlegedBytesCheckpoint)
            {
                completeWriteRequestIfFullyWritten();
            }
            break;
        default:
            completeWriteRequestIfFullyWritten();
            break;
        }
    }

    public boolean isTargetWriteRequestInProgress()
    {
        return targetWriteRequestInProgress;
    }

    private void completeWriteRequestIfFullyWritten()
    {
        final MessageEvent writeRequest = writeRequests.peekFirst();
        final ChannelBuffer message = (ChannelBuffer) writeRequest.getMessage();
        if (!message.readable())
        {
            targetWriteRequestInProgress = false;
            writeRequests.removeFirst();
            writeRequest.getFuture().setSuccess();
        }
    }
}
