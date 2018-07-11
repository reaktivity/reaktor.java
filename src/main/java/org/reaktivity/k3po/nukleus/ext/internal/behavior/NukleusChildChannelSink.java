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

import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.MessageEvent;
import org.kaazing.k3po.driver.internal.netty.bootstrap.channel.AbstractChannelSink;
import org.kaazing.k3po.driver.internal.netty.channel.FlushEvent;
import org.kaazing.k3po.driver.internal.netty.channel.ReadAbortEvent;
import org.kaazing.k3po.driver.internal.netty.channel.ShutdownOutputEvent;
import org.kaazing.k3po.driver.internal.netty.channel.WriteAbortEvent;

public class NukleusChildChannelSink extends AbstractChannelSink
{
    @Override
    protected void abortOutputRequested(
        ChannelPipeline pipeline,
        WriteAbortEvent evt) throws Exception
    {
        NukleusChannel channel = (NukleusChannel) evt.getChannel();
        ChannelFuture abortFuture = evt.getFuture();

        if (!channel.isWriteClosed())
        {
            channel.reaktor.abortOutput(channel, abortFuture);
        }
    }

    @Override
    protected void abortInputRequested(
        ChannelPipeline pipeline,
        ReadAbortEvent evt) throws Exception
    {
        NukleusChannel channel = (NukleusChannel) evt.getChannel();
        ChannelFuture abortFuture = evt.getFuture();

        if (!channel.isReadClosed())
        {
            channel.reaktor.abortInput(channel, abortFuture);
        }
    }

    @Override
    protected void writeRequested(
        ChannelPipeline pipeline,
        MessageEvent evt) throws Exception
    {
        NukleusChannel channel = (NukleusChannel) evt.getChannel();
        if (!channel.isWriteClosed())
        {
            channel.reaktor.write(evt);
        }
    }

    @Override
    protected void flushRequested(
        ChannelPipeline pipeline,
        FlushEvent evt) throws Exception
    {
        NukleusChannel channel = (NukleusChannel) evt.getChannel();
        if (!channel.isWriteClosed())
        {
            ChannelFuture flushFuture = evt.getFuture();
            channel.reaktor.flush(channel, flushFuture);
        }
    }

    @Override
    protected void shutdownOutputRequested(
        ChannelPipeline pipeline,
        ShutdownOutputEvent evt) throws Exception
    {
        NukleusChannel channel = (NukleusChannel) evt.getChannel();
        if (!channel.isWriteClosed())
        {
            ChannelFuture shutdownFuture = evt.getFuture();
            channel.reaktor.shutdownOutput(channel, shutdownFuture);
        }
    }

    @Override
    protected void closeRequested(
        ChannelPipeline pipeline,
        ChannelStateEvent evt) throws Exception
    {
        NukleusChannel channel = (NukleusChannel) evt.getChannel();
        ChannelFuture handlerFuture = evt.getFuture();
        channel.reaktor.close(channel, handlerFuture);
    }

    @Override
    protected void disconnectRequested(
        ChannelPipeline pipeline,
        ChannelStateEvent evt) throws Exception
    {
        NukleusChannel channel = (NukleusChannel) evt.getChannel();
        if (!channel.isWriteClosed())
        {
            ChannelFuture handlerFuture = evt.getFuture();
            channel.reaktor.close(channel, handlerFuture);
        }
    }
}
