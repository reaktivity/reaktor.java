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

import static java.lang.Thread.currentThread;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.jboss.netty.channel.Channels.fireChannelBound;
import static org.jboss.netty.channel.Channels.fireChannelUnbound;
import static org.jboss.netty.channel.Channels.fireExceptionCaught;
import static org.reaktivity.k3po.nukleus.ext.internal.behavior.NukleusTransmission.SIMPLEX;

import java.util.Deque;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.agrona.CloseHelper;
import org.agrona.collections.ArrayUtil;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.jboss.netty.channel.ChannelException;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.util.ExternalResourceReleasable;
import org.reaktivity.k3po.nukleus.ext.internal.NukleusExtConfiguration;

public final class NukleusReaktor implements Runnable, ExternalResourceReleasable
{
    private static final long MAX_PARK_NS = MILLISECONDS.toNanos(100L);
    private static final long MIN_PARK_NS = MILLISECONDS.toNanos(1L);
    private static final int MAX_YIELDS = 30;
    private static final int MAX_SPINS = 20;

    private final NukleusExtConfiguration config;
    private final Deque<Runnable> taskQueue;
    private final AtomicLong traceIds;
    private final Int2ObjectHashMap<NukleusScope> scopesByIndex;
    private final Map<String, Integer> scopeIndexByReceiverAddress;
    private final LabelManager labels;

    private final CountDownLatch shutdownLatch = new CountDownLatch(1);
    private final AtomicBoolean shutdown = new AtomicBoolean();

    private NukleusScope[] scopes;
    private final AtomicReference<Thread> thread;

    NukleusReaktor(
        NukleusExtConfiguration config)
    {
        this.config = config;
        this.scopesByIndex = new Int2ObjectHashMap<>();
        this.scopeIndexByReceiverAddress = new ConcurrentHashMap<>();
        this.taskQueue = new ConcurrentLinkedDeque<>();
        this.traceIds = new AtomicLong(Long.MIN_VALUE); // negative
        this.scopes = new NukleusScope[0];
        this.labels = new LabelManager(config.directory());
        this.thread = new AtomicReference<>();
    }

    public void bind(
        NukleusServerChannel serverChannel,
        NukleusChannelAddress localAddress,
        ChannelFuture bindFuture)
    {
        submitTask(new BindServerTask(serverChannel, localAddress, bindFuture), true);
    }

    public void unbind(
        NukleusServerChannel serverChannel,
        ChannelFuture unbindFuture)
    {
        submitTask(new UnbindServerTask(serverChannel, unbindFuture), true);
    }

    public void close(
        NukleusServerChannel serverChannel)
    {
        submitTask(new CloseServerTask(serverChannel), true);
    }

    public void connect(
        NukleusClientChannel channel,
        NukleusChannelAddress remoteAddress,
        ChannelFuture connectFuture)
    {
        submitTask(new ConnectClientTask(channel, remoteAddress, connectFuture));
    }

    public void abortOutput(
        NukleusChannel channel,
        ChannelFuture handlerFuture)
    {
        submitTask(new AbortOutputTask(channel, handlerFuture));
    }

    public void abortInput(
        NukleusChannel channel,
        ChannelFuture handlerFuture)
    {
        submitTask(new AbortInputTask(channel, handlerFuture));
    }

    public void write(
        MessageEvent writeRequest)
    {
        submitTask(new WriteTask(writeRequest));
    }

    public void flush(
        NukleusChannel channel,
        ChannelFuture handlerFuture)
    {
        submitTask(new FlushTask(channel, handlerFuture));
    }

    public void shutdownOutput(
        NukleusChannel channel,
        ChannelFuture handlerFuture)
    {
        submitTask(new ShutdownOutputTask(channel, handlerFuture));
    }

    public void close(
        NukleusChannel channel,
        ChannelFuture handlerFuture)
    {
        submitTask(new CloseTask(channel, handlerFuture));
    }

    @Override
    public void run()
    {
        final IdleStrategy idleStrategy = new BackoffIdleStrategy(MAX_SPINS, MAX_YIELDS, MIN_PARK_NS, MAX_PARK_NS);

        if (!thread.compareAndSet(null, Thread.currentThread()))
        {
            return;
        }

        while (!shutdown.get())
        {
            int workCount = 0;

            workCount += executeTasks();
            workCount += readMessages();

            idleStrategy.idle(workCount);
        }

        // ensure task queue is drained
        // so that all channels are closed
        executeTasks();

        shutdownLatch.countDown();
    }

    public void shutdown()
    {
        if (shutdown.compareAndSet(false, true))
        {
            try
            {
                shutdownLatch.await();

                for (int i=0; i < scopes.length; i++)
                {
                    CloseHelper.quietClose(scopes[i]);
                }
            }
            catch (InterruptedException ex)
            {
                currentThread().interrupt();
            }
        }
    }

    @Override
    public void releaseExternalResources()
    {
        shutdown();
    }

    private int executeTasks()
    {
        int workCount = 0;

        Runnable task;
        while ((task = taskQueue.poll()) != null)
        {
            task.run();
            workCount++;
        }

        return workCount;
    }

    private int readMessages()
    {
        int workCount = 0;

        for (int i=0; i < scopes.length; i++)
        {
            workCount += scopes[i].process();
        }

        return workCount;
    }

    private NukleusScope newScope(
        int scopeIndex)
    {
        NukleusScope scope = new NukleusScope(config, labels, scopeIndex, this::lookupTargetIndex,
                System::nanoTime, traceIds::incrementAndGet);
        this.scopes = ArrayUtil.add(this.scopes, scope);
        return scope;
    }

    private void submitTask(
        Runnable task)
    {
        submitTask(task, false);
    }

    private void submitTask(
        Runnable task,
        boolean immediateIfAligned)
    {
        if (immediateIfAligned && thread.get() == Thread.currentThread())
        {
            task.run();
        }
        else
        {
            taskQueue.offer(task);
        }
    }

    private int lookupTargetIndex(
        String address)
    {
        return scopeIndexByReceiverAddress.getOrDefault(address, 0);
    }

    private final class BindServerTask implements Runnable
    {
        private final NukleusServerChannel serverChannel;
        private final NukleusChannelAddress localAddress;
        private final ChannelFuture bindFuture;

        private BindServerTask(
            NukleusServerChannel serverChannel,
            NukleusChannelAddress localAddress,
            ChannelFuture bindFuture)
        {
            this.serverChannel = serverChannel;
            this.localAddress = localAddress;
            this.bindFuture = bindFuture;
        }

        @Override
        public void run()
        {
            try
            {
                NukleusReaktor reaktor = serverChannel.reaktor;

                int scopeIndex = serverChannel.getLocalScope();
                NukleusScope scope = reaktor.scopesByIndex.computeIfAbsent(scopeIndex, reaktor::newScope);

                String receiverAddress = localAddress.getReceiverAddress();
                long authorization = localAddress.getAuthorization();
                scope.doRoute(receiverAddress, authorization, serverChannel);

                scopeIndexByReceiverAddress.put(receiverAddress, scopeIndex);

                serverChannel.setLocalAddress(localAddress);
                serverChannel.setBound();

                fireChannelBound(serverChannel, localAddress);
                bindFuture.setSuccess();
            }
            catch (Exception ex)
            {
                bindFuture.setFailure(ex);
            }
        }
    }

    private final class UnbindServerTask implements Runnable
    {
        private final NukleusServerChannel serverChannel;
        private final ChannelFuture unbindFuture;

        private UnbindServerTask(
            NukleusServerChannel serverChannel,
            ChannelFuture unbindFuture)
        {
            this.serverChannel = serverChannel;
            this.unbindFuture = unbindFuture;
        }

        @Override
        public void run()
        {
            try
            {
                NukleusReaktor reaktor = serverChannel.reaktor;
                NukleusChannelAddress localAddress = serverChannel.getLocalAddress();

                int scopeIndex = serverChannel.getLocalScope();
                NukleusScope scope = reaktor.scopesByIndex.computeIfAbsent(scopeIndex, reaktor::newScope);

                String receiverAddress = localAddress.getReceiverAddress();
                long authorization = localAddress.getAuthorization();
                scope.doUnroute(receiverAddress, authorization, serverChannel);

                serverChannel.setLocalAddress(null);
                fireChannelUnbound(serverChannel);
                unbindFuture.setSuccess();
            }
            catch (Exception ex)
            {
                unbindFuture.setFailure(ex);
            }
        }
    }

    private final class CloseServerTask implements Runnable
    {
        private final NukleusServerChannel serverChannel;

        private CloseServerTask(
            NukleusServerChannel serverChannel)
        {
            this.serverChannel = serverChannel;
        }

        @Override
        public void run()
        {
            try
            {
                NukleusReaktor reaktor = serverChannel.reaktor;
                NukleusChannelAddress localAddress = serverChannel.getLocalAddress();

                if (localAddress != null)
                {
                    int scopeIndex = serverChannel.getLocalScope();
                    NukleusScope scope = reaktor.scopesByIndex.computeIfAbsent(scopeIndex, reaktor::newScope);

                    String receiverAddress = localAddress.getReceiverAddress();
                    long authorization = localAddress.getAuthorization();
                    scope.doUnroute(receiverAddress, authorization, serverChannel);

                    serverChannel.setLocalAddress(null);
                    fireChannelUnbound(serverChannel);
                }

                serverChannel.setClosed();
            }
            catch (ChannelException ex)
            {
                fireExceptionCaught(serverChannel, ex);
            }
        }
    }

    private final class ConnectClientTask implements Runnable
    {
        private final NukleusClientChannel clientChannel;
        private final NukleusChannelAddress remoteAddress;
        private final ChannelFuture connectFuture;

        private ConnectClientTask(
            NukleusClientChannel clientChannel,
            NukleusChannelAddress remoteAddress,
            ChannelFuture connectFuture)
        {
            this.clientChannel = clientChannel;
            this.remoteAddress = remoteAddress;
            this.connectFuture = connectFuture;
        }

        @Override
        public void run()
        {
            final String replyAddress = remoteAddress.getSenderAddress();
            final NukleusChannelAddress localAddress = remoteAddress.newReplyToAddress(replyAddress);

            if (!clientChannel.isBound())
            {
                clientChannel.setLocalAddress(localAddress);
                clientChannel.setBound();
                fireChannelBound(clientChannel, localAddress);
            }

            try
            {
                NukleusReaktor reaktor = clientChannel.reaktor;
                int scopeIndex = clientChannel.getLocalScope();

                final NukleusChannelConfig clientConfig = clientChannel.getConfig();
                if (clientConfig.getTransmission() == SIMPLEX)
                {
                    clientChannel.setReadClosed();
                }

                NukleusScope scope = reaktor.scopesByIndex.computeIfAbsent(scopeIndex, reaktor::newScope);
                scope.doConnect(clientChannel, localAddress, remoteAddress, connectFuture);

                connectFuture.addListener(new ChannelFutureListener()
                {
                    @Override
                    public void operationComplete(
                        ChannelFuture future) throws Exception
                    {
                        if (future.isCancelled())
                        {
                            submitTask(new ConnectAbortTask(clientChannel, remoteAddress));
                        }
                    }
                });
            }
            catch (Exception ex)
            {
                connectFuture.setFailure(ex);
            }
        }

        private final class ConnectAbortTask implements Runnable
        {
            private final NukleusClientChannel clientChannel;
            private final NukleusChannelAddress remoteAddress;

            private ConnectAbortTask(
                NukleusClientChannel clientChannel,
                NukleusChannelAddress remoteAddress)
            {
                this.clientChannel = clientChannel;
                this.remoteAddress = remoteAddress;
            }

            @Override
            public void run()
            {
                NukleusReaktor reaktor = clientChannel.reaktor;
                int scopeIndex = clientChannel.getLocalScope();

                NukleusScope scope = reaktor.scopesByIndex.computeIfAbsent(scopeIndex, reaktor::newScope);
                scope.doConnectAbort(clientChannel, remoteAddress);
            }
        }
    }

    private final class AbortOutputTask implements Runnable
    {
        private final NukleusChannel channel;
        private final ChannelFuture handlerFuture;

        private AbortOutputTask(
            NukleusChannel channel,
            ChannelFuture handlerFuture)
        {
            this.channel = channel;
            this.handlerFuture = handlerFuture;
        }

        @Override
        public void run()
        {
            try
            {
                if (!channel.isWriteClosed())
                {
                    NukleusReaktor reaktor = channel.reaktor;
                    int scopeIndex = channel.getLocalScope();  // ??

                    NukleusScope scope = reaktor.scopesByIndex.computeIfAbsent(scopeIndex, reaktor::newScope);
                    scope.doAbortOutput(channel, handlerFuture);
                }
            }
            catch (Exception ex)
            {
                handlerFuture.setFailure(ex);
            }
        }
    }

    private final class AbortInputTask implements Runnable
    {
        private final NukleusChannel channel;
        private final ChannelFuture handlerFuture;

        private AbortInputTask(
            NukleusChannel channel,
            ChannelFuture handlerFuture)
        {
            this.channel = channel;
            this.handlerFuture = handlerFuture;
        }

        @Override
        public void run()
        {
            try
            {
                if (!channel.isReadClosed())
                {
                    NukleusReaktor reaktor = channel.reaktor;
                    int scopeIndex = channel.getLocalScope();
                    NukleusScope scope = reaktor.scopesByIndex.computeIfAbsent(scopeIndex, reaktor::newScope);
                    scope.doAbortInput(channel, handlerFuture);
                }
            }
            catch (Exception ex)
            {
                handlerFuture.setFailure(ex);
            }
        }
    }

    private final class WriteTask implements Runnable
    {
        private final MessageEvent writeRequest;

        private WriteTask(
            MessageEvent writeRequest)
        {
            this.writeRequest = writeRequest;
        }

        @Override
        public void run()
        {
            try
            {
                NukleusChannel channel = (NukleusChannel) writeRequest.getChannel();
                if (!channel.isWriteClosed())
                {
                    NukleusReaktor reaktor = channel.reaktor;
                    int scopeIndex = channel.getLocalScope();

                    NukleusScope scope = reaktor.scopesByIndex.computeIfAbsent(scopeIndex, reaktor::newScope);
                    scope.doWrite(channel, writeRequest);
                }
            }
            catch (Exception ex)
            {
                ChannelFuture writeFuture = writeRequest.getFuture();
                writeFuture.setFailure(ex);
            }
        }
    }

    private final class FlushTask implements Runnable
    {
        private final NukleusChannel channel;
        private final ChannelFuture flushFuture;

        private FlushTask(
            NukleusChannel channel,
            ChannelFuture future)
        {
            this.channel = channel;
            this.flushFuture = future;
        }

        @Override
        public void run()
        {
            try
            {
                if (!channel.isWriteClosed())
                {
                    NukleusReaktor reaktor = channel.reaktor;
                    int scopeIndex = channel.getLocalScope();

                    NukleusScope scope = reaktor.scopesByIndex.computeIfAbsent(scopeIndex, reaktor::newScope);
                    scope.doFlush(channel, flushFuture);
                }
            }
            catch (Exception ex)
            {
                flushFuture.setFailure(ex);
            }
        }
    }

    private final class ShutdownOutputTask implements Runnable
    {
        private final NukleusChannel channel;
        private final ChannelFuture handlerFuture;

        private ShutdownOutputTask(
            NukleusChannel channel,
            ChannelFuture handlerFuture)
        {
            this.channel = channel;
            this.handlerFuture = handlerFuture;
        }

        @Override
        public void run()
        {
            try
            {
                if (!channel.isWriteClosed())
                {
                    NukleusReaktor reaktor = channel.reaktor;
                    int scopeIndex = channel.getLocalScope();

                    NukleusScope scope = reaktor.scopesByIndex.computeIfAbsent(scopeIndex, reaktor::newScope);
                    scope.doShutdownOutput(channel, handlerFuture);
                }
            }
            catch (Exception ex)
            {
                handlerFuture.setFailure(ex);
            }
        }
    }

    private final class CloseTask implements Runnable
    {
        private final NukleusChannel channel;
        private final ChannelFuture handlerFuture;

        private CloseTask(
            NukleusChannel channel,
            ChannelFuture handlerFuture)
        {
            this.channel = channel;
            this.handlerFuture = handlerFuture;
        }

        @Override
        public void run()
        {
            try
            {
                NukleusReaktor reaktor = channel.reaktor;
                NukleusChannelAddress remoteAddress = channel.getRemoteAddress();

                if (remoteAddress != null)
                {
                    int scopeIndex = channel.getLocalScope();

                    NukleusScope scope = reaktor.scopesByIndex.computeIfAbsent(scopeIndex, reaktor::newScope);
                    scope.doClose(channel, handlerFuture);
                }
            }
            catch (ChannelException ex)
            {
                fireExceptionCaught(channel, ex);
            }
        }
    }
}
