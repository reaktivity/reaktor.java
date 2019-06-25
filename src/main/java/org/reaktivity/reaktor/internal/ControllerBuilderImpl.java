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
package org.reaktivity.reaktor.internal;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;

import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.MessageHandler;
import org.agrona.concurrent.broadcast.BroadcastReceiver;
import org.agrona.concurrent.broadcast.CopyBroadcastReceiver;
import org.agrona.concurrent.ringbuffer.ManyToOneRingBuffer;
import org.agrona.concurrent.ringbuffer.RingBuffer;
import org.reaktivity.nukleus.Controller;
import org.reaktivity.nukleus.ControllerBuilder;
import org.reaktivity.nukleus.ControllerSpi;
import org.reaktivity.nukleus.function.MessageFunction;
import org.reaktivity.reaktor.ReaktorConfiguration;
import org.reaktivity.reaktor.internal.layouts.ControlLayout;
import org.reaktivity.reaktor.internal.types.control.CommandFW;
import org.reaktivity.reaktor.internal.types.control.ErrorFW;
import org.reaktivity.reaktor.internal.types.control.FreezeFW;
import org.reaktivity.reaktor.internal.types.control.ResponseFW;
import org.reaktivity.reaktor.internal.types.control.RouteFW;
import org.reaktivity.reaktor.internal.types.control.RoutedFW;
import org.reaktivity.reaktor.internal.types.control.UnrouteFW;
import org.reaktivity.reaktor.internal.types.control.ResolveFW;
import org.reaktivity.reaktor.internal.types.control.ResolvedFW;
import org.reaktivity.reaktor.internal.types.control.UnresolveFW;

public final class ControllerBuilderImpl<T extends Controller> implements ControllerBuilder<T>
{
    private final ReaktorConfiguration config;
    private final Class<T> kind;

    private Function<ControllerSpi, T> factory;

    public ControllerBuilderImpl(
        ReaktorConfiguration config,
        Class<T> kind)
    {
        this.config = config;
        this.kind = kind;
    }

    @Override
    public Class<T> kind()
    {
        return kind;
    }

    @Override
    public ControllerBuilder<T> setFactory(
        Function<ControllerSpi, T> factory)
    {
        this.factory = factory;
        return this;
    }

    @Override
    public T build()
    {
        Objects.requireNonNull(factory, "factory");

        ControllerSpi controllerSpi = new ControllerSpiImpl(config);

        return factory.apply(controllerSpi);
    }

    private final class ControllerSpiImpl implements ControllerSpi
    {
        private final ControlLayout.Builder controlRW = new ControlLayout.Builder();
        private final CommandFW commandRO = new CommandFW();
        private final ResponseFW responseRO = new ResponseFW();

        private final RoutedFW routedRO = new RoutedFW();
        private final ResolvedFW resolvedRO = new ResolvedFW();

        private final RingBuffer conductorCommands;
        private final CopyBroadcastReceiver conductorResponses;
        private final ConcurrentMap<Long, PendingCommand<?>> commandsByCorrelationId;
        private final MessageHandler responseHandler;
        private final ControlLayout control;

        private ControllerSpiImpl(
            ReaktorConfiguration config)
        {
            this.control = controlRW
                    .controlPath(config.directory().resolve("control"))
                    .commandBufferCapacity(config.commandBufferCapacity())
                    .responseBufferCapacity(config.responseBufferCapacity())
                    .readonly(true)
                    .build();

            this.conductorCommands = new ManyToOneRingBuffer(control.commandBuffer());
            this.conductorResponses = new CopyBroadcastReceiver(new BroadcastReceiver(control.responseBuffer()));
            this.commandsByCorrelationId = new ConcurrentHashMap<>();
            this.responseHandler = this::handleResponse;
        }

        @Override
        public long nextCorrelationId()
        {
            return conductorCommands.nextCorrelationId();
        }

        @Override
        public int doProcess()
        {
            return conductorResponses.receive(responseHandler);
        }

        @Override
        public void doClose()
        {
            CloseHelper.quietClose(control);
        }

        @Override
        public CompletableFuture<Long> doResolve(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            assert msgTypeId == ResolveFW.TYPE_ID;

            return doCommand(msgTypeId, buffer, index, length, (t, b, i, l) -> resolvedRO.wrap(b, i, i + l).authorization());
        }

        @Override
        public CompletableFuture<Long> doRoute(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            assert msgTypeId == RouteFW.TYPE_ID;

            return doCommand(msgTypeId, buffer, index, length, (t, b, i, l) -> routedRO.wrap(b, i, i + l).correlationId());
        }

        @Override
        public CompletableFuture<Void> doUnresolve(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            assert msgTypeId == UnresolveFW.TYPE_ID;

            return doCommand(msgTypeId, buffer, index, length);
        }

        @Override
        public CompletableFuture<Void> doUnroute(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            assert msgTypeId == UnrouteFW.TYPE_ID;

            return doCommand(msgTypeId, buffer, index, length);
        }

        @Override
        public CompletableFuture<Void> doFreeze(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            assert msgTypeId == FreezeFW.TYPE_ID;

            return doCommand(msgTypeId, buffer, index, length);
        }

        @Override
        public CompletableFuture<Void> doCommand(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            return doCommand(msgTypeId, buffer, index, length, (t, b, i, l) -> null);
        }

        @Override
        public <R> CompletableFuture<R> doCommand(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length,
            MessageFunction<R> mapper)
        {
            final CompletableFuture<R> promise = new CompletableFuture<>();

            final CommandFW command = commandRO.wrap(buffer, index, index + length);
            final long correlationId = command.correlationId();

            PendingCommand<R> pending = new PendingCommand<>(mapper, promise);
            commandSent(correlationId, pending);

            if (!conductorCommands.write(msgTypeId, buffer, index, length))
            {
                commandSendFailed(correlationId);
            }

            return promise;
        }

        private int handleResponse(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            final ResponseFW response = responseRO.wrap(buffer, index, length);
            long correlationId = response.correlationId();

            PendingCommand<?> command = commandsByCorrelationId.remove(correlationId);

            switch (msgTypeId)
            {
            case ErrorFW.TYPE_ID:
                commandFailed(command, "command failed");
                break;
            default:
                commandSucceeded(command, msgTypeId, buffer, index, length);
                break;
            }

            return 1;
        }

        private <R> void commandSent(
            final long correlationId,
            final PendingCommand<R> command)
        {
            commandsByCorrelationId.put(correlationId, command);
        }

        private <R> boolean commandSucceeded(
            final PendingCommand<R> command,
            final int msgTypeId,
            final DirectBuffer buffer,
            final int index,
            final int length)
        {
            return command != null && command.succeeded(msgTypeId, buffer, index, length);
        }

        private boolean commandSendFailed(
            final long correlationId)
        {
            PendingCommand<?> command = commandsByCorrelationId.remove(correlationId);
            return commandFailed(command, "unable to offer command");
        }

        private boolean commandFailed(
            final PendingCommand<?> command,
            final String message)
        {
            return command != null && command.failed(message);
        }
    }

    private static final class PendingCommand<R>
    {
        final MessageFunction<R> mapper;
        final CompletableFuture<R> promise;

        private PendingCommand(
            final MessageFunction<R> mapper,
            final CompletableFuture<R> promise)
        {
            this.mapper = mapper;
            this.promise = promise;
        }

        private boolean succeeded(
            final int msgTypeId,
            final DirectBuffer buffer,
            final int index,
            final int length)
        {
            return promise.complete(mapper.apply(msgTypeId, buffer, index, length));
        }

        private boolean failed(
            final String message)
        {
            return promise.completeExceptionally(new IllegalStateException(message));
        }
    }
}
