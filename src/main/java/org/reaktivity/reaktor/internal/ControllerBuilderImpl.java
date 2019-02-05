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
import org.reaktivity.reaktor.internal.layouts.ControlLayout;
import org.reaktivity.reaktor.internal.types.control.CommandFW;
import org.reaktivity.reaktor.internal.types.control.ErrorFW;
import org.reaktivity.reaktor.internal.types.control.FreezeFW;
import org.reaktivity.reaktor.internal.types.control.FrozenFW;
import org.reaktivity.reaktor.internal.types.control.RouteFW;
import org.reaktivity.reaktor.internal.types.control.RoutedFW;
import org.reaktivity.reaktor.internal.types.control.UnrouteFW;
import org.reaktivity.reaktor.internal.types.control.UnroutedFW;
import org.reaktivity.reaktor.internal.types.control.auth.ResolveFW;
import org.reaktivity.reaktor.internal.types.control.auth.ResolvedFW;
import org.reaktivity.reaktor.internal.types.control.auth.UnresolveFW;
import org.reaktivity.reaktor.internal.types.control.auth.UnresolvedFW;

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
        private final RoutedFW routedRO = new RoutedFW();
        private final ResolvedFW resolvedRO = new ResolvedFW();
        private final UnresolvedFW unresolvedRO = new UnresolvedFW();
        private final UnroutedFW unroutedRO = new UnroutedFW();
        private final FrozenFW frozenRO = new FrozenFW();
        private final ErrorFW errorRO = new ErrorFW();

        private final RingBuffer conductorCommands;
        private final CopyBroadcastReceiver conductorResponses;
        private final ConcurrentMap<Long, CompletableFuture<?>> promisesByCorrelationId;
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
            this.promisesByCorrelationId = new ConcurrentHashMap<>();
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

            return doCommand(msgTypeId, buffer, index, length);
        }

        @Override
        public CompletableFuture<Long> doRoute(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            assert msgTypeId == RouteFW.TYPE_ID;

            return doCommand(msgTypeId, buffer, index, length);
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

        private <R> CompletableFuture<R> doCommand(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            final CompletableFuture<R> promise = new CompletableFuture<>();

            final CommandFW command = commandRO.wrap(buffer, index, index + length);
            final long correlationId = command.correlationId();

            commandSent(correlationId, promise);

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
            switch (msgTypeId)
            {
            case ErrorFW.TYPE_ID:
                handleErrorResponse(buffer, index, length);
                break;
            case ResolvedFW.TYPE_ID:
                handleResolvedResponse(buffer, index, length);
                break;
            case RoutedFW.TYPE_ID:
                handleRoutedResponse(buffer, index, length);
                break;
            case UnresolvedFW.TYPE_ID:
                handleUnresolvedResponse(buffer, index, length);
                break;
            case UnroutedFW.TYPE_ID:
                handleUnroutedResponse(buffer, index, length);
                break;
            case FrozenFW.TYPE_ID:
                handleFrozenResponse(buffer, index, length);
                break;
            default:
                break;
            }

            return 1;
        }

        private void handleErrorResponse(
            DirectBuffer buffer,
            int index,
            int length)
        {
            errorRO.wrap(buffer, index, length);
            long correlationId = errorRO.correlationId();

            CompletableFuture<?> promise = promisesByCorrelationId.remove(correlationId);
            commandFailed(promise, "command failed");
        }

        @SuppressWarnings("unchecked")
        private void handleResolvedResponse(
            DirectBuffer buffer,
            int index,
            int length)
        {
            final ResolvedFW response = resolvedRO.wrap(buffer, index, length);
            long correlationId = response.correlationId();
            long authorization = response.authorization();

            CompletableFuture<Long> promise = (CompletableFuture<Long>) promisesByCorrelationId.remove(correlationId);
            commandSucceeded(promise, authorization);
        }

        @SuppressWarnings("unchecked")
        private void handleRoutedResponse(
            DirectBuffer buffer,
            int index,
            int length)
        {
            final RoutedFW routed = routedRO.wrap(buffer, index, length);
            final long correlationId = routed.correlationId();
            final long routeId = routed.routeId();

            CompletableFuture<Long> promise = (CompletableFuture<Long>) promisesByCorrelationId.remove(correlationId);
            commandSucceeded(promise, routeId);
        }

        private void handleUnresolvedResponse(
            DirectBuffer buffer,
            int index,
            int length)
        {
            final UnresolvedFW unrouted = unresolvedRO.wrap(buffer, index, length);
            final long correlationId = unrouted.correlationId();

            CompletableFuture<?> promise = promisesByCorrelationId.remove(correlationId);
            commandSucceeded(promise);
        }

        private void handleUnroutedResponse(
            DirectBuffer buffer,
            int index,
            int length)
        {
            final UnroutedFW unrouted = unroutedRO.wrap(buffer, index, length);
            final long correlationId = unrouted.correlationId();

            CompletableFuture<?> promise = promisesByCorrelationId.remove(correlationId);
            commandSucceeded(promise);
        }

        private void handleFrozenResponse(
            DirectBuffer buffer,
            int index,
            int length)
        {
            final FrozenFW frozen = frozenRO.wrap(buffer, index, length);
            final long correlationId = frozen.correlationId();

            CompletableFuture<?> promise = promisesByCorrelationId.remove(correlationId);
            commandSucceeded(promise);
        }

        private void commandSent(
            final long correlationId,
            final CompletableFuture<?> promise)
        {
            promisesByCorrelationId.put(correlationId, promise);
        }

        private <R> boolean commandSucceeded(
            final CompletableFuture<R> promise)
        {
            return commandSucceeded(promise, null);
        }

        private <R> boolean commandSucceeded(
            final CompletableFuture<R> promise,
            final R value)
        {
            return promise != null && promise.complete(value);
        }

        private boolean commandSendFailed(
            final long correlationId)
        {
            CompletableFuture<?> promise = promisesByCorrelationId.remove(correlationId);
            return commandFailed(promise, "unable to offer command");
        }

        private boolean commandFailed(
            final CompletableFuture<?> promise,
            final String message)
        {
            return promise != null && promise.completeExceptionally(new IllegalStateException(message));
        }
    }
}
