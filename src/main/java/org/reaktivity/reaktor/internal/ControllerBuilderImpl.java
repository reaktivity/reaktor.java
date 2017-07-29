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
package org.reaktivity.reaktor.internal;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.ToIntFunction;

import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.concurrent.MessageHandler;
import org.agrona.concurrent.broadcast.BroadcastReceiver;
import org.agrona.concurrent.broadcast.CopyBroadcastReceiver;
import org.agrona.concurrent.ringbuffer.RingBuffer;
import org.reaktivity.nukleus.Configuration;
import org.reaktivity.nukleus.Controller;
import org.reaktivity.nukleus.ControllerBuilder;
import org.reaktivity.nukleus.ControllerSpi;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.function.MessagePredicate;
import org.reaktivity.reaktor.internal.layouts.StreamsLayout;
import org.reaktivity.reaktor.internal.types.control.ErrorFW;
import org.reaktivity.reaktor.internal.types.control.FrameFW;
import org.reaktivity.reaktor.internal.types.control.RouteFW;
import org.reaktivity.reaktor.internal.types.control.RoutedFW;
import org.reaktivity.reaktor.internal.types.control.UnrouteFW;
import org.reaktivity.reaktor.internal.types.control.UnroutedFW;

public final class ControllerBuilderImpl<T extends Controller> implements ControllerBuilder<T>
{
    private final Configuration config;
    private final Class<T> kind;

    private Function<ControllerSpi, T> factory;
    private String name;

    public ControllerBuilderImpl(
        Configuration config,
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
    public ControllerBuilder<T> setName(
        String name)
    {
        this.name = name;
        return this;
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
        Objects.requireNonNull(name, "name");

        Context context = new Context();
        context.name(name).readonly(true).conclude(config);

        ControllerSpi controllerSpi = new ControllerSpiImpl(context);

        return factory.apply(controllerSpi);
    }

    private final class ControllerSpiImpl implements ControllerSpi
    {
        private final FrameFW frameRO = new FrameFW();
        private final RoutedFW routedRO = new RoutedFW();
        private final UnroutedFW unroutedRO = new UnroutedFW();
        private final ErrorFW errorRO = new ErrorFW();

        private final Context context;
        private final RingBuffer conductorCommands;
        private final CopyBroadcastReceiver conductorResponses;
        private final Long2ObjectHashMap<CompletableFuture<?>> promisesByCorrelationId;
        private final MessageHandler readHandler;
        private final Map<String, StreamsLayout> sourcesByName;
        private final Map<String, StreamsLayout> targetsByName;

        private ControllerSpiImpl(
            Context context)
        {
            this.context = context;
            this.conductorCommands = context.conductorCommands();
            this.conductorResponses = new CopyBroadcastReceiver(new BroadcastReceiver(context.conductorResponseBuffer()));
            this.promisesByCorrelationId = new Long2ObjectHashMap<>();
            this.sourcesByName = new HashMap<>();
            this.targetsByName = new HashMap<>();
            this.readHandler = this::handleResponse;
        }

        @Override
        public long nextCorrelationId()
        {
            return conductorCommands.nextCorrelationId();
        }

        @Override
        public int doProcess()
        {
            return conductorResponses.receive(readHandler);
        }

        @Override
        public void doClose()
        {
            sourcesByName.values().forEach(CloseHelper::close);
            sourcesByName.clear();

            targetsByName.values().forEach(CloseHelper::close);
            targetsByName.clear();

            CloseHelper.close(context);
        }

        @Override
        public CompletableFuture<Long> doRoute(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            assert msgTypeId == RouteFW.TYPE_ID;

            return handleCommand(Long.class, msgTypeId, buffer, index, length);
        }

        @Override
        public CompletableFuture<Void> doUnroute(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            assert msgTypeId == UnrouteFW.TYPE_ID;

            return handleCommand(Void.class, msgTypeId, buffer, index, length);
        }

        @Override
        public <R> R doSupplySource(
            String sourceName,
            BiFunction<MessagePredicate, ToIntFunction<MessageConsumer>, R> factory)
        {
            StreamsLayout source = sourcesByName.computeIfAbsent(sourceName, this::newSource);

            MessagePredicate streams = source.streamsBuffer()::write;
            ToIntFunction<MessageConsumer> throttle = source.throttleBuffer()::read;

            return factory.apply(streams, throttle);
        }

        @Override
        public <R> R doSupplyTarget(
            String targetName,
            BiFunction<ToIntFunction<MessageConsumer>, MessagePredicate, R> factory)
        {
            StreamsLayout target = targetsByName.computeIfAbsent(targetName, this::newTarget);

            ToIntFunction<MessageConsumer> streams = target.streamsBuffer()::read;
            MessagePredicate throttle = target.throttleBuffer()::write;

            return factory.apply(streams, throttle);
        }

        @Override
        public long doCount(String name)
        {
            return context.counters().readonlyCounter(name).getAsLong();
        }

        private StreamsLayout newSource(
            String sourceName)
        {
            return new StreamsLayout.Builder()
                    .path(context.sourceStreamsPath().apply(sourceName))
                    .streamsCapacity(context.streamsBufferCapacity())
                    .throttleCapacity(context.throttleBufferCapacity())
                    .readonly(true)
                    .build();
        }

        private StreamsLayout newTarget(
            String targetName)
        {
            return new StreamsLayout.Builder()
                    .path(context.targetStreamsPath().apply(targetName))
                    .streamsCapacity(context.streamsBufferCapacity())
                    .throttleCapacity(context.throttleBufferCapacity())
                    .readonly(false)
                    .build();
        }

        private <R> CompletableFuture<R> handleCommand(
            Class<R> resultType,
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            final CompletableFuture<R> promise = new CompletableFuture<>();

            final FrameFW frame = frameRO.wrap(buffer, index, index + length);

            if (!conductorCommands.write(msgTypeId, buffer, index, length))
            {
                commandSendFailed(promise);
            }
            else
            {
                commandSent(frame.correlationId(), promise);
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
            case RoutedFW.TYPE_ID:
                handleRoutedResponse(buffer, index, length);
                break;
            case UnroutedFW.TYPE_ID:
                handleUnroutedResponse(buffer, index, length);
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
            if (promise != null)
            {
                commandFailed(promise, "command failed");
            }
        }

        @SuppressWarnings("unchecked")
        private void handleRoutedResponse(
            DirectBuffer buffer,
            int index,
            int length)
        {
            final RoutedFW routed = routedRO.wrap(buffer, index, length);
            long correlationId = routed.correlationId();
            long sourceRef = routed.sourceRef();

            CompletableFuture<Long> promise = (CompletableFuture<Long>) promisesByCorrelationId.remove(correlationId);
            if (promise != null)
            {
                commandSucceeded(promise, sourceRef);
            }
        }

        private void handleUnroutedResponse(
            DirectBuffer buffer,
            int index,
            int length)
        {
            final UnroutedFW unrouted = unroutedRO.wrap(buffer, index, length);
            final long correlationId = unrouted.correlationId();

            CompletableFuture<?> promise = promisesByCorrelationId.remove(correlationId);
            if (promise != null)
            {
                commandSucceeded(promise);
            }
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
            return promise.complete(value);
        }

        private boolean commandSendFailed(
            final CompletableFuture<?> promise)
        {
            return commandFailed(promise, "unable to offer command");
        }

        private boolean commandFailed(
            final CompletableFuture<?> promise,
            final String message)
        {
            return promise.completeExceptionally(new IllegalStateException(message));
        }
    }
}
