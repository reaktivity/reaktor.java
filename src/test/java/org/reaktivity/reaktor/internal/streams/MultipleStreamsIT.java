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
package org.reaktivity.reaktor.internal.streams;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.rules.RuleChain.outerRule;
import static org.mockito.ArgumentCaptor.forClass;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.reaktivity.reaktor.internal.router.StreamId.isInitial;
import static org.reaktivity.reaktor.test.ReaktorRule.EXTERNAL_AFFINITY_MASK;

import java.util.function.Function;
import java.util.function.LongFunction;
import java.util.function.LongSupplier;
import java.util.function.LongToIntFunction;
import java.util.function.LongUnaryOperator;
import java.util.function.Supplier;
import java.util.function.ToIntFunction;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;
import org.kaazing.k3po.junit.annotation.ScriptProperty;
import org.kaazing.k3po.junit.annotation.Specification;
import org.kaazing.k3po.junit.rules.K3poRule;
import org.mockito.ArgumentCaptor;
import org.reaktivity.nukleus.Configuration;
import org.reaktivity.nukleus.Elektron;
import org.reaktivity.nukleus.Nukleus;
import org.reaktivity.nukleus.NukleusFactorySpi;
import org.reaktivity.nukleus.budget.BudgetCreditor;
import org.reaktivity.nukleus.concurrent.Signaler;
import org.reaktivity.nukleus.concurrent.SignalingExecutor;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.function.MessagePredicate;
import org.reaktivity.nukleus.route.RouteKind;
import org.reaktivity.nukleus.route.RouteManager;
import org.reaktivity.nukleus.stream.StreamFactory;
import org.reaktivity.nukleus.stream.StreamFactoryBuilder;
import org.reaktivity.reaktor.test.ReaktorRule;
import org.reaktivity.reaktor.test.internal.k3po.ext.types.control.RouteFW;
import org.reaktivity.reaktor.test.internal.k3po.ext.types.stream.BeginFW;
import org.reaktivity.reaktor.test.internal.k3po.ext.types.stream.EndFW;
import org.reaktivity.reaktor.test.internal.k3po.ext.types.stream.WindowFW;

public class MultipleStreamsIT
{
    private final K3poRule k3po = new K3poRule()
            .addScriptRoot("route", "org/reaktivity/specification/nukleus/control/route")
            .addScriptRoot("streams", "org/reaktivity/specification/nukleus/streams");

    private final TestRule timeout = new DisableOnDebug(new Timeout(10, SECONDS));

    private final ReaktorRule reaktor = new ReaktorRule()
        .nukleus("example"::equals)
        .directory("target/nukleus-itests")
        .commandBufferCapacity(1024)
        .responseBufferCapacity(1024)
        .counterValuesBufferCapacity(8192)
        .nukleusFactory(ExampleNukleusFactorySpi.class)
        .affinityMask("target#0", EXTERNAL_AFFINITY_MASK)
        .affinityMask("target#1", EXTERNAL_AFFINITY_MASK)
        .clean();

    @Rule
    public final TestRule chain = outerRule(reaktor).around(k3po).around(timeout);

    @Test
    @Specification({
        "${route}/server/multiple.routes/controller",
        "${streams}/multiple.connections.established/client",
        "${streams}/multiple.connections.established/server"
    })
    @ScriptProperty({"serverAddress1 \"nukleus://streams/target#0\"",
                     "serverAddress2 \"nukleus://streams/target#1\"",
                     "route1Authorization  0x0001_000000000008L",
                     "stream1Authorization 0x0001_000000000008L",
                     "route2Authorization  0x0001_000000000000L",
                     "stream2Authorization 0x0001_000000000000L"})
    public void shouldEstablishMultipleAuthorizedConnections() throws Exception
    {
        k3po.finish();
    }

    public static class ExampleNukleusFactorySpi implements NukleusFactorySpi
    {
        private StreamFactoryBuilder serverStreamFactory = mock(StreamFactoryBuilder.class);
        private StreamFactory streamFactory = mock(StreamFactory.class);

        private ArgumentCaptor<RouteManager> routerRef = forClass(RouteManager.class);
        private ArgumentCaptor<LongUnaryOperator> supplyInitialIdRef = forClass(LongUnaryOperator.class);
        private ArgumentCaptor<LongUnaryOperator> supplyReplyIdRef = forClass(LongUnaryOperator.class);
        private ArgumentCaptor<LongSupplier> supplyGroupId = forClass(LongSupplier.class);
        private ArgumentCaptor<MutableDirectBuffer> writeBufferRef = forClass(MutableDirectBuffer.class);
        private ArgumentCaptor<MessageConsumer> acceptReplyRef = forClass(MessageConsumer.class);

        private MessageConsumer acceptInitial1 = mock(MessageConsumer.class);
        private MessageConsumer connectReply1 = mock(MessageConsumer.class);
        private MessageConsumer acceptInitial2 = mock(MessageConsumer.class);
        private MessageConsumer connectReply2 = mock(MessageConsumer.class);
        private boolean newStream1Started;

        private final RouteFW routeRO = new RouteFW();

        private final BeginFW beginRO = new BeginFW();
        private final EndFW endRO = new EndFW();
        private final WindowFW windowRO = new WindowFW();

        private final BeginFW.Builder beginRW = new BeginFW.Builder();
        private final EndFW.Builder endRW = new EndFW.Builder();
        private final WindowFW.Builder windowRW = new WindowFW.Builder();

        private MessageConsumer acceptReply1;
        private long acceptRouteId1;
        private long acceptInitialId1;
        private long acceptInitialSeq1;
        private long acceptInitialAck1;
        private long acceptReplyId1;
        private long acceptReplySeq1;
        private long acceptReplyAck1;

        private MessageConsumer connectInitial1;
        private long connectRouteId1;
        private long connectInitialId1;
        private long connectInitialSeq1;
        private long connectInitialAck1;
        private long connectReplyId1;
        private long connectReplySeq1;
        private long connectReplyAck1;

        private MessageConsumer acceptReply2;
        private long acceptRouteId2;
        private long acceptInitialId2;
        private long acceptInitialSeq2;
        private long acceptInitialAck2;
        private long acceptReplyId2;
        private long acceptReplySeq2;
        private long acceptReplyAck2;

        private MessageConsumer connectInitial2;
        private long connectRouteId2;
        private long connectInitialId2;
        private long connectInitialSeq2;
        private long connectInitialAck2;
        private long connectReplyId2;
        private long connectReplySeq2;
        private long connectReplyAck2;

        @SuppressWarnings("unchecked")
        public ExampleNukleusFactorySpi()
        {
            when(serverStreamFactory.setInitialIdSupplier(supplyInitialIdRef.capture())).thenReturn(serverStreamFactory);
            when(serverStreamFactory.setReplyIdSupplier(supplyReplyIdRef.capture())).thenReturn(serverStreamFactory);
            when(serverStreamFactory.setTraceIdSupplier(any(LongSupplier.class))).thenReturn(serverStreamFactory);
            when(serverStreamFactory.setTypeIdSupplier(any(ToIntFunction.class))).thenReturn(serverStreamFactory);
            when(serverStreamFactory.setBudgetIdSupplier(supplyGroupId.capture())).thenReturn(serverStreamFactory);
            when(serverStreamFactory.setBudgetCreditor(any(BudgetCreditor.class))).thenReturn(serverStreamFactory);
            when(serverStreamFactory.setBudgetDebitorSupplier(any(LongFunction.class))).thenReturn(serverStreamFactory);
            when(serverStreamFactory.setRouteManager(routerRef.capture())).thenReturn(serverStreamFactory);
            when(serverStreamFactory.setExecutor(any(SignalingExecutor.class))).thenReturn(serverStreamFactory);
            when(serverStreamFactory.setSignaler(any(Signaler.class))).thenReturn(serverStreamFactory);
            when(serverStreamFactory.setWriteBuffer(writeBufferRef.capture())).thenReturn(serverStreamFactory);
            when(serverStreamFactory.setCounterSupplier(any(Function.class))).thenReturn(serverStreamFactory);
            when(serverStreamFactory.setAccumulatorSupplier(any(Function.class))).thenReturn(serverStreamFactory);
            when(serverStreamFactory.setBufferPoolSupplier(any(Supplier.class))).thenReturn(serverStreamFactory);
            when(serverStreamFactory.setDroppedFrameConsumer(any(MessageConsumer.class))).thenReturn(serverStreamFactory);
            when(serverStreamFactory.setRemoteIndexSupplier(any(LongToIntFunction.class))).thenReturn(serverStreamFactory);
            when(serverStreamFactory.setHostResolver(any(Function.class))).thenReturn(serverStreamFactory);
            when(serverStreamFactory.build()).thenReturn(streamFactory);

            when(streamFactory.newStream(
                    eq(BeginFW.TYPE_ID),
                    any(DirectBuffer.class),
                    anyInt(),
                    anyInt(),
                    acceptReplyRef.capture()))
                 .thenAnswer(invocation ->
                 {
                     final DirectBuffer buffer = invocation.getArgument(1);
                     final int index = invocation.getArgument(2);
                     final int length = invocation.getArgument(3);

                     final BeginFW begin = beginRO.wrap(buffer, index, index + length);

                     MessageConsumer result;
                     if (isInitial(begin.streamId()))
                     {
                         if (!newStream1Started)
                         {
                             acceptReply1 = acceptReplyRef.getValue();
                             result = acceptInitial1;
                             newStream1Started = true;
                         }
                         else
                         {
                             acceptReply2 = acceptReplyRef.getValue();
                             result = acceptInitial2;
                         }
                     }
                     else
                     {
                         if (begin.streamId() == connectReplyId1)
                         {
                             result = connectReply1;
                         }
                         else
                         {
                             result = connectReply2;
                         }
                     }
                     return result;
                 });

            doAnswer(invocation ->
            {
                final DirectBuffer buffer = invocation.getArgument(1);
                final int index = invocation.getArgument(2);
                final int length = invocation.getArgument(3);

                final BeginFW begin = beginRO.wrap(buffer, index, index + length);
                final long routeId = begin.routeId();
                final long authorization = begin.authorization();
                final int maximum = begin.maximum();

                final RouteManager router = routerRef.getValue();
                MessagePredicate filter = (m, b, i, l) -> true;
                RouteFW route = router.resolve(routeId, authorization, filter,
                    (m, b, i, l) -> routeRO.wrap(b, i, i + l));

                if (route != null)
                {
                    final LongUnaryOperator supplyInitialId = supplyInitialIdRef.getValue();
                    final LongUnaryOperator supplyReplyId = supplyReplyIdRef.getValue();

                    acceptRouteId1 = routeId;
                    acceptInitialId1 = begin.streamId();
                    acceptReplyId1 = supplyReplyId.applyAsLong(begin.streamId());

                    connectRouteId1 = route.correlationId();
                    connectInitialId1 = supplyInitialId.applyAsLong(connectRouteId1);
                    connectReplyId1 = supplyReplyId.applyAsLong(connectInitialId1);

                    connectInitial1 = router.supplyReceiver(connectInitialId1);

                    doBegin(connectInitial1, connectRouteId1, connectInitialId1, 0L, 0L,
                            begin.authorization(), maximum);
                    router.setThrottle(connectInitialId1, connectReply1);
                }

                return null;
            }
            ).when(acceptInitial1).accept(eq(BeginFW.TYPE_ID), any(DirectBuffer.class), anyInt(), anyInt());

            doAnswer(invocation ->
            {
                final DirectBuffer buffer = invocation.getArgument(1);
                final int index = invocation.getArgument(2);
                final int length = invocation.getArgument(3);

                final WindowFW window = windowRO.wrap(buffer, index, index + length);
                final long budgetId = window.budgetId();
                final int maximum = window.maximum();
                final int padding = window.padding();

                doWindow(acceptReply1, acceptRouteId1, acceptInitialId1, acceptInitialSeq1, acceptInitialAck1,
                        budgetId, maximum, padding);
                return null;
            }
            ).when(connectReply1).accept(eq(WindowFW.TYPE_ID), any(DirectBuffer.class), anyInt(), anyInt());

            doAnswer(invocation ->
            {
                final DirectBuffer buffer = invocation.getArgument(1);
                final int index = invocation.getArgument(2);
                final int length = invocation.getArgument(3);

                final EndFW end = endRO.wrap(buffer, index, index + length);
                final int maximum = end.maximum();

                doEnd(connectInitial1, connectRouteId1, connectInitialId1, connectInitialSeq1, connectInitialAck1, maximum);
                return null;
            }
            ).when(acceptInitial1).accept(eq(EndFW.TYPE_ID), any(DirectBuffer.class), anyInt(), anyInt());

            doAnswer(invocation ->
            {
                final DirectBuffer buffer = invocation.getArgument(1);
                final int index = invocation.getArgument(2);
                final int length = invocation.getArgument(3);

                final BeginFW begin = beginRO.wrap(buffer, index, index + length);
                final int maximum = begin.maximum();

                doBegin(acceptReply1, acceptRouteId1, acceptReplyId1, acceptReplySeq1, acceptReplyAck1, 0L, maximum);
                final RouteManager router = routerRef.getValue();
                router.setThrottle(acceptReplyId1, acceptInitial1);
                return null;
            }
            ).when(connectReply1).accept(eq(BeginFW.TYPE_ID), any(DirectBuffer.class), anyInt(), anyInt());

            doAnswer(invocation ->
            {
                final DirectBuffer buffer = invocation.getArgument(1);
                final int index = invocation.getArgument(2);
                final int length = invocation.getArgument(3);

                final WindowFW window = windowRO.wrap(buffer, index, index + length);
                final long budgetId = window.budgetId();
                final int maximum = window.maximum();
                final int padding = window.padding();

                doWindow(connectInitial1, connectRouteId1, connectReplyId1, connectReplySeq1, connectReplyAck1,
                        budgetId, maximum, padding);
                return null;
            }
            ).when(acceptInitial1).accept(eq(WindowFW.TYPE_ID), any(DirectBuffer.class), anyInt(), anyInt());

            doAnswer(invocation ->
            {
                final DirectBuffer buffer = invocation.getArgument(1);
                final int index = invocation.getArgument(2);
                final int length = invocation.getArgument(3);

                final EndFW end = endRO.wrap(buffer, index, index + length);
                final int maximum = end.maximum();

                doEnd(acceptReply1, acceptRouteId1, acceptReplyId1, acceptReplySeq1, acceptReplyAck1, maximum);
                return null;
            }
            ).when(connectReply1).accept(eq(EndFW.TYPE_ID), any(DirectBuffer.class), anyInt(), anyInt());

            doAnswer(invocation ->
            {
                final DirectBuffer buffer = invocation.getArgument(1);
                final int index = invocation.getArgument(2);
                final int length = invocation.getArgument(3);

                final BeginFW begin = beginRO.wrap(buffer, index, index + length);

                final long routeId = begin.routeId();
                final long authorization = begin.authorization();
                final int maximum = begin.maximum();

                final RouteManager router = routerRef.getValue();
                MessagePredicate filter = (m, b, i, l) -> true;
                RouteFW route = router.resolve(routeId, authorization, filter,
                    (m, b, i, l) -> routeRO.wrap(b, i, i + l));

                if (route != null)
                {
                    final LongUnaryOperator supplyInitialId = supplyInitialIdRef.getValue();
                    final LongUnaryOperator supplyReplyId = supplyReplyIdRef.getValue();

                    acceptRouteId2 = routeId;
                    acceptInitialId2 = begin.streamId();
                    acceptReplyId2 = supplyReplyId.applyAsLong(begin.streamId());

                    connectRouteId2 = route.correlationId();
                    connectInitialId2 = supplyInitialId.applyAsLong(connectRouteId2);
                    connectReplyId2 = supplyReplyId.applyAsLong(connectInitialId2);

                    connectInitial2 = router.supplyReceiver(connectInitialId2);

                    doBegin(connectInitial2, connectRouteId2, connectInitialId2, connectInitialSeq1, connectInitialAck1,
                            begin.authorization(), maximum);
                    router.setThrottle(connectInitialId2, connectReply2);
                }
                return null;
            }
            ).when(acceptInitial2).accept(eq(BeginFW.TYPE_ID), any(DirectBuffer.class), anyInt(), anyInt());

            doAnswer(invocation ->
            {
                final DirectBuffer buffer = invocation.getArgument(1);
                final int index = invocation.getArgument(2);
                final int length = invocation.getArgument(3);

                final WindowFW window = windowRO.wrap(buffer, index, index + length);
                final long budgetId = window.budgetId();
                final int maximum = window.maximum();
                final int padding = window.padding();

                doWindow(acceptReply2, acceptRouteId2, acceptInitialId2, acceptInitialSeq2, acceptInitialAck2,
                        budgetId, maximum, padding);
                return null;
            }
            ).when(connectReply2).accept(eq(WindowFW.TYPE_ID), any(DirectBuffer.class), anyInt(), anyInt());

            doAnswer(invocation ->
            {
                final DirectBuffer buffer = invocation.getArgument(1);
                final int index = invocation.getArgument(2);
                final int length = invocation.getArgument(3);

                final EndFW end = endRO.wrap(buffer, index, index + length);
                final int maximum = end.maximum();

                doEnd(connectInitial2, connectRouteId2, connectInitialId2, connectInitialSeq2, connectInitialAck2, maximum);
                return null;
            }
            ).when(acceptInitial2).accept(eq(EndFW.TYPE_ID), any(DirectBuffer.class), anyInt(), anyInt());

            doAnswer(invocation ->
            {
                final DirectBuffer buffer = invocation.getArgument(1);
                final int index = invocation.getArgument(2);
                final int length = invocation.getArgument(3);

                final BeginFW begin = beginRO.wrap(buffer, index, index + length);
                final int maximum = begin.maximum();

                doBegin(acceptReply2, acceptRouteId2, acceptReplyId2, acceptReplySeq2, acceptReplyAck2, 0L, maximum);
                final RouteManager router = routerRef.getValue();
                router.setThrottle(acceptReplyId2, acceptInitial2);
                return null;
            }).when(connectReply2).accept(eq(BeginFW.TYPE_ID), any(DirectBuffer.class), anyInt(), anyInt());

            doAnswer(invocation ->
            {
                final DirectBuffer buffer = invocation.getArgument(1);
                final int index = invocation.getArgument(2);
                final int length = invocation.getArgument(3);

                final WindowFW window = windowRO.wrap(buffer, index, index + length);
                final long budgetId = window.budgetId();
                final int maximum = window.maximum();
                final int padding = window.padding();

                doWindow(connectInitial2, connectRouteId2, connectReplyId2, connectReplySeq2, connectReplyAck2,
                        budgetId, maximum, padding);
                return null;
            }
            ).when(acceptInitial2).accept(eq(WindowFW.TYPE_ID), any(DirectBuffer.class), anyInt(), anyInt());

            doAnswer(invocation ->
            {
                final DirectBuffer buffer = invocation.getArgument(1);
                final int index = invocation.getArgument(2);
                final int length = invocation.getArgument(3);

                final EndFW end = endRO.wrap(buffer, index, index + length);
                final int maximum = end.maximum();

                doEnd(acceptReply2, acceptRouteId2, acceptReplyId2, acceptReplySeq2, acceptReplyAck2, maximum);
                return null;
            }
            ).when(connectReply2).accept(eq(EndFW.TYPE_ID), any(DirectBuffer.class), anyInt(), anyInt());
        }

        @Override
        public String name()
        {
            return "example";
        }

        @Override
        public Nukleus create(
            Configuration config)
        {
            return new ExampleNukleus(config);
        }

        private final class ExampleNukleus implements Nukleus
        {
            private final Configuration config;

            private ExampleNukleus(
                Configuration config)
            {
                this.config = config;
            }

            @Override
            public String name()
            {
                return "example";
            }

            @Override
            public Configuration config()
            {
                return config;
            }

            @Override
            public Elektron supplyElektron()
            {
                return new ExampleElektron();
            }

            private final class ExampleElektron implements Elektron
            {
                @Override
                public StreamFactoryBuilder streamFactoryBuilder(
                    RouteKind kind)
                {
                    StreamFactoryBuilder builder = null;

                    switch (kind)
                    {
                    case SERVER:
                        builder = serverStreamFactory;
                        break;
                    default:
                        break;
                    }

                    return builder;
                }
            }
        }

        private void doBegin(
            MessageConsumer receiver,
            long routeId,
            long streamId,
            long sequence,
            long acknowledge,
            long authorization,
            int maximum)
        {
            final MutableDirectBuffer writeBuffer = writeBufferRef.getValue();
            final BeginFW begin = beginRW.wrap(writeBuffer,  0, writeBuffer.capacity())
                    .routeId(routeId)
                    .streamId(streamId)
                    .sequence(sequence)
                    .acknowledge(acknowledge)
                    .maximum(maximum)
                    .authorization(authorization)
                    .affinity(0L)
                    .build();
            receiver.accept(begin.typeId(), begin.buffer(), begin.offset(), begin.sizeof());
        }

        private void doEnd(
            MessageConsumer receiver,
            long routeId,
            long streamId,
            long sequence,
            long acknowledge,
            int maximum)
        {
            final MutableDirectBuffer writeBuffer = writeBufferRef.getValue();
            final EndFW end = endRW.wrap(writeBuffer,  0, writeBuffer.capacity())
                    .routeId(routeId)
                    .streamId(streamId)
                    .sequence(sequence)
                    .acknowledge(acknowledge)
                    .maximum(maximum)
                    .build();
            receiver.accept(end.typeId(), end.buffer(), end.offset(), end.sizeof());
        }

        private void doWindow(
            MessageConsumer receiver,
            long routeId,
            long streamId,
            long sequence,
            long acknowledge,
            long budgetId,
            int maximum,
            int padding)
        {
            final MutableDirectBuffer writeBuffer = writeBufferRef.getValue();
            final WindowFW window = windowRW.wrap(writeBuffer,  0, writeBuffer.capacity())
                    .routeId(routeId)
                    .streamId(streamId)
                    .sequence(sequence)
                    .acknowledge(acknowledge)
                    .maximum(maximum)
                    .budgetId(budgetId)
                    .padding(padding)
                    .build();
            receiver.accept(window.typeId(), window.buffer(), window.offset(), window.sizeof());
        }
    }
}
