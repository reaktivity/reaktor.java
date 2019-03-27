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
package org.reaktivity.reaktor.internal.streams;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.rules.RuleChain.outerRule;
import static org.mockito.ArgumentCaptor.forClass;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.reaktivity.reaktor.test.ReaktorRule.EXTERNAL_AFFINITY_MASK;

import java.util.function.Function;
import java.util.function.IntUnaryOperator;
import java.util.function.LongFunction;
import java.util.function.LongSupplier;
import java.util.function.LongUnaryOperator;
import java.util.function.Supplier;

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
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.function.MessagePredicate;
import org.reaktivity.nukleus.function.SignalingExecutor;
import org.reaktivity.nukleus.route.RouteKind;
import org.reaktivity.nukleus.route.RouteManager;
import org.reaktivity.nukleus.stream.StreamFactory;
import org.reaktivity.nukleus.stream.StreamFactoryBuilder;
import org.reaktivity.reaktor.internal.types.control.RouteFW;
import org.reaktivity.reaktor.internal.types.stream.BeginFW;
import org.reaktivity.reaktor.internal.types.stream.ResetFW;
import org.reaktivity.reaktor.test.ReaktorRule;

public class StreamsIT
{
    private static final long SERVER_ROUTE_ID = 0x0002000100000001L;

    private final K3poRule k3po = new K3poRule()
            .addScriptRoot("route", "org/reaktivity/specification/nukleus/control/route")
            .addScriptRoot("streams", "org/reaktivity/specification/nukleus/streams");

    private final TestRule timeout = new DisableOnDebug(new Timeout(15, SECONDS));

    private final ReaktorRule reaktor = new ReaktorRule()
        .nukleus("example"::equals)
        .directory("target/nukleus-itests")
        .commandBufferCapacity(1024)
        .responseBufferCapacity(1024)
        .counterValuesBufferCapacity(4096)
        .nukleusFactory(TestNukleusFactorySpi.class)
        .affinityMask("target#0", EXTERNAL_AFFINITY_MASK)
        .clean();

    @Rule
    public final TestRule chain = outerRule(reaktor).around(k3po).around(timeout);

    @Test
    @Specification({
        "${route}/server/controller",
        "${streams}/connection.established/client",
        "${streams}/connection.established/server"
    })
    @ScriptProperty("serverAddress \"nukleus://streams/target#0\"")
    public void shouldEstablishConnection() throws Exception
    {
        k3po.finish();

        assertEquals(1, reaktor.opensRead("example", SERVER_ROUTE_ID));
        assertEquals(1, reaktor.opensWritten("example", SERVER_ROUTE_ID));
        assertEquals(0, reaktor.closesRead("example", SERVER_ROUTE_ID));
        assertEquals(0, reaktor.closesWritten("example", SERVER_ROUTE_ID));
        assertEquals(0, reaktor.abortsRead("example", SERVER_ROUTE_ID));
        assertEquals(0, reaktor.abortsWritten("example", SERVER_ROUTE_ID));
        assertEquals(0, reaktor.resetsRead("example", SERVER_ROUTE_ID));
        assertEquals(0, reaktor.resetsWritten("example", SERVER_ROUTE_ID));
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${streams}/connection.established/client",
        "${streams}/connection.established/server"
    })
    @ScriptProperty({"serverAddress \"nukleus://streams/target#0\"",
                     "routeAuthorization  0x0001_000000000080L",
                     "streamAuthorization 0x0001_000000000081L"})
    public void shouldEstablishAuthorizedConnection() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${streams}/connection.refused.not.authorized/client",
        "${streams}/connection.refused.not.authorized/server"
    })
    @ScriptProperty({"serverAddress \"nukleus://streams/target#0\"",
                     "routeAuthorization [0x01 0x00 0x81 0x00 0x00 0x00 0x00 0x00]"})
    public void shoulResetConnectionWhenNotAuthorizedMissingARequiredRole() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${streams}/connection.refused.not.authorized/client",
        "${streams}/connection.refused.not.authorized/server"
    })
    @ScriptProperty({"serverAddress \"nukleus://streams/target#0\"",
                     "routeAuthorization [0x02 0x00 0x80 0x00 0x00 0x00 0x00 0x00]"})
    public void shoulResetConnectionWhenNotAuthorizedWrongSecurityScope() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${streams}/connection.refused.unknown.route.ref/client"
    })
    public void shouldResetConnectionWhenNotRouted() throws Exception
    {
        k3po.finish();
    }

    public static class TestNukleusFactorySpi implements NukleusFactorySpi
    {
        private StreamFactoryBuilder serverStreamFactory = mock(StreamFactoryBuilder.class);
        private StreamFactory streamFactory = mock(StreamFactory.class);

        private ArgumentCaptor<LongSupplier> supplySourceCorrelationId = forClass(LongSupplier.class);
        private ArgumentCaptor<LongSupplier> supplyTargetCorrelationIdRef = forClass(LongSupplier.class);
        private ArgumentCaptor<RouteManager> routerRef = forClass(RouteManager.class);
        private ArgumentCaptor<LongUnaryOperator> supplyInitialIdRef = forClass(LongUnaryOperator.class);
        private ArgumentCaptor<LongUnaryOperator> supplyReplyIdRef = forClass(LongUnaryOperator.class);
        private ArgumentCaptor<LongSupplier> supplyGroupId = forClass(LongSupplier.class);
        @SuppressWarnings("unchecked")
        private ArgumentCaptor<LongFunction<IntUnaryOperator>> groupBudgetClaimer = forClass(LongFunction.class);
        @SuppressWarnings("unchecked")
        private ArgumentCaptor<LongFunction<IntUnaryOperator>> groupBudgetReleaser = forClass(LongFunction.class);
        private ArgumentCaptor<MutableDirectBuffer> writeBufferRef = forClass(MutableDirectBuffer.class);

        private MessageConsumer acceptInitial = mock(MessageConsumer.class);
        private MessageConsumer connectReply = mock(MessageConsumer.class);
        private ArgumentCaptor<MessageConsumer> acceptReplyRef = forClass(MessageConsumer.class);
        private ArgumentCaptor<MessageConsumer> connectInitialRef = forClass(MessageConsumer.class);

        private final RouteFW routeRO = new RouteFW();
        private final BeginFW beginRO = new BeginFW();

        private final BeginFW.Builder beginRW = new BeginFW.Builder();
        private final ResetFW.Builder resetRW = new ResetFW.Builder();

        private long acceptRouteId;
        private long acceptInitialId;
        private long acceptCorrelationId;
        private long acceptReplyId;
        private long connectRouteId;
        private long connectInitialId;
        private long connectCorrelationId;

        @SuppressWarnings("unchecked")
        public TestNukleusFactorySpi()
        {
            when(serverStreamFactory.setSourceCorrelationIdSupplier(supplySourceCorrelationId.capture()))
                .thenReturn(serverStreamFactory);
            when(serverStreamFactory.setTargetCorrelationIdSupplier(supplyTargetCorrelationIdRef.capture()))
                .thenReturn(serverStreamFactory);
            when(serverStreamFactory.setInitialIdSupplier(supplyInitialIdRef.capture())).thenReturn(serverStreamFactory);
            when(serverStreamFactory.setReplyIdSupplier(supplyReplyIdRef.capture())).thenReturn(serverStreamFactory);
            when(serverStreamFactory.setTraceSupplier(any(LongSupplier.class))).thenReturn(serverStreamFactory);
            when(serverStreamFactory.setGroupIdSupplier(supplyGroupId.capture())).thenReturn(serverStreamFactory);
            when(serverStreamFactory.setGroupBudgetClaimer(groupBudgetClaimer.capture())).thenReturn(serverStreamFactory);
            when(serverStreamFactory.setGroupBudgetReleaser(groupBudgetReleaser.capture())).thenReturn(serverStreamFactory);
            when(serverStreamFactory.setRouteManager(routerRef.capture())).thenReturn(serverStreamFactory);
            when(serverStreamFactory.setExecutor(any(SignalingExecutor.class))).thenReturn(serverStreamFactory);
            when(serverStreamFactory.setWriteBuffer(writeBufferRef.capture())).thenReturn(serverStreamFactory);
            when(serverStreamFactory.setCounterSupplier(any(Function.class))).thenReturn(serverStreamFactory);
            when(serverStreamFactory.setAccumulatorSupplier(any(Function.class))).thenReturn(serverStreamFactory);
            when(serverStreamFactory.setBufferPoolSupplier(any(Supplier.class))).thenReturn(serverStreamFactory);
            when(serverStreamFactory.build()).thenReturn(streamFactory);

            when(streamFactory.newStream(
                    eq(BeginFW.TYPE_ID),
                    any(DirectBuffer.class),
                    anyInt(),
                    anyInt(),
                    acceptReplyRef.capture()))
                 .thenAnswer(invocation ->
                 {
                     when(streamFactory.newStream(anyInt(), any(DirectBuffer.class), anyInt(), anyInt(),
                             connectInitialRef.capture()))
                         .thenReturn(connectReply);
                     return acceptInitial;
                 });

            doAnswer(invocation ->
            {
                final DirectBuffer buffer = invocation.getArgument(1);
                final int index = invocation.getArgument(2);
                final int length = invocation.getArgument(3);

                final BeginFW begin = beginRO.wrap(buffer, index, index + length);
                final long routeId = begin.routeId();
                final long authorization = begin.authorization();

                final RouteManager router = routerRef.getValue();
                MessagePredicate filter = (m, b, i, l) -> true;
                RouteFW route = router.resolve(routeId, authorization, filter,
                        (m, b, i, l) -> routeRO.wrap(b, i, i + l));

                if (route != null)
                {
                    final LongUnaryOperator supplyInitialId = supplyInitialIdRef.getValue();
                    final LongUnaryOperator supplyReplyId = supplyReplyIdRef.getValue();
                    final LongSupplier supplyTargetCorrelationId = supplyTargetCorrelationIdRef.getValue();

                    acceptRouteId = routeId;
                    acceptInitialId = begin.streamId();
                    acceptCorrelationId = begin.correlationId();
                    acceptReplyId = supplyReplyId.applyAsLong(acceptInitialId);

                    connectRouteId = route.correlationId();
                    connectInitialId = supplyInitialId.applyAsLong(connectRouteId);
                    connectCorrelationId = supplyTargetCorrelationId.getAsLong();

                    MessageConsumer connectInitial = router.supplyReceiver(connectInitialId);

                    doBegin(connectInitial, connectRouteId, connectInitialId,
                            begin.authorization(), connectCorrelationId);
                    router.setThrottle(connectInitialId, connectReply);
                }
                else
                {
                    final long streamId = begin.streamId();
                    final MessageConsumer acceptReply = acceptReplyRef.getValue();

                    doReset(acceptReply, routeId, streamId);
                }
                return null;
            }
            ).when(acceptInitial).accept(eq(BeginFW.TYPE_ID), any(DirectBuffer.class), anyInt(), anyInt());

            doAnswer(invocation ->
            {
                final MessageConsumer acceptReply = acceptReplyRef.getValue();
                doBegin(acceptReply, acceptRouteId, acceptReplyId, 0L, acceptCorrelationId);
                return null;
            }
            ).when(connectReply).accept(eq(BeginFW.TYPE_ID), any(DirectBuffer.class), anyInt(), anyInt());

            doAnswer(invocation ->
            {
                final MessageConsumer acceptReply = acceptReplyRef.getValue();
                doReset(acceptReply, acceptRouteId, acceptInitialId);
                return null;
            }
            ).when(connectReply).accept(eq(ResetFW.TYPE_ID), any(DirectBuffer.class), anyInt(), anyInt());
        }

        @Override
        public String name()
        {
           return "example";
        }

        @Override
        public ExampleNukleus create(Configuration config)
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
            long authorization,
            long correlationId)
        {
            final MutableDirectBuffer writeBuffer = writeBufferRef.getValue();
            final BeginFW begin = beginRW.wrap(writeBuffer,  0, writeBuffer.capacity())
                    .routeId(routeId)
                    .streamId(streamId)
                    .authorization(authorization)
                    .correlationId(correlationId)
                    .build();
            receiver.accept(begin.typeId(), begin.buffer(), begin.offset(), begin.sizeof());
        }

        private void doReset(
            MessageConsumer receiver,
            long routeId,
            long streamId)
        {
            final MutableDirectBuffer writeBuffer = writeBufferRef.getValue();
            final ResetFW reset = resetRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                    .routeId(routeId)
                    .streamId(streamId)
                    .build();
            receiver.accept(reset.typeId(), reset.buffer(), reset.offset(), reset.sizeof());
        }
    }
}
