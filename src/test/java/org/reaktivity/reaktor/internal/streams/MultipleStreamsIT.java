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
import static org.reaktivity.nukleus.route.RouteKind.SERVER;

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
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.reaktivity.nukleus.Configuration;
import org.reaktivity.nukleus.Nukleus;
import org.reaktivity.nukleus.NukleusBuilder;
import org.reaktivity.nukleus.NukleusFactorySpi;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.function.MessagePredicate;
import org.reaktivity.nukleus.route.RouteManager;
import org.reaktivity.nukleus.stream.StreamFactory;
import org.reaktivity.nukleus.stream.StreamFactoryBuilder;
import org.reaktivity.reaktor.internal.types.control.RouteFW;
import org.reaktivity.reaktor.internal.types.stream.BeginFW;
import org.reaktivity.reaktor.test.ReaktorRule;

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
        .counterValuesBufferCapacity(4096)
        .nukleusFactory(TestNukleusFactorySpi.class)
        .clean();

    @Rule
    public final TestRule chain = outerRule(reaktor).around(k3po).around(timeout);

    @Test
    @Specification({
        "${route}/server/multiple.routes/controller",
        "${streams}/multiple.connections.established/client",
        "${streams}/multiple.connections.established/server"
    })
    @ScriptProperty({"route1Authorization  0x0001_000000000008L",
                     "stream1Authorization 0x0001_000000000008L",
                     "route2Authorization  0x0001_000000000000L",
                     "stream2Authorization 0x0001_000000000000L"})
    public void shouldEstablishMultipleAuthorizedConnections() throws Exception
    {
        k3po.finish();
    }

    public static class TestNukleusFactorySpi implements NukleusFactorySpi
    {
        private StreamFactoryBuilder serverStreamFactory = mock(StreamFactoryBuilder.class);
        private StreamFactory streamFactory = mock(StreamFactory.class);

        private ArgumentCaptor<LongSupplier> supplySourceCorrelationId = forClass(LongSupplier.class);
        private ArgumentCaptor<LongSupplier> supplyTargetCorrelationId = forClass(LongSupplier.class);
        private ArgumentCaptor<RouteManager> router = forClass(RouteManager.class);
        private ArgumentCaptor<LongSupplier> supplyInitialId = forClass(LongSupplier.class);
        private ArgumentCaptor<LongUnaryOperator> supplyReplyId = forClass(LongUnaryOperator.class);
        private ArgumentCaptor<LongSupplier> supplyGroupId = forClass(LongSupplier.class);
        @SuppressWarnings("unchecked")
        private ArgumentCaptor<LongFunction<IntUnaryOperator>> groupBudgetClaimer = forClass(LongFunction.class);
        @SuppressWarnings("unchecked")
        private ArgumentCaptor<LongFunction<IntUnaryOperator>> groupBudgetReleaser = forClass(LongFunction.class);
        private ArgumentCaptor<MutableDirectBuffer> writeBuffer = forClass(MutableDirectBuffer.class);

        private MessageConsumer acceptInitial1 = mock(MessageConsumer.class);
        private MessageConsumer connectReply1 = mock(MessageConsumer.class);
        private MessageConsumer acceptInitial2 = mock(MessageConsumer.class);
        private MessageConsumer connectReply2 = mock(MessageConsumer.class);
        private boolean newStream1Started;

        private final BeginFW beginRO = new BeginFW();
        private final RouteFW routeRO = new RouteFW();

        private final BeginFW.Builder beginRW = new BeginFW.Builder();

        private String acceptName1;
        private long acceptRouteId1;
        private long acceptReplyId1;
        private long acceptCorrelationId1;
        private long connectRouteId1;
        private long connectCorrelationId1;

        private String acceptName2;
        private long acceptRouteId2;
        private long acceptReplyId2;
        private long acceptCorrelationId2;
        private long connectRouteId2;
        private long connectCorrelationId2;

        @SuppressWarnings("unchecked")
        public TestNukleusFactorySpi()
        {
            when(serverStreamFactory.setSourceCorrelationIdSupplier(supplySourceCorrelationId.capture()))
                .thenReturn(serverStreamFactory);
            when(serverStreamFactory.setTargetCorrelationIdSupplier(supplyTargetCorrelationId.capture()))
                .thenReturn(serverStreamFactory);
            when(serverStreamFactory.setInitialIdSupplier(supplyInitialId.capture())).thenReturn(serverStreamFactory);
            when(serverStreamFactory.setReplyIdSupplier(supplyReplyId.capture())).thenReturn(serverStreamFactory);
            when(serverStreamFactory.setTraceSupplier(any(LongSupplier.class))).thenReturn(serverStreamFactory);
            when(serverStreamFactory.setGroupIdSupplier(supplyGroupId.capture())).thenReturn(serverStreamFactory);
            when(serverStreamFactory.setGroupBudgetClaimer(groupBudgetClaimer.capture())).thenReturn(serverStreamFactory);
            when(serverStreamFactory.setGroupBudgetReleaser(groupBudgetReleaser.capture())).thenReturn(serverStreamFactory);
            when(serverStreamFactory.setRouteManager(router.capture())).thenReturn(serverStreamFactory);
            when(serverStreamFactory.setWriteBuffer(writeBuffer.capture())).thenReturn(serverStreamFactory);
            when(serverStreamFactory.setCounterSupplier(any(Function.class))).thenReturn(serverStreamFactory);
            when(serverStreamFactory.setAccumulatorSupplier(any(Function.class))).thenReturn(serverStreamFactory);
            when(serverStreamFactory.setBufferPoolSupplier(any(Supplier.class))).thenReturn(serverStreamFactory);
            when(serverStreamFactory.build()).thenReturn(streamFactory);

            when(streamFactory.newStream(anyInt(), any(DirectBuffer.class), anyInt(), anyInt(), any(MessageConsumer.class)))
                 .thenAnswer((invocation) ->
                 {
                     MessageConsumer result;
                     int maxLength = (int) invocation.getArgument(2) + (int) invocation.getArgument(3);
                     BeginFW begin = beginRO.wrap((DirectBuffer)invocation.getArgument(1),
                             invocation.getArgument(2), maxLength);
                     if (begin.sourceRef() == 0)
                     {
                         result = begin.correlationId() == connectCorrelationId1 ? connectReply1 : connectReply2;
                     }
                     else
                     {
                         result = newStream1Started ? acceptInitial1 : acceptInitial2;
                         newStream1Started = true;
                     }
                     return result;
                 });

            doAnswer(new Answer<Object>()
                    {
                        @Override
                        public Object answer(
                            InvocationOnMock invocation) throws Throwable
                        {
                            int maxLength = (int) invocation.getArgument(2) + (int) invocation.getArgument(3);
                            BeginFW begin = beginRO.wrap((DirectBuffer)invocation.getArgument(1),
                                    invocation.getArgument(2), maxLength);
                            long sourceRef = begin.sourceRef();
                            long authorization = begin.authorization();
                            MessagePredicate filter = (m, b, i, l) ->
                            {
                                RouteFW route = routeRO.wrap(b, i, i + l);
                                final long routeSourceRef = route.sourceRef();
                                return sourceRef == routeSourceRef;
                            };
                            RouteFW route = router.getValue().resolve(authorization, filter,
                                    (m, b, i, l) -> routeRO.wrap(b, i, i + l));
                            MutableDirectBuffer buffer = writeBuffer.getValue();
                            if (route != null)
                            {
                                MessageConsumer target = router.getValue().supplyTarget(route.target().asString());
                                long newConnectId = supplyInitialId.getValue().getAsLong();
                                acceptRouteId1 = begin.routeId();
                                acceptCorrelationId1 = begin.correlationId();
                                acceptName1 = begin.source().asString();
                                acceptReplyId1 = supplyReplyId.getValue().applyAsLong(begin.streamId());
                                connectRouteId1 = route.correlationId();
                                connectCorrelationId1 = supplyTargetCorrelationId.getValue().getAsLong();
                                final BeginFW newBegin = beginRW.wrap(buffer,  0, buffer.capacity())
                                        .routeId(connectRouteId1)
                                        .streamId(newConnectId)
                                        .authorization(begin.authorization())
                                        .source("example")
                                        .sourceRef(route.targetRef())
                                        .correlationId(connectCorrelationId1)
                                        .build();
                                target.accept(BeginFW.TYPE_ID, buffer, newBegin.offset(), newBegin.sizeof());
                            }
                            return null;
                        }
                    }
            ).when(acceptInitial1).accept(eq(BeginFW.TYPE_ID), any(DirectBuffer.class), anyInt(), anyInt());

            doAnswer(new Answer<Object>()
                    {
                        @Override
                        public Object answer(
                            InvocationOnMock invocation) throws Throwable
                        {
                            MessageConsumer acceptReply = router.getValue().supplyTarget(acceptName1);
                            MutableDirectBuffer buffer = writeBuffer.getValue();
                            final BeginFW beginOut = beginRW.wrap(buffer,  0, buffer.capacity())
                                    .routeId(acceptRouteId1)
                                    .streamId(acceptReplyId1)
                                    .source("example")
                                    .sourceRef(0L)
                                    .correlationId(acceptCorrelationId1)
                                    .build();
                            acceptReply.accept(BeginFW.TYPE_ID, buffer, beginOut.offset(), beginOut.sizeof());
                            return null;
                        }
                    }
            ).when(connectReply1).accept(eq(BeginFW.TYPE_ID), any(DirectBuffer.class), anyInt(), anyInt());

            doAnswer(new Answer<Object>()
                    {
                        @Override
                        public Object answer(
                            InvocationOnMock invocation) throws Throwable
                        {
                            int maxLength = (int) invocation.getArgument(2) + (int) invocation.getArgument(3);
                            BeginFW begin = beginRO.wrap((DirectBuffer)invocation.getArgument(1),
                                    invocation.getArgument(2), maxLength);
                            long sourceRef = begin.sourceRef();
                            long authorization = begin.authorization();
                            MessagePredicate filter = (m, b, i, l) ->
                            {
                                RouteFW route = routeRO.wrap(b, i, i + l);
                                final long routeSourceRef = route.sourceRef();
                                return sourceRef == routeSourceRef;
                            };
                            RouteFW route = router.getValue().resolve(authorization, filter,
                                    (m, b, i, l) -> routeRO.wrap(b, i, i + l));
                            MutableDirectBuffer buffer = writeBuffer.getValue();
                            if (route != null)
                            {
                                MessageConsumer target = router.getValue().supplyTarget(route.target().asString());
                                long newConnectId = supplyInitialId.getValue().getAsLong();
                                acceptRouteId2 = begin.routeId();
                                acceptCorrelationId2 = begin.correlationId();
                                acceptName2 = begin.source().asString();
                                acceptReplyId2 = supplyReplyId.getValue().applyAsLong(begin.streamId());
                                connectRouteId2 = route.correlationId();
                                connectCorrelationId2 = supplyTargetCorrelationId.getValue().getAsLong();
                                final BeginFW newBegin = beginRW.wrap(buffer,  0, buffer.capacity())
                                        .routeId(connectRouteId2)
                                        .streamId(newConnectId)
                                        .authorization(begin.authorization())
                                        .source("example")
                                        .sourceRef(route.targetRef())
                                        .correlationId(connectCorrelationId2)
                                        .build();
                                target.accept(BeginFW.TYPE_ID, buffer, newBegin.offset(), newBegin.sizeof());
                            }
                            return null;
                        }
                    }
            ).when(acceptInitial2).accept(eq(BeginFW.TYPE_ID), any(DirectBuffer.class), anyInt(), anyInt());

            doAnswer(new Answer<Object>()
                    {
                        @Override
                        public Object answer(
                            InvocationOnMock invocation) throws Throwable
                        {
                            MessageConsumer acceptReply = router.getValue().supplyTarget(acceptName2);
                            MutableDirectBuffer buffer = writeBuffer.getValue();
                            final BeginFW beginOut = beginRW.wrap(buffer,  0, buffer.capacity())
                                    .routeId(acceptRouteId2)
                                    .streamId(acceptReplyId2)
                                    .source("example")
                                    .sourceRef(0L)
                                    .correlationId(acceptCorrelationId2)
                                    .build();
                            acceptReply.accept(BeginFW.TYPE_ID, buffer, beginOut.offset(), beginOut.sizeof());
                            return null;
                        }
                    }
            ).when(connectReply2).accept(eq(BeginFW.TYPE_ID), any(DirectBuffer.class), anyInt(), anyInt());
        }

        @Override
        public String name()
        {
           return "example";
        }

        @Override
        public Nukleus create(Configuration config, NukleusBuilder builder)
        {
            return builder.configure(config)
                          .streamFactory(SERVER, serverStreamFactory)
                          .build();
        }
    }

}
