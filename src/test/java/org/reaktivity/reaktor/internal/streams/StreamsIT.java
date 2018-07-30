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
import org.reaktivity.reaktor.internal.types.stream.ResetFW;
import org.reaktivity.reaktor.test.ReaktorRule;

public class StreamsIT
{
    private final K3poRule k3po = new K3poRule()
            .addScriptRoot("route", "org/reaktivity/specification/nukleus/control/route")
            .addScriptRoot("streams", "org/reaktivity/specification/nukleus/streams");

    private final TestRule timeout = new DisableOnDebug(new Timeout(15, SECONDS));

    private final ReaktorRule reaktor = new ReaktorRule()
        .nukleus("example"::equals)
        .directory("target/nukleus-itests")
        .commandBufferCapacity(1024)
        .responseBufferCapacity(1024)
        .counterValuesBufferCapacity(1024)
        .nukleusFactory(TestNukleusFactorySpi.class)
        .clean();

    @Rule
    public final TestRule chain = outerRule(reaktor).around(k3po).around(timeout);

    @Test
    @Specification({
        "${route}/server/controller",
        "${streams}/connection.established/client",
        "${streams}/connection.established/server"
    })
    public void shouldEstablishConnection() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${streams}/connection.established/client",
        "${streams}/connection.established/server"
    })
    @ScriptProperty({"routeAuthorization  0x0001_000000000080L",
                     "streamAuthorization 0x0001_000000000081L"})
    public void shouldEstablishAuthorizedConnection() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${streams}/connection.refused.not.authorized/client"
    })
    @ScriptProperty("routeAuthorization [0x01 0x00 0x81 0x00 0x00 0x00 0x00 0x00]")
    public void shoulResetConnectionWhenNotAuthorizedMissingARequiredRole() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${streams}/connection.refused.not.authorized/client"
    })
    @ScriptProperty("routeAuthorization [0x02 0x00 0x80 0x00 0x00 0x00 0x00 0x00]")
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
        private ArgumentCaptor<LongSupplier> supplyTargetCorrelationId = forClass(LongSupplier.class);
        private ArgumentCaptor<RouteManager> router = forClass(RouteManager.class);
        private ArgumentCaptor<LongSupplier> supplyStreamId = forClass(LongSupplier.class);
        private ArgumentCaptor<LongSupplier> supplyGroupId = forClass(LongSupplier.class);
        @SuppressWarnings("unchecked")
        private ArgumentCaptor<LongFunction<IntUnaryOperator>> groupBudgetClaimer = forClass(LongFunction.class);
        @SuppressWarnings("unchecked")
        private ArgumentCaptor<LongFunction<IntUnaryOperator>> groupBudgetReleaser = forClass(LongFunction.class);
        private ArgumentCaptor<MutableDirectBuffer> writeBuffer = forClass(MutableDirectBuffer.class);

        private MessageConsumer newStream = mock(MessageConsumer.class);
        private MessageConsumer replyStream = mock(MessageConsumer.class);
        private ArgumentCaptor<MessageConsumer> newStreamThrottle = forClass(MessageConsumer.class);
        private ArgumentCaptor<MessageConsumer> replyStreamThrottle = forClass(MessageConsumer.class);

        private final BeginFW beginRO = new BeginFW();
        private final RouteFW routeRO = new RouteFW();

        private final BeginFW.Builder beginRW = new BeginFW.Builder();
        private final ResetFW.Builder resetRW = new ResetFW.Builder();

        private long newCorrelationId;
        private long correlationId;
        private String source;

        @SuppressWarnings("unchecked")
        public TestNukleusFactorySpi()
        {
            when(serverStreamFactory.setSourceCorrelationIdSupplier(supplySourceCorrelationId.capture()))
                .thenReturn(serverStreamFactory);
            when(serverStreamFactory.setTargetCorrelationIdSupplier(supplyTargetCorrelationId.capture()))
                .thenReturn(serverStreamFactory);
            when(serverStreamFactory.setStreamIdSupplier(supplyStreamId.capture())).thenReturn(serverStreamFactory);
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

            when(streamFactory.newStream(anyInt(), any(DirectBuffer.class), anyInt(), anyInt(), newStreamThrottle.capture()))
                 .thenAnswer((invocation) ->
                 {
                     when(streamFactory.newStream(anyInt(), any(DirectBuffer.class), anyInt(), anyInt(),
                             replyStreamThrottle.capture()))
                         .thenReturn(replyStream);
                     return newStream;
                 });

            doAnswer(new Answer<Object>()
                    {
                        @Override
                        public Object answer(
                            InvocationOnMock invocation) throws Throwable
                        {
                            int offset = invocation.getArgument(2);
                            int maxLimit = offset + (Integer) invocation.getArgument(3);
                            BeginFW begin = beginRO.wrap((DirectBuffer)invocation.getArgument(1),
                                    offset, maxLimit);
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
                                long newConnectId = supplyStreamId.getValue().getAsLong();
                                newCorrelationId = supplyTargetCorrelationId.getValue().getAsLong();
                                correlationId = begin.correlationId();
                                source = begin.source().asString();
                                final BeginFW newBegin = beginRW.wrap(buffer,  0, buffer.capacity())
                                        .streamId(newConnectId)
                                        .authorization(begin.authorization())
                                        .source("example")
                                        .sourceRef(route.targetRef())
                                        .correlationId(newCorrelationId)
                                        .build();
                                target.accept(BeginFW.TYPE_ID, buffer, newBegin.offset(), newBegin.sizeof());
                            }
                            else
                            {
                                final ResetFW reset = resetRW.wrap(buffer, 0, buffer.capacity())
                                        .streamId(begin.streamId())
                                        .build();
                                newStreamThrottle.getValue().accept(
                                        reset.typeId(), reset.buffer(), reset.offset(), reset.sizeof());
                            }
                            return null;
                        }
                    }
            ).when(newStream).accept(eq(BeginFW.TYPE_ID), any(DirectBuffer.class), anyInt(), anyInt());

            doAnswer(new Answer<Object>()
                    {
                        @Override
                        public Object answer(
                            InvocationOnMock invocation) throws Throwable
                        {
                            MessageConsumer acceptReply = router.getValue().supplyTarget(source);
                            long newReplyId = supplyStreamId.getValue().getAsLong();
                            MutableDirectBuffer buffer = writeBuffer.getValue();
                            final BeginFW beginOut = beginRW.wrap(buffer,  0, buffer.capacity())
                                    .streamId(newReplyId)
                                    .source("example")
                                    .sourceRef(0L)
                                    .correlationId(correlationId)
                                    .build();
                            acceptReply.accept(BeginFW.TYPE_ID, buffer, beginOut.offset(), beginOut.sizeof());
                            return null;
                        }
                    }
            ).when(replyStream).accept(eq(BeginFW.TYPE_ID), any(DirectBuffer.class), anyInt(), anyInt());
        }

        @Override
        public String name()
        {
           return "example";
        }

        @Override
        public Nukleus create(Configuration config, NukleusBuilder builder)
        {
            return builder.streamFactory(SERVER, serverStreamFactory)
                   .build();
        }
    }

}
