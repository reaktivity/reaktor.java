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
import java.util.function.LongSupplier;

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
import org.reaktivity.nukleus.buffer.MemoryManager;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.function.MessagePredicate;
import org.reaktivity.nukleus.route.RouteManager;
import org.reaktivity.nukleus.stream.StreamFactory;
import org.reaktivity.nukleus.stream.StreamFactoryBuilder;
import org.reaktivity.reaktor.internal.types.control.RouteFW;
import org.reaktivity.reaktor.internal.types.stream.AckFW;
import org.reaktivity.reaktor.internal.types.stream.BeginFW;
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

        private ArgumentCaptor<LongSupplier> supplyCorrelationId = forClass(LongSupplier.class);
        private ArgumentCaptor<RouteManager> router = forClass(RouteManager.class);
        private ArgumentCaptor<LongSupplier> supplyStreamId = forClass(LongSupplier.class);
        private ArgumentCaptor<MutableDirectBuffer> writeBuffer = forClass(MutableDirectBuffer.class);

        private MessageConsumer acceptStream = mock(MessageConsumer.class);
        private MessageConsumer connectThrottle = mock(MessageConsumer.class);
        private MessageConsumer connectReplyStream = mock(MessageConsumer.class);
        private ArgumentCaptor<MessageConsumer> acceptThrottle = forClass(MessageConsumer.class);
        private ArgumentCaptor<MessageConsumer> connectReplyThrottle = forClass(MessageConsumer.class);

        private final RouteFW routeRO = new RouteFW();
        private final BeginFW beginRO = new BeginFW();
        private final AckFW ackRO = new AckFW();

        private final BeginFW.Builder beginRW = new BeginFW.Builder();
        private final AckFW.Builder ackRW = new AckFW.Builder();

        private long newCorrelationId;
        private long correlationId;
        private String source;

        @SuppressWarnings("unchecked")
        public TestNukleusFactorySpi()
        {
            when(serverStreamFactory.setCorrelationIdSupplier(supplyCorrelationId.capture())).thenReturn(serverStreamFactory);
            when(serverStreamFactory.setStreamIdSupplier(supplyStreamId.capture())).thenReturn(serverStreamFactory);
            when(serverStreamFactory.setRouteManager(router.capture())).thenReturn(serverStreamFactory);
            when(serverStreamFactory.setWriteBuffer(writeBuffer.capture())).thenReturn(serverStreamFactory);
            when(serverStreamFactory.setCounterSupplier(any(Function.class))).thenReturn(serverStreamFactory);
            when(serverStreamFactory.setAccumulatorSupplier(any(Function.class))).thenReturn(serverStreamFactory);
            when(serverStreamFactory.setMemoryManager(any(MemoryManager.class))).thenReturn(serverStreamFactory);
            when(serverStreamFactory.build()).thenReturn(streamFactory);

            when(streamFactory.newStream(anyInt(), any(DirectBuffer.class), anyInt(), anyInt(), acceptThrottle.capture()))
                 .thenAnswer(invocation ->
                 {
                     when(streamFactory.newStream(anyInt(), any(DirectBuffer.class), anyInt(), anyInt(),
                             connectReplyThrottle.capture()))
                         .thenReturn(connectReplyStream);
                     return acceptStream;
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
                    long sourceId = begin.streamId();
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
                        String targetName = route.target().asString();
                        MessageConsumer target = router.getValue().supplyTarget(targetName);
                        long newConnectId = supplyStreamId.getValue().getAsLong();
                        newCorrelationId = supplyCorrelationId.getValue().getAsLong();
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

                        router.getValue().setThrottle(targetName, newConnectId, connectThrottle);

                        doAnswer(new Answer<Object>()
                        {
                            @Override
                            public Object answer(
                                InvocationOnMock invocation) throws Throwable
                            {
                                final int offset = invocation.getArgument(2);
                                final int maxLimit = offset + (Integer) invocation.getArgument(3);
                                final AckFW ack = ackRO.wrap((DirectBuffer)invocation.getArgument(1), offset, maxLimit);
                                final int flags = ack.flags();

                                final MessageConsumer throttle = acceptThrottle.getValue();
                                final MutableDirectBuffer buffer = writeBuffer.getValue();
                                final AckFW ackOut = ackRW.wrap(buffer,  0, buffer.capacity())
                                        .streamId(sourceId)
                                        .flags(flags)
                                        .build();
                                throttle.accept(ackOut.typeId(), ackOut.buffer(), ackOut.offset(), ackOut.sizeof());
                                return null;
                            }
                        }).when(connectThrottle).accept(eq(AckFW.TYPE_ID), any(DirectBuffer.class), anyInt(), anyInt());
                    }
                    else
                    {
                        final AckFW ack = ackRW.wrap(buffer, 0, buffer.capacity())
                                .streamId(sourceId)
                                .flags(0x02) // rst
                                .build();
                        acceptThrottle.getValue().accept(
                                ack.typeId(), ack.buffer(), ack.offset(), ack.sizeof());
                    }

                    return null;
                }
            }).when(acceptStream).accept(eq(BeginFW.TYPE_ID), any(DirectBuffer.class), anyInt(), anyInt());

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
            }).when(connectReplyStream).accept(eq(BeginFW.TYPE_ID), any(DirectBuffer.class), anyInt(), anyInt());
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
