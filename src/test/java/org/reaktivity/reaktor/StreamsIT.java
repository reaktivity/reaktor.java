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
package org.reaktivity.reaktor;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.rules.RuleChain.outerRule;
import static org.mockito.ArgumentCaptor.forClass;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.reaktivity.nukleus.route.RouteKind.SERVER;

import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Long2ObjectHashMap;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;
import org.kaazing.k3po.junit.annotation.ScriptProperty;
import org.kaazing.k3po.junit.annotation.Specification;
import org.kaazing.k3po.junit.rules.K3poRule;
import org.mockito.ArgumentCaptor;
import org.mockito.stubbing.Answer;
import org.reaktivity.nukleus.Configuration;
import org.reaktivity.nukleus.Nukleus;
import org.reaktivity.nukleus.NukleusBuilder;
import org.reaktivity.nukleus.NukleusFactorySpi;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.route.RouteManager;
import org.reaktivity.nukleus.stream.StreamFactory;
import org.reaktivity.nukleus.stream.StreamFactoryBuilder;
import org.reaktivity.reaktor.test.ReaktorRule;

public class StreamsIT
{
    private final K3poRule k3po = new K3poRule()
            .addScriptRoot("route", "org/reaktivity/specification/nukleus/control/route")
            .addScriptRoot("streams", "org/reaktivity/specification/nukleus/streams");

    private final TestRule timeout = new DisableOnDebug(new Timeout(5, SECONDS));

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
        "${streams}/connection.established.authorized/client",
        "${streams}/connection.established.authorized/server"
    })
    @ScriptProperty("routeAuthorization [0x01 0x00 0x80 0x00 0x00 0x00 0x00 0x00]")
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
        private ArgumentCaptor<MessageConsumer> throttle = forClass(MessageConsumer.class);

        private final Long2ObjectHashMap<TestStream.Accepted> correlations = new Long2ObjectHashMap<>();

        @SuppressWarnings("unchecked")
        public TestNukleusFactorySpi()
        {
            when(serverStreamFactory.setCorrelationIdSupplier(supplyCorrelationId.capture())).thenReturn(serverStreamFactory);
            when(serverStreamFactory.setStreamIdSupplier(supplyStreamId.capture())).thenReturn(serverStreamFactory);
            when(serverStreamFactory.setRouteManager(router.capture())).thenReturn(serverStreamFactory);
            when(serverStreamFactory.setWriteBuffer(writeBuffer.capture())).thenReturn(serverStreamFactory);
            when(serverStreamFactory.setBufferPoolSupplier(any(Supplier.class))).thenReturn(serverStreamFactory);
            when(serverStreamFactory.setCounterSupplier(any(Function.class))).thenReturn(serverStreamFactory);
            when(serverStreamFactory.build()).thenReturn(streamFactory);

            when(streamFactory.newStream(anyInt(), any(DirectBuffer.class), anyInt(), anyInt(), throttle.capture()))
                 .thenAnswer(newStreamAnswer());
        }

        private Answer<MessageConsumer> newStreamAnswer()
        {
            return (invocation) ->
                new TestStream(
                        router.getValue(),
                        supplyStreamId.getValue(),
                        supplyCorrelationId.getValue(),
                        writeBuffer.getValue(),
                        throttle.getValue(),
                        correlations);
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
