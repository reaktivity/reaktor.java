/**
 * Copyright 2016-2020 The Reaktivity Project
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
package org.reaktivity.reaktor.internal.control;

import static java.nio.ByteBuffer.allocateDirect;
import static java.nio.ByteOrder.nativeOrder;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.rules.RuleChain.outerRule;
import static org.mockito.ArgumentCaptor.forClass;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;

import java.util.function.Consumer;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;
import org.kaazing.k3po.junit.annotation.Specification;
import org.kaazing.k3po.junit.rules.K3poRule;
import org.mockito.ArgumentCaptor;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.reaktivity.nukleus.Configuration;
import org.reaktivity.nukleus.Elektron;
import org.reaktivity.nukleus.Nukleus;
import org.reaktivity.nukleus.NukleusFactorySpi;
import org.reaktivity.nukleus.function.CommandHandler;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.reaktor.test.ReaktorRule;
import org.reaktivity.reaktor.test.internal.k3po.ext.types.control.ResolveFW;
import org.reaktivity.reaktor.test.internal.k3po.ext.types.control.ResolvedFW;
import org.reaktivity.reaktor.test.internal.k3po.ext.types.control.UnresolveFW;
import org.reaktivity.reaktor.test.internal.k3po.ext.types.control.UnresolvedFW;

public class ControlResolveIT
{

    private final K3poRule k3po = new K3poRule()
            .addScriptRoot("control", "org/reaktivity/specification/nukleus/control");

    private final TestRule timeout = new DisableOnDebug(new Timeout(5, SECONDS));

    private final ReaktorRule reaktor = new ReaktorRule()
        .nukleus("security"::equals)
        .directory("target/nukleus-itests")
        .commandBufferCapacity(1024)
        .responseBufferCapacity(1024)
        .counterValuesBufferCapacity(1024)
        .nukleusFactory(SecurityNukleusFactorySpi.class)
        .clean();

    @Rule
    public final TestRule chain = outerRule(reaktor).around(k3po).around(timeout);

    @Test
    @Specification({
        "${control}/resolve/no.roles/controller"
    })
    public void shouldResolve() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${control}/resolve/no.roles/controller",
        "${control}/unresolve/succeeds/controller"
    })
    public void shouldUnresolve() throws Exception
    {
        k3po.finish();
    }

    public static class SecurityNukleusFactorySpi implements NukleusFactorySpi
    {
        private CommandHandler resolver = mock(CommandHandler.class);
        private CommandHandler unresolver = mock(CommandHandler.class);
        @SuppressWarnings("unchecked")
        private Consumer<MessageConsumer> replyConsumer = mock(Consumer.class);
        private ArgumentCaptor<MessageConsumer> reply = forClass(MessageConsumer.class);

        private ResolveFW resolveRO = new ResolveFW();
        private UnresolveFW unresolveRO = new UnresolveFW();
        private ResolvedFW.Builder resolvedRW = new ResolvedFW.Builder();
        private UnresolvedFW.Builder unresolvedRW = new UnresolvedFW.Builder();
        private MutableDirectBuffer writeBuffer = new UnsafeBuffer(allocateDirect(1024).order(nativeOrder()));

        public SecurityNukleusFactorySpi()
        {
            doNothing().when(replyConsumer).accept(reply.capture());

            doAnswer(new Answer<Object>()
            {
                @Override
                public Object answer(
                    InvocationOnMock invocation) throws Throwable
                {
                    DirectBuffer buffer = invocation.getArgument(0);
                    int index = invocation.getArgument(1);
                    int length = invocation.getArgument(2);
                    resolveRO.wrap(buffer, index, index + length);
                    long correlationId = resolveRO.correlationId();
                    ResolvedFW result = resolvedRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                            .correlationId(correlationId)
                            .authorization(0x0001_000000000000L)
                            .build();
                    MessageConsumer reply = (MessageConsumer) invocation.getArgument(3);
                    reply.accept(result.typeId(), result.buffer(), result.offset(), result.sizeof());
                    return null;
                }
            }
            ).when(resolver).handle(any(DirectBuffer.class), anyInt(), anyInt(), any(MessageConsumer.class),
                    any(MutableDirectBuffer.class));

            doAnswer(new Answer<Object>()
            {
                @Override
                public Object answer(
                    InvocationOnMock invocation) throws Throwable
                {
                    DirectBuffer buffer = invocation.getArgument(0);
                    int index = invocation.getArgument(1);
                    int length = invocation.getArgument(2);
                    unresolveRO.wrap(buffer, index, index + length);
                    UnresolvedFW result = unresolvedRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                            .correlationId(unresolveRO.correlationId())
                            .build();
                    MessageConsumer reply = (MessageConsumer) invocation.getArgument(3);
                    reply.accept(result.typeId(), result.buffer(), result.offset(), result.sizeof());
                    return null;
                }
            }
            ).when(unresolver).handle(any(DirectBuffer.class), anyInt(), anyInt(), any(MessageConsumer.class),
                    any(MutableDirectBuffer.class));
        }


        @Override
        public String name()
        {
            return "security";
        }

        @Override
        public SecurityNukleus create(
            Configuration config)
        {
            return new SecurityNukleus(config);
        }

        private final class SecurityNukleus implements Nukleus
        {
            private final Configuration config;

            private SecurityNukleus(
                Configuration config)
            {
                this.config = config;
            }

            @Override
            public String name()
            {
                return "security";
            }

            @Override
            public Configuration config()
            {
                return config;
            }

            @Override
            public CommandHandler commandHandler(
                int msgTypeId)
            {
                CommandHandler handler = null;

                switch (msgTypeId)
                {
                case ResolveFW.TYPE_ID:
                    handler = resolver;
                    break;
                case UnresolveFW.TYPE_ID:
                    handler = unresolver;
                    break;
                }

                return handler;
            }

            @Override
            public Elektron supplyElektron()
            {
                return null;
            }
        }
    }

}
