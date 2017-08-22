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
import static org.reaktivity.nukleus.route.RouteKind.SERVER;
import static org.reaktivity.reaktor.test.TestUtil.toTestRule;

import org.jmock.Expectations;
import org.jmock.integration.junit4.JUnitRuleMockery;
import org.jmock.lib.concurrent.Synchroniser;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;
import org.kaazing.k3po.junit.annotation.Specification;
import org.kaazing.k3po.junit.rules.K3poRule;
import org.reaktivity.nukleus.Configuration;
import org.reaktivity.nukleus.Nukleus;
import org.reaktivity.nukleus.NukleusBuilder;
import org.reaktivity.nukleus.NukleusFactorySpi;
import org.reaktivity.nukleus.route.RouteManager;
import org.reaktivity.nukleus.stream.StreamFactoryBuilder;
import org.reaktivity.reaktor.test.NukleusClassLoader;
import org.reaktivity.reaktor.test.ReaktorRule;

public class StreamsIT
{
    Nukleus testNukleus;

    private static StreamFactoryBuilder serverStreamFactory;

    @Rule
    public JUnitRuleMockery context = new JUnitRuleMockery()
    {
        {
            setThreadingPolicy(new Synchroniser());
            testNukleus = mock(Nukleus.class);
            serverStreamFactory = mock(StreamFactoryBuilder.class, "serverStreamFactory");

            checking(new Expectations()
            {
                {
                    oneOf(serverStreamFactory).setRouteManager(with(any(RouteManager.class)));
                }
            });
        }
    };

    private final K3poRule k3po = new K3poRule();

    private final TestRule timeout = new DisableOnDebug(new Timeout(5, SECONDS));

    private final ReaktorRule reaktor = new ReaktorRule()
        .nukleus("example"::equals)
        .directory("target/nukleus-itests")
        .commandBufferCapacity(1024)
        .responseBufferCapacity(1024)
        .counterValuesBufferCapacity(1024)
        .loader(new NukleusClassLoader(TestNukleusFactorySpi.class.getName()))
        .clean();

    @Rule
    public final TestRule chain = outerRule(toTestRule(context)).around(reaktor).around(k3po).around(timeout);

    @Test
    @Specification({
        "server/new.accept.stream"
    })
    public void shouldBeginServerAcceptStream() throws Exception
    {
        k3po.finish();
    }

    public static class TestNukleusFactorySpi implements NukleusFactorySpi
    {

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
