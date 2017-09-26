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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;
import org.kaazing.k3po.junit.annotation.ScriptProperty;
import org.kaazing.k3po.junit.annotation.Specification;
import org.kaazing.k3po.junit.rules.K3poRule;
import org.reaktivity.nukleus.Configuration;
import org.reaktivity.nukleus.Nukleus;
import org.reaktivity.nukleus.NukleusBuilder;
import org.reaktivity.nukleus.NukleusFactorySpi;
import org.reaktivity.reaktor.test.ReaktorRule;

public class ControllerIT
{

    private final K3poRule k3po = new K3poRule()
            .addScriptRoot("control", "org/reaktivity/specification/nukleus/control");

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
        "${control}/route/client/controller"
    })
    public void shouldRouteAsClient() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${control}/route/proxy/controller"
    })
    public void shouldRouteAsProxy() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${control}/route/server/controller"
    })
    public void shouldRouteAsServer() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${control}/route/server/controller"
    })
    @ScriptProperty("routeAuthorization [0x01 0x00 0x00 0x00 0x00 0x00 0x00 0x00]")
    public void shouldRouteAsServerWithAuthenticationRequired() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${control}/route/server/controller"
    })
    @ScriptProperty("routeAuthorization [0x01 0x00 0xc0 0x00 0x00 0x00 0x00 0x00]")
    public void shouldRouteAsServerWithRolesRequired() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${control}/route/client/controller",
        "${control}/unroute/client/controller"
    })
    public void shouldUnrouteAsClient() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${control}/route/proxy/controller",
        "${control}/unroute/proxy/controller"
    })
    public void shouldUnrouteAsProxy() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${control}/route/server/controller",
        "${control}/unroute/server/controller"
    })
    public void shouldUnrouteAsServer() throws Exception
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
            return builder.build();
        }

    }

}
