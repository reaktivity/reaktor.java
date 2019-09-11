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
package org.reaktivity.reaktor.internal.control;

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
import org.reaktivity.nukleus.Elektron;
import org.reaktivity.nukleus.Nukleus;
import org.reaktivity.nukleus.NukleusFactorySpi;
import org.reaktivity.reaktor.test.ReaktorRule;

public class ControlIT
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
        .nukleusFactory(ExampleNukleusFactorySpi.class)
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
        "${control}/route/server/multiple.routes/controller"
    })
    public void shouldRouteAsServerWithMultipleRoutes() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${control}/route/server/multiple.authorizations/controller"
    })
    @ScriptProperty({"route1Authorization 0x0001_000000000000L",
                     "route2Authorization 0x0002_000000000000L"})
    public void shouldRouteByAuthorizationAsServer() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${control}/route/server/multiple.extensions/controller"
    })
    public void shouldRouteByExtensionAsServer() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${control}/route/server/controller"
    })
    @ScriptProperty("routeAuthorization 0x0001_000000000000L")
    public void shouldRouteAsServerWithAuthenticationRequired() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${control}/route/server/controller"
    })
    @ScriptProperty("routeAuthorization 0x0001_000000000003L")
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

    @Test
    @Specification({
        "${control}/route/server/multiple.routes/controller",
        "${control}/unroute/server/multiple.routes/controller"
    })
    @ScriptProperty({"route1Authorization 0x0001_000000000000L",
                     "route2Authorization 0x0001_000000000001L"})
    public void shouldUnrouteAsServerWithMultipleRoutes() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${control}/route/server/multiple.authorizations/controller",
        "${control}/unroute/server/multiple.authorizations/controller"
    })
    @ScriptProperty({"route1Authorization 0x0001_000000000000L",
                     "route2Authorization 0x0002_000000000000L"})
    public void shouldUnrouteByAuthorizationAsServer() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${control}/route/server/multiple.extensions/controller",
        "${control}/unroute/server/multiple.extensions/controller"
    })
    public void shouldUnrouteByExtensionAsServer() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${control}/freeze/controller",
    })
    public void shouldFreeze() throws Exception
    {
        k3po.finish();
    }

    public static class ExampleNukleusFactorySpi implements NukleusFactorySpi
    {
        @Override
        public String name()
        {
            return "example";
        }

        @Override
        public ExampleNukleus create(
            Configuration config)
        {
            return new ExampleNukleus(config);
        }

        private static final class ExampleNukleus implements Nukleus
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

            private static final class ExampleElektron implements Elektron
            {
            }
        }
    }

}
