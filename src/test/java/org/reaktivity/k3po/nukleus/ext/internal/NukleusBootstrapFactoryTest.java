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
package org.reaktivity.k3po.nukleus.ext.internal;

import static org.junit.Assert.assertNotNull;

import org.junit.Test;
import org.kaazing.k3po.driver.internal.netty.bootstrap.BootstrapFactory;
import org.kaazing.k3po.driver.internal.netty.bootstrap.ClientBootstrap;
import org.kaazing.k3po.driver.internal.netty.bootstrap.ServerBootstrap;

public class NukleusBootstrapFactoryTest
{
    @Test
    public void shouldCreateServerBootstrap() throws Exception
    {
        BootstrapFactory factory = BootstrapFactory.newBootstrapFactory();
        ServerBootstrap bootstrap = factory.newServerBootstrap("nukleus");

        assertNotNull(bootstrap);
    }

    @Test
    public void shouldCreateClientBootstrap() throws Exception
    {
        BootstrapFactory factory = BootstrapFactory.newBootstrapFactory();
        ClientBootstrap bootstrap = factory.newClientBootstrap("nukleus");

        assertNotNull(bootstrap);
    }
}
