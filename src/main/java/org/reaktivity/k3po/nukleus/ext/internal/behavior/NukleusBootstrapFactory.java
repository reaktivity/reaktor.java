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
package org.reaktivity.k3po.nukleus.ext.internal.behavior;

import java.util.Properties;

import org.jboss.netty.channel.ChannelFactory;
import org.kaazing.k3po.driver.internal.netty.bootstrap.BootstrapFactorySpi;
import org.kaazing.k3po.driver.internal.netty.bootstrap.ClientBootstrap;
import org.kaazing.k3po.driver.internal.netty.bootstrap.ServerBootstrap;
import org.reaktivity.nukleus.Configuration;

public class NukleusBootstrapFactory extends BootstrapFactorySpi
{
    private final ChannelFactory clientChannelFactory;
    private final ChannelFactory serverChannelFactory;
    private final NukleusReaktorPool reaktorPool;

    public NukleusBootstrapFactory()
    {
        // TODO: parameterize
        Properties properties = new Properties();
        properties.setProperty(Configuration.DIRECTORY_PROPERTY_NAME, "target/nukleus-itests");
        Configuration config = new Configuration(properties);

        this.reaktorPool = new NukleusReaktorPool(config);

        this.clientChannelFactory = new NukleusClientChannelFactory(reaktorPool);
        this.serverChannelFactory = new NukleusServerChannelFactory(reaktorPool);
    }

    @Override
    public String getTransportName()
    {
        return "nukleus";
    }

    @Override
    public ClientBootstrap newClientBootstrap()
            throws Exception
    {
        return new ClientBootstrap(clientChannelFactory);
    }

    @Override
    public ServerBootstrap newServerBootstrap()
            throws Exception
    {
        return new ServerBootstrap(serverChannelFactory);
    }

    @Override
    public void shutdown()
    {
        reaktorPool.shutdown();
        clientChannelFactory.shutdown();
        serverChannelFactory.shutdown();
    }


    @Override
    public void releaseExternalResources()
    {
        shutdown();
        reaktorPool.releaseExternalResources();
        clientChannelFactory.releaseExternalResources();
        serverChannelFactory.releaseExternalResources();
    }
}
