/**
 * Copyright 2016-2021 The Reaktivity Project
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

import static java.util.Collections.emptySet;
import static org.reaktivity.reaktor.ReaktorConfiguration.REAKTOR_DIRECTORY;

import java.util.BitSet;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import org.agrona.ErrorHandler;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.UnsafeBuffer;
import org.jmock.Expectations;
import org.jmock.integration.junit4.JUnitRuleMockery;
import org.jmock.lib.concurrent.Synchroniser;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.reaktivity.nukleus.Controller;
import org.reaktivity.nukleus.Elektron;
import org.reaktivity.nukleus.Nukleus;
import org.reaktivity.nukleus.route.RouteKind;
import org.reaktivity.reaktor.internal.agent.ControllerAgent;
import org.reaktivity.reaktor.internal.agent.ElektronAgent;
import org.reaktivity.reaktor.internal.agent.NukleusAgent;
import org.reaktivity.reaktor.internal.router.RouteId;
import org.reaktivity.reaktor.internal.types.OctetsFW;
import org.reaktivity.reaktor.internal.types.control.Role;

public class ReaktorTest
{
    private static final OctetsFW EMPTY_OCTETS = new OctetsFW().wrap(new UnsafeBuffer(new byte[0]), 0, 0);

    @Rule
    public final TemporaryFolder folder = new TemporaryFolder();

    @Rule
    public final JUnitRuleMockery context = new JUnitRuleMockery()
    {
        {
            setThreadingPolicy(new Synchroniser());
        }
    };

    @Test
    public void shouldCloseOnElektronAgentError() throws Exception
    {
        final Properties properties = new Properties();
        properties.setProperty(REAKTOR_DIRECTORY.name(), folder.getRoot().getAbsolutePath());

        final ReaktorConfiguration config = new ReaktorConfiguration(properties);
        final Nukleus nukleus = context.mock(Nukleus.class);
        final Elektron elektron = context.mock(Elektron.class);
        final Agent agent = context.mock(Agent.class);
        final ErrorHandler errorHandler = context.mock(ErrorHandler.class);

        context.checking(new Expectations()
        {
            {
                allowing(nukleus).name();
                will(returnValue("test"));

                allowing(elektron).streamFactoryBuilder(with(any(RouteKind.class)));
                will(returnValue(null));

                allowing(elektron).addressFactoryBuilder(with(any(RouteKind.class)));
                will(returnValue(null));

                oneOf(nukleus).supplyElektron(with(any(int.class)));
                will(returnValue(elektron));

                oneOf(elektron).agent();
                will(returnValue(agent));

                oneOf(agent).doWork();
                will(throwException(new Exception("elektron agent failed")));

                oneOf(errorHandler).onError(with(aNonNull(Exception.class)));
                oneOf(agent).onClose();
            }
        });

        final ExecutorService executor = Executors.newFixedThreadPool(1);
        final NukleusAgent nukleusAgent = new NukleusAgent(config, null);
        final BitSet allBitsSet = BitSet.valueOf(new long[] { -1L });
        final ElektronAgent elektronAgent = nukleusAgent.supplyElektronAgent(0, 1, executor, a -> allBitsSet);
        final Thread[] threadRef = new Thread[2];
        final AtomicInteger threadCount = new AtomicInteger();
        final ThreadFactory threadFactory = task -> threadRef[threadCount.getAndIncrement()] = new Thread(task);
        final int localId = nukleusAgent.labels().supplyLabelId("test");
        final long routeId = RouteId.routeId(localId, 1, Role.SERVER, 1);
        nukleusAgent.assign(nukleus);
        nukleusAgent.onRouteable(routeId, nukleus);
        nukleusAgent.onRouted(nukleus, RouteKind.SERVER, routeId, EMPTY_OCTETS);


        try (Reaktor reaktor = new Reaktor(
            config,
            errorHandler,
            emptySet(),
            executor,
            new Agent[] { nukleusAgent, elektronAgent },
            threadFactory))
        {
            reaktor.start();
            threadRef[1].join();
        }
    }

    @Test
    public void shouldCloseControllers() throws Exception
    {
        final Properties properties = new Properties();
        properties.setProperty(REAKTOR_DIRECTORY.name(), folder.getRoot().getAbsolutePath());

        final ReaktorConfiguration config = new ReaktorConfiguration(properties);
        final Controller controller = context.mock(Controller.class);
        final ErrorHandler errorHandler = context.mock(ErrorHandler.class);

        context.checking(new Expectations()
        {
            {
                allowing(controller).process();
                will(returnValue(0));

                oneOf(controller).kind();
                will(returnValue(Controller.class));

                oneOf(controller).close();
            }
        });

        final ExecutorService executor = Executors.newFixedThreadPool(1);
        final ControllerAgent controllerAgent = new ControllerAgent();
        controllerAgent.assign(controller);

        Reaktor reaktor = new Reaktor(
            config,
            errorHandler,
            emptySet(),
            executor,
            new Agent[] { controllerAgent },
            Thread::new);
        reaktor.start();
        reaktor.close();
    }

    @Test
    public void shouldReportControllerCloseError() throws Exception
    {
        final Properties properties = new Properties();
        properties.setProperty(REAKTOR_DIRECTORY.name(), folder.getRoot().getAbsolutePath());

        final ReaktorConfiguration config = new ReaktorConfiguration(properties);
        final Controller controller = context.mock(Controller.class);
        final ErrorHandler errorHandler = context.mock(ErrorHandler.class);

        context.checking(new Expectations()
        {
            {
                allowing(controller).process();
                will(returnValue(0));

                oneOf(controller).kind();
                will(returnValue(Controller.class));

                oneOf(controller).close();
                will(throwException(new Exception("controller close failed")));

                oneOf(errorHandler).onError(with(aNonNull(Exception.class)));
            }
        });

        final ExecutorService executor = Executors.newFixedThreadPool(1);
        final ControllerAgent controllerAgent = new ControllerAgent();
        controllerAgent.assign(controller);

        Reaktor reaktor = new Reaktor(
            config,
            errorHandler,
            emptySet(),
            executor,
            new Agent[] { controllerAgent },
            Thread::new);
        reaktor.start();
        try
        {
            reaktor.close();
        }
        catch (Throwable t)
        {
            assert t.getSuppressed().length == 0;
            throw t;
        }
    }
}
