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
package org.reaktivity.reaktor;

import static java.util.Collections.emptySet;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.agrona.ErrorHandler;
import org.agrona.concurrent.Agent;
import org.jmock.Expectations;
import org.jmock.integration.junit4.JUnitRuleMockery;
import org.jmock.lib.concurrent.Synchroniser;
import org.junit.Rule;
import org.junit.Test;
import org.reaktivity.nukleus.Controller;
import org.reaktivity.reaktor.internal.agent.ControllerAgent;

public class ReaktorTest
{
    @Rule
    public final JUnitRuleMockery context = new JUnitRuleMockery()
    {
        {
            setThreadingPolicy(new Synchroniser());
        }
    };

    @Test
    public void shouldCloseControllers() throws Exception
    {
        final ReaktorConfiguration config = new ReaktorConfiguration();
        final Controller controller = context.mock(Controller.class);
        final ErrorHandler errorHandler = context.mock(ErrorHandler.class);

        context.checking(new Expectations()
        {
            {
                allowing(controller).process(); will(returnValue(0));

                oneOf(controller).kind(); will(returnValue(Controller.class));
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
            new Agent[] { controllerAgent });
        reaktor.start();
        reaktor.close();
    }

    @Test
    public void shouldReportControllerCloseError() throws Exception
    {
        final ReaktorConfiguration config = new ReaktorConfiguration();
        final Controller controller = context.mock(Controller.class);
        final ErrorHandler errorHandler = context.mock(ErrorHandler.class);

        context.checking(new Expectations()
        {
            {
                allowing(controller).process(); will(returnValue(0));

                oneOf(controller).kind(); will(returnValue(Controller.class));
                oneOf(controller).close(); will(throwException(new Exception("controller close failed")));
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
            new Agent[] { controllerAgent });
        reaktor.start();
        try
        {
            reaktor.close();
        }
        catch(Throwable t)
        {
            assert(t.getSuppressed().length == 0);
            throw t;
        }
    }
}
