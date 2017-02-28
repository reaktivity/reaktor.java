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

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Predicate;

import org.agrona.ErrorHandler;
import org.agrona.collections.ArrayUtil;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.reaktivity.nukleus.Configuration;
import org.reaktivity.nukleus.Controller;
import org.reaktivity.nukleus.ControllerFactory;
import org.reaktivity.nukleus.Nukleus;
import org.reaktivity.nukleus.NukleusFactory;
import org.reaktivity.reaktor.matchers.ControllerMatcher;
import org.reaktivity.reaktor.matchers.NukleusMatcher;

public class ReaktorBuilder
{
    private Configuration config;
    private Predicate<String> includeNukleus;
    private Predicate<Class<? extends Controller>> includeController;
    private IdleStrategy idleStrategy;
    private ErrorHandler errorHandler;

    ReaktorBuilder()
    {
    }

    public ReaktorBuilder config(
        Configuration config)
    {
        this.config = requireNonNull(config);
        return this;
    }

    public ReaktorBuilder discover(
        NukleusMatcher include)
    {
        this.includeNukleus = requireNonNull(include);
        return this;
    }

    public ReaktorBuilder discover(
        ControllerMatcher include)
    {
        this.includeController = requireNonNull(include);
        return this;
    }

    public ReaktorBuilder idleStrategy(
        IdleStrategy idleStrategy)
    {
        this.idleStrategy = requireNonNull(idleStrategy);
        return this;
    }

    public ReaktorBuilder errorHandler(
        ErrorHandler errorHandler)
    {
        this.errorHandler = requireNonNull(errorHandler);
        return this;
    }

    public Reaktor build()
    {
        final Configuration config = this.config != null ? this.config : new Configuration();
        final NukleusFactory nukleusFactory = NukleusFactory.instantiate();

        Nukleus[] nuklei = new Nukleus[0];
        if (includeNukleus != null)
        {
            for (String name : nukleusFactory.names())
            {
                if (includeNukleus.test(name))
                {
                    Nukleus nukleus = nukleusFactory.create(name, config);
                    nuklei = ArrayUtil.add(nuklei, nukleus);
                }
            }
        }

        ControllerFactory controllerFactory = ControllerFactory.instantiate();

        Controller[] controllers = new Controller[0];
        Map<Class<? extends Controller>, Controller> controllersByKind = new HashMap<>();

        if (includeController != null)
        {
            for (Class<? extends Controller> kind : controllerFactory.kinds())
            {
                if (includeController.test(kind))
                {
                    Controller controller = controllerFactory.create(kind, config);
                    controllersByKind.put(kind, controller);
                    controllers = ArrayUtil.add(controllers, controller);
                }
            }
        }

        IdleStrategy idleStrategy = this.idleStrategy;
        if (idleStrategy == null)
        {
            idleStrategy = new BackoffIdleStrategy(64, 64, NANOSECONDS.toNanos(64L), MILLISECONDS.toNanos(1L));
        }
        ErrorHandler errorHandler = requireNonNull(this.errorHandler);

        return new Reaktor(idleStrategy, errorHandler, nuklei, controllers);
    }
}