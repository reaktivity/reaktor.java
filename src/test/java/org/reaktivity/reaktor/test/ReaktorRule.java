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
package org.reaktivity.reaktor.test;

import static java.lang.String.valueOf;
import static java.nio.file.FileVisitOption.FOLLOW_LINKS;
import static java.nio.file.Files.exists;
import static org.junit.runners.model.MultipleFailureException.assertEmpty;
import static org.reaktivity.reaktor.internal.ReaktorConfiguration.COMMAND_BUFFER_CAPACITY_PROPERTY_NAME;
import static org.reaktivity.reaktor.internal.ReaktorConfiguration.COUNTERS_BUFFER_CAPACITY_PROPERTY_NAME;
import static org.reaktivity.reaktor.internal.ReaktorConfiguration.DIRECTORY_PROPERTY_NAME;
import static org.reaktivity.reaktor.internal.ReaktorConfiguration.RESPONSE_BUFFER_CAPACITY_PROPERTY_NAME;
import static org.reaktivity.reaktor.internal.ReaktorConfiguration.STREAMS_BUFFER_CAPACITY_PROPERTY_NAME;
import static org.reaktivity.reaktor.internal.ReaktorConfiguration.THROTTLE_BUFFER_CAPACITY_PROPERTY_NAME;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.function.Predicate;

import org.agrona.LangUtil;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.reaktivity.nukleus.Controller;
import org.reaktivity.nukleus.Nukleus;
import org.reaktivity.nukleus.NukleusFactorySpi;
import org.reaktivity.reaktor.Reaktor;
import org.reaktivity.reaktor.ReaktorBuilder;
import org.reaktivity.reaktor.internal.ReaktorConfiguration;
import org.reaktivity.reaktor.test.annotation.Configure;

public final class ReaktorRule implements TestRule
{
    private final Properties properties;
    private final ReaktorBuilder builder;

    private Reaktor reaktor;

    private ReaktorConfiguration configuration;
    private boolean clean;

    public ReaktorRule()
    {
        this.builder = Reaktor.builder();
        this.properties = new Properties();
    }

    public ReaktorRule directory(String directory)
    {
        return configure(DIRECTORY_PROPERTY_NAME, directory);
    }

    public ReaktorRule commandBufferCapacity(int commandBufferCapacity)
    {
        return configure(COMMAND_BUFFER_CAPACITY_PROPERTY_NAME, commandBufferCapacity);
    }

    public ReaktorRule responseBufferCapacity(int responseBufferCapacity)
    {
        return configure(RESPONSE_BUFFER_CAPACITY_PROPERTY_NAME, responseBufferCapacity);
    }

    public ReaktorRule counterValuesBufferCapacity(int counterValuesBufferCapacity)
    {
        return configure(COUNTERS_BUFFER_CAPACITY_PROPERTY_NAME, counterValuesBufferCapacity);
    }

    public ReaktorRule streamsBufferCapacity(int streamsBufferCapacity)
    {
        return configure(STREAMS_BUFFER_CAPACITY_PROPERTY_NAME, streamsBufferCapacity);
    }

    public ReaktorRule throttleBufferCapacity(int throttleBufferCapacity)
    {
        return configure(THROTTLE_BUFFER_CAPACITY_PROPERTY_NAME, throttleBufferCapacity);
    }

    public ReaktorRule configure(String name, int value)
    {
        properties.setProperty(name, valueOf(value));
        return this;
    }

    public ReaktorRule configure(String name, String value)
    {
        properties.setProperty(name, value);
        return this;
    }

    public ReaktorRule clean()
    {
        this.clean = true;
        return this;
    }

    public ReaktorRule nukleus(
        Predicate<String> matcher)
    {
        builder.nukleus(matcher);
        return this;
    }

    public ReaktorRule loader(
        ClassLoader loader)
    {
        builder.loader(loader);
        return this;
    }

    public ReaktorRule controller(
        Predicate<Class<? extends Controller>> matcher)
    {
        builder.controller(matcher);
        return this;
    }

    public <T extends Nukleus> T nukleus(
        String name,
        Class<T> kind)
    {
        if (reaktor == null)
        {
            throw new IllegalStateException("Reaktor not started");
        }

        T nukleus = reaktor.nukleus(name, kind);
        if (nukleus == null)
        {
            throw new IllegalStateException("nukleus not found: " + name + " " + kind.getName());
        }

        return nukleus;
    }

    public ReaktorRule nukleusFactory(Class<? extends NukleusFactorySpi> factory)
    {
        loader(new NukleusClassLoader(factory.getName()));
        return this;
    }

    public <T extends Controller> T controller(
        Class<T> kind)
    {
        if (reaktor == null)
        {
            throw new IllegalStateException("Reaktor not started");
        }

        T controller = reaktor.controller(kind);
        if (controller == null)
        {
            throw new IllegalStateException("controller not found: " + kind.getName());
        }

        return controller;
    }

    private ReaktorConfiguration configuration()
    {
        if (configuration == null)
        {
            configuration = new ReaktorConfiguration(properties);
        }
        return configuration;
    }

    @Override
    public Statement apply(Statement base, Description description)
    {
        final String testMethod = description.getMethodName().replaceAll("\\[.*\\]", "");
        try
        {
            Configure[] configures = description.getTestClass()
                       .getDeclaredMethod(testMethod)
                       .getAnnotationsByType(Configure.class);
            Arrays.stream(configures).forEach(
                    p -> properties.setProperty(p.name(), p.value()));
        }
        catch (Exception e)
        {
            LangUtil.rethrowUnchecked(e);
        }

        return new Statement()
        {
            private boolean shouldDeletePath(
                Path path)
            {
                final int count = path.getNameCount();
                return "control".equals(path.getName(count - 1).toString()) ||
                       "routes".equals(path.getName(count - 1).toString()) ||
                        (count >= 2 && "streams".equals(path.getName(count - 2).toString()));
            }

            @Override
            public void evaluate() throws Throwable
            {
                ReaktorConfiguration config = configuration();
                Path directory = config.directory();

                if (clean && exists(directory))
                {
                    Files.walk(directory, FOLLOW_LINKS)
                         .filter(this::shouldDeletePath)
                         .map(Path::toFile)
                         .forEach(File::delete);
                }

                final List<Throwable> errors = new ArrayList<>();

                reaktor = builder.config(config)
                                 .errorHandler(errors::add)
                                 .build();

                try
                {
                    reaktor.start();

                    base.evaluate();
                }
                catch (Throwable t)
                {
                    errors.add(t);
                }
                finally
                {
                    try
                    {
                        reaktor.close();
                    }
                    catch (Throwable t)
                    {
                        errors.add(t);
                    }
                    finally
                    {
                        assertEmpty(errors);
                    }
                }
            }
        };
    }
}
