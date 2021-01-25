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
package org.reaktivity.reaktor.test;

import static java.lang.String.format;
import static java.nio.file.FileVisitOption.FOLLOW_LINKS;
import static java.nio.file.Files.exists;
import static java.util.Objects.requireNonNull;
import static org.junit.runners.model.MultipleFailureException.assertEmpty;
import static org.reaktivity.reaktor.ReaktorConfiguration.REAKTOR_COMMAND_BUFFER_CAPACITY;
import static org.reaktivity.reaktor.ReaktorConfiguration.REAKTOR_COUNTERS_BUFFER_CAPACITY;
import static org.reaktivity.reaktor.ReaktorConfiguration.REAKTOR_DIRECTORY;
import static org.reaktivity.reaktor.ReaktorConfiguration.REAKTOR_DRAIN_ON_CLOSE;
import static org.reaktivity.reaktor.ReaktorConfiguration.REAKTOR_RESPONSE_BUFFER_CAPACITY;
import static org.reaktivity.reaktor.ReaktorConfiguration.REAKTOR_ROUTED_DELAY_MILLIS;
import static org.reaktivity.reaktor.ReaktorConfiguration.REAKTOR_STREAMS_BUFFER_CAPACITY;
import static org.reaktivity.reaktor.ReaktorConfiguration.REAKTOR_SYNTHETIC_ABORT;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import org.agrona.ErrorHandler;
import org.agrona.LangUtil;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.reaktivity.nukleus.Configuration.PropertyDef;
import org.reaktivity.nukleus.Controller;
import org.reaktivity.nukleus.Nukleus;
import org.reaktivity.nukleus.NukleusFactorySpi;
import org.reaktivity.reaktor.Reaktor;
import org.reaktivity.reaktor.ReaktorBuilder;
import org.reaktivity.reaktor.ReaktorConfiguration;
import org.reaktivity.reaktor.test.annotation.Configure;

public final class ReaktorRule implements TestRule
{
    public static final long EXTERNAL_AFFINITY_MASK = 1L << (Long.SIZE - 1);

    // needed by test annotations
    public static final String REAKTOR_BUFFER_POOL_CAPACITY_NAME = "reaktor.buffer.pool.capacity";
    public static final String REAKTOR_BUFFER_SLOT_CAPACITY_NAME = "reaktor.buffer.slot.capacity";

    private static final Pattern DATA_FILENAME_PATTERN = Pattern.compile("data\\d+");

    private final Properties properties;
    private final ReaktorBuilder builder;

    private Reaktor reaktor;

    private ReaktorConfiguration configuration;
    private boolean clean;

    public ReaktorRule()
    {
        this.builder = Reaktor.builder();
        this.properties = new Properties();

        configure(REAKTOR_DRAIN_ON_CLOSE, true);
        configure(REAKTOR_SYNTHETIC_ABORT, true);
        configure(REAKTOR_ROUTED_DELAY_MILLIS, 500L);
    }

    public ReaktorRule directory(String directory)
    {
        return configure(REAKTOR_DIRECTORY, directory);
    }

    public ReaktorRule commandBufferCapacity(int commandBufferCapacity)
    {
        return configure(REAKTOR_COMMAND_BUFFER_CAPACITY, commandBufferCapacity);
    }

    public ReaktorRule responseBufferCapacity(int responseBufferCapacity)
    {
        return configure(REAKTOR_RESPONSE_BUFFER_CAPACITY, responseBufferCapacity);
    }

    public ReaktorRule counterValuesBufferCapacity(int counterValuesBufferCapacity)
    {
        return configure(REAKTOR_COUNTERS_BUFFER_CAPACITY, counterValuesBufferCapacity);
    }

    public ReaktorRule streamsBufferCapacity(int streamsBufferCapacity)
    {
        return configure(REAKTOR_STREAMS_BUFFER_CAPACITY, streamsBufferCapacity);
    }

    public <T> ReaktorRule configure(
        PropertyDef<T> property,
        T value)
    {
        properties.setProperty(property.name(), value.toString());
        return this;
    }

    public ReaktorRule configure(
        String name,
        String value)
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
        Predicate<String> matcher)
    {
        builder.controller(matcher);
        return this;
    }

    public ReaktorRule affinityMask(
        String address,
        long affinityMask)
    {
        builder.affinityMask(address, affinityMask);
        return this;
    }

    public ReaktorRule nukleusFactory(
        Class<? extends NukleusFactorySpi> factory)
    {
        loader(Services.newLoader(NukleusFactorySpi.class, factory));
        return this;
    }

    public <T extends Controller> T controller(
        Class<T> kind)
    {
        ensureReaktorStarted();

        return requireNonNull(reaktor.controller(kind));
    }

    public <T extends Nukleus> T nukleus(
        Class<T> kind)
    {
        ensureReaktorStarted();

        return requireNonNull(reaktor.nukleus(kind));
    }

    public long opensRead(
        String nukleus,
        long routeId)
    {
        return counter(format("%s.%d.opens.read", nukleus, routeId));
    }

    public long opensWritten(
        String nukleus,
        long routeId)
    {
        return counter(format("%s.%d.opens.written", nukleus, routeId));
    }

    public long closesRead(
        String nukleus,
        long routeId)
    {
        return counter(format("%s.%d.closes.read", nukleus, routeId));
    }

    public long closesWritten(
        String nukleus,
        long routeId)
    {
        return counter(format("%s.%d.closes.written", nukleus, routeId));
    }

    public long abortsRead(
        String nukleus,
        long routeId)
    {
        return counter(format("%s.%d.aborts.read", nukleus, routeId));
    }

    public long abortsWritten(
        String nukleus,
        long routeId)
    {
        return counter(format("%s.%d.aborts.written", nukleus, routeId));
    }

    public long resetsRead(
        String nukleus,
        long routeId)
    {
        return counter(format("%s.%d.resets.read", nukleus, routeId));
    }

    public long resetsWritten(
        String nukleus,
        long routeId)
    {
        return counter(format("%s.%d.resets.written", nukleus, routeId));
    }

    public long bytesRead(
        String nukleus,
        long routeId)
    {
        return counter(format("%s.%d.bytes.read", nukleus, routeId));
    }

    public long bytesWritten(
        String nukleus,
        long routeId)
    {
        return counter(format("%s.%d.bytes.written", nukleus, routeId));
    }

    public long framesRead(
        String nukleus,
        long routeId)
    {
        return counter(format("%s.%d.frames.read", nukleus, routeId));
    }

    public long framesWritten(
        String nukleus,
        long routeId)
    {
        return counter(format("%s.%d.frames.written", nukleus, routeId));
    }

    public long counter(
        String name)
    {
        ensureReaktorStarted();

        return reaktor.counter(name);
    }

    private ReaktorConfiguration configuration()
    {
        if (configuration == null)
        {
            configuration = new ReaktorConfiguration(properties);
        }
        return configuration;
    }

    private void ensureReaktorStarted()
    {
        if (reaktor == null)
        {
            throw new IllegalStateException("Reaktor not started");
        }
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
                String filename = path.getFileName().toString();
                return "control".equals(filename) ||
                       "routes".equals(filename) ||
                       "streams".equals(filename) ||
                       "labels".equals(filename) ||
                       DATA_FILENAME_PATTERN.matcher(filename).matches();
            }

            @Override
            public void evaluate() throws Throwable
            {
                ReaktorConfiguration config = configuration();
                Path directory = config.directory();
                Path cacheDirectory = config.cacheDirectory();

                if (clean && exists(directory))
                {
                    Files.walk(directory, FOLLOW_LINKS)
                         .filter(this::shouldDeletePath)
                         .map(Path::toFile)
                         .forEach(File::delete);
                }

                if (clean && exists(cacheDirectory))
                {
                    Files.walk(cacheDirectory)
                         .map(Path::toFile)
                         .forEach(File::delete);
                }

                final Thread baseThread = Thread.currentThread();
                final List<Throwable> errors = new ArrayList<>();
                final ErrorHandler errorHandler = ex ->
                {
                    errors.add(ex);
                    baseThread.interrupt();
                };
                reaktor = builder.config(config)
                                 .errorHandler(errorHandler)
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
