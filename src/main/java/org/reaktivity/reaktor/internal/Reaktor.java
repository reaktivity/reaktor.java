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
package org.reaktivity.reaktor.internal;

import static java.lang.String.format;
import static java.util.Arrays.binarySearch;
import static java.util.Arrays.sort;
import static java.util.Collections.unmodifiableMap;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.agrona.IoUtil.tmpDirName;
import static org.agrona.LangUtil.rethrowUnchecked;
import static org.apache.commons.cli.Option.builder;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.function.Predicate;

import org.agrona.ErrorHandler;
import org.agrona.collections.ArrayUtil;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.SigIntBarrier;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.reaktivity.nukleus.Configuration;
import org.reaktivity.nukleus.Controller;
import org.reaktivity.nukleus.ControllerFactory;
import org.reaktivity.nukleus.Nukleus;
import org.reaktivity.nukleus.NukleusFactory;

public final class Reaktor implements AutoCloseable
{
    private final AgentRunner runner;
    private final Map<Class<? extends Controller>, Controller> controllersByKind;

    private Reaktor(
        Configuration config,
        Predicate<String> nukleusIncludes,
        Predicate<Class<? extends Controller>> controllerIncludes)
    {
        NukleusFactory nukleusFactory = NukleusFactory.instantiate();

        Nukleus[] nuklei = new Nukleus[0];

        for (String name : nukleusFactory.names())
        {
            if (nukleusIncludes.test(name))
            {
                Nukleus nukleus = nukleusFactory.create(name, config);
                nuklei = ArrayUtil.add(nuklei, nukleus);
            }
        }

        ControllerFactory controllerFactory = ControllerFactory.instantiate();

        Controller[] controllers = new Controller[0];
        Map<Class<? extends Controller>, Controller> controllersByKind = new HashMap<>();

        for (Class<? extends Controller> kind : controllerFactory.kinds())
        {
            if (controllerIncludes.test(kind))
            {
                Controller controller = controllerFactory.create(kind, config);
                controllersByKind.put(kind, controller);
                controllers = ArrayUtil.add(controllers, controller);
            }
        }

        this.controllersByKind = unmodifiableMap(controllersByKind);

        final Nukleus[] nuklei0 = nuklei;
        final Controller[] controllers0 = controllers;
        IdleStrategy idleStrategy = new BackoffIdleStrategy(64, 64, NANOSECONDS.toNanos(64L), MICROSECONDS.toNanos(64L));
        ErrorHandler errorHandler = throwable -> throwable.printStackTrace(System.err);
        Agent agent = new Agent()
        {
            @Override
            public String roleName()
            {
                return "reaktor";
            }

            @Override
            public int doWork() throws Exception
            {
                int work = 0;

                for (int i=0; i < nuklei0.length; i++)
                {
                    work += nuklei0[i].process();
                }

                for (int i=0; i < controllers0.length; i++)
                {
                    work += controllers0[i].process();
                }

                return work;
            }
        };

        this.runner = new AgentRunner(idleStrategy, errorHandler, null, agent);
    }

    public <T extends Controller> T controller(
        Class<T> kind)
    {
        return kind.cast(controllersByKind.get(kind));
    }

    public Reaktor start()
    {
        new Thread(runner).start();
        return this;
    }

    @Override
    public void close() throws Exception
    {
        try
        {
            runner.close();
        }
        catch (final Exception ex)
        {
            rethrowUnchecked(ex);
        }
    }

    public static Reaktor launch(
        Configuration config,
        Predicate<String> includes)
    {
        return launch(config, includes, k -> false);
    }

    public static Reaktor init(
        Configuration config,
        Predicate<String> nuklei,
        Predicate<Class<? extends Controller>> controllers)
    {
        return new Reaktor(config, nuklei, controllers);
    }

    public static Reaktor launch(
        Configuration config,
        Predicate<String> nuklei,
        Predicate<Class<? extends Controller>> controllers)
    {
        Reaktor reaktor = init(config, nuklei, controllers);
        reaktor.start();
        return reaktor;
    }

    public static void main(final String[] args) throws Exception
    {
        CommandLineParser parser = new DefaultParser();

        Options options = new Options();
        options.addOption(builder("d").longOpt("directory").hasArg().desc("configuration directory").build());
        options.addOption(builder("h").longOpt("help").desc("print this message").build());
        options.addOption(builder("n").longOpt("nukleus").hasArgs().desc("nukleus name").build());

        CommandLine cmdline = parser.parse(options, args);

        if (cmdline.hasOption("help"))
        {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("reaktor", options);
        }
        else
        {
            String directory = cmdline.getOptionValue("directory", format("%s/org.reaktivity.nukleus.reaktor", tmpDirName()));
            String[] nuklei = cmdline.getOptionValues("nukleus");

            Properties properties = new Properties();
            properties.setProperty(Configuration.DIRECTORY_PROPERTY_NAME, directory);

            Configuration config = new Configuration(properties);

            Predicate<String> includes = name -> true;
            if (nuklei != null)
            {
                Comparator<String> comparator = (o1, o2) -> o1.compareTo(o2);
                sort(nuklei, comparator);
                includes = name -> binarySearch(nuklei, name, comparator) >= 0;
            }

            try (Reaktor reaktor = Reaktor.launch(config, includes))
            {
                System.out.println("Started in " + directory);

                new SigIntBarrier().await();
            }
        }
    }
}
