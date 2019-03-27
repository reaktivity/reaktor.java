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

import static java.lang.String.format;
import static java.util.Arrays.binarySearch;
import static java.util.Arrays.sort;
import static org.agrona.IoUtil.tmpDirName;

import java.util.Comparator;
import java.util.Properties;
import java.util.function.Predicate;

import org.agrona.concurrent.SigIntBarrier;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.reaktivity.nukleus.Configuration;
import org.reaktivity.reaktor.internal.ReaktorConfiguration;

public final class ReaktorMain
{
    public static void main(final String[] args) throws Exception
    {
        CommandLineParser parser = new DefaultParser();

        Options options = new Options();
        options.addOption(Option.builder("d").longOpt("directory").hasArg().desc("configuration directory").build());
        options.addOption(Option.builder("h").longOpt("help").desc("print this message").build());
        options.addOption(Option.builder("n").longOpt("nukleus").hasArgs().desc("nukleus name").build());

        CommandLine cmdline = parser.parse(options, args);

        if (cmdline.hasOption("help"))
        {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("reaktor", options);
        }
        else
        {
            String directory = cmdline.getOptionValue("directory", format("%s/org.reaktivity.reaktor", tmpDirName()));
            String[] nuklei = cmdline.getOptionValues("nukleus");

            Properties properties = new Properties();
            properties.setProperty(ReaktorConfiguration.REAKTOR_DIRECTORY.name(), directory);

            Configuration config = new Configuration(properties);

            Predicate<String> includes = name -> true;
            if (nuklei != null)
            {
                Comparator<String> comparator = (o1, o2) -> o1.compareTo(o2);
                sort(nuklei, comparator);
                includes = name -> binarySearch(nuklei, name, comparator) >= 0;
            }

            try (Reaktor reaktor = Reaktor.builder()
                    .config(config)
                    .nukleus(includes)
                    .errorHandler(ex -> ex.printStackTrace(System.err))
                    .build()
                    .start())
            {
                System.out.println("Started in " + directory);

                new SigIntBarrier().await();
            }
        }
    }
}
