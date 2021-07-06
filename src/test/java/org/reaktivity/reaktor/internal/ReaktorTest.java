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
package org.reaktivity.reaktor.internal;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.reaktivity.reaktor.ReaktorConfiguration.REAKTOR_DIRECTORY;

import java.net.URI;
import java.net.URL;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import org.junit.Before;
import org.junit.Test;
import org.reaktivity.reaktor.Reaktor;
import org.reaktivity.reaktor.ReaktorConfiguration;
import org.reaktivity.reaktor.ReaktorLoad;
import org.reaktivity.reaktor.ext.ReaktorExtContext;
import org.reaktivity.reaktor.ext.ReaktorExtSpi;

public class ReaktorTest
{
    private ReaktorConfiguration config;

    @Before
    public void initConfig()
    {
        Properties properties = new Properties();
        properties.put(REAKTOR_DIRECTORY.name(), "target/nukleus-itests");
        config = new ReaktorConfiguration(properties);
    }

    @Test
    public void shouldConfigureEmpty() throws Exception
    {
        List<Throwable> errors = new LinkedList<>();
        try (Reaktor reaktor = Reaktor.builder()
                .config(config)
                .errorHandler(errors::add)
                .build())
        {
            reaktor.start().get();
        }
        catch (Throwable ex)
        {
            errors.add(ex);
        }
        finally
        {
            assertThat(errors, empty());
        }
    }

    @Test
    public void shouldConfigure() throws Exception
    {
        String resource = String.format("%s-%s.json", getClass().getSimpleName(), "configure");
        URL configURL = getClass().getResource(resource);
        List<Throwable> errors = new LinkedList<>();
        try (Reaktor reaktor = Reaktor.builder()
                .config(config)
                .configURL(configURL)
                .errorHandler(errors::add)
                .build())
        {
            reaktor.start().get();

            ReaktorLoad load = reaktor.load("default", "test0");
            assertEquals(0L, load.initialOpens());
            assertEquals(0L, load.initialCloses());
            assertEquals(0L, load.initialErrors());
            assertEquals(0L, load.initialBytes());
            assertEquals(0L, load.replyOpens());
            assertEquals(0L, load.replyCloses());
            assertEquals(0L, load.replyErrors());
            assertEquals(0L, load.replyBytes());
        }
        catch (Throwable ex)
        {
            errors.add(ex);
        }
        finally
        {
            assertThat(errors, empty());
        }
    }

    @Test
    public void shouldNotConfigureUnknownScheme() throws Exception
    {
        List<Throwable> errors = new LinkedList<>();
        try (Reaktor reaktor = Reaktor.builder()
                .config(config)
                .configURL(URI.create("unknown://path").toURL())
                .errorHandler(errors::add)
                .build())
        {
            reaktor.start().get();
        }
        catch (Throwable ex)
        {
            errors.add(ex);
        }
        finally
        {
            assertThat(errors, not(empty()));
        }
    }

    public static final class TestReaktorExt implements ReaktorExtSpi
    {
        @Override
        public void onConfigured(
            ReaktorExtContext context)
        {
        }

        @Override
        public void onClosed(
            ReaktorExtContext context)
        {
        }
    }
}
