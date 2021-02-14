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

import java.net.URI;
import java.util.LinkedList;
import java.util.List;

import org.junit.Test;
import org.reaktivity.reaktor.Reaktor;

public class ReaktorTest
{
    @Test
    public void shouldConfigureEmpty() throws Exception
    {
        List<Throwable> errors = new LinkedList<>();
        try (Reaktor reaktor = Reaktor.builder()
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
        URI configURI = getClass().getResource(resource).toURI();
        List<Throwable> errors = new LinkedList<>();
        try (Reaktor reaktor = Reaktor.builder()
                .configURI(configURI)
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
    public void shouldNotConfigureUnknownScheme() throws Exception
    {
        List<Throwable> errors = new LinkedList<>();
        try (Reaktor reaktor = Reaktor.builder()
                .configURI(URI.create("unknown://path"))
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
}