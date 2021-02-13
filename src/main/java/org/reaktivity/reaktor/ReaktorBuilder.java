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

import static java.util.Objects.requireNonNull;

import java.net.URI;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Set;

import org.agrona.ErrorHandler;
import org.reaktivity.reaktor.nukleus.Configuration;
import org.reaktivity.reaktor.nukleus.Nukleus;
import org.reaktivity.reaktor.nukleus.NukleusFactory;

public class ReaktorBuilder
{
    private Configuration config;
    private ErrorHandler errorHandler;

    private int threads = 1;
    private URI configURI;
    private Collection<ReaktorAffinity> affinities;

    ReaktorBuilder()
    {
        this.affinities = new LinkedHashSet<>();
    }

    public ReaktorBuilder config(
        Configuration config)
    {
        this.config = requireNonNull(config);
        return this;
    }

    public ReaktorBuilder configURI(
        URI configURI)
    {
        this.configURI = configURI;
        return this;
    }

    public ReaktorBuilder threads(
        int threads)
    {
        this.threads = threads;
        return this;
    }

    public void affinity(
        String namespace,
        String binding,
        long mask)
    {
        affinities.add(new ReaktorAffinity(namespace, binding, mask));
    }

    public ReaktorBuilder errorHandler(
        ErrorHandler errorHandler)
    {
        this.errorHandler = requireNonNull(errorHandler);
        return this;
    }

    public Reaktor build()
    {
        final ReaktorConfiguration config = new ReaktorConfiguration(this.config != null ? this.config : new Configuration());

        final Set<Nukleus> nuklei = new LinkedHashSet<>();
        final NukleusFactory factory = NukleusFactory.instantiate();
        for (String name : factory.names())
        {
            Nukleus nukleus = factory.create(name, config);
            nuklei.add(nukleus);
        }

        final ErrorHandler errorHandler = requireNonNull(this.errorHandler, "errorHandler");

        return new Reaktor(config, nuklei, errorHandler, configURI, threads, affinities);
    }
}
