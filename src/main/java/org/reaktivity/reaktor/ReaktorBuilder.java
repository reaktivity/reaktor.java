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

import org.agrona.ErrorHandler;
import org.reaktivity.reaktor.internal.agent.NukleusAgent;
import org.reaktivity.reaktor.nukleus.Configuration;

public class ReaktorBuilder
{
    private Configuration config;
    private ErrorHandler errorHandler;

    private int threads = 1;

    ReaktorBuilder()
    {
    }

    public ReaktorBuilder config(
        Configuration config)
    {
        this.config = requireNonNull(config);
        return this;
    }

    public ReaktorBuilder threads(
        int threads)
    {
        this.threads = threads;
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
        final ReaktorConfiguration config = new ReaktorConfiguration(this.config != null ? this.config : new Configuration());
        final NukleusAgent nukleusAgent = new NukleusAgent(config, threads);
        final ErrorHandler errorHandler = requireNonNull(this.errorHandler, "errorHandler");

        return new Reaktor(config, errorHandler, nukleusAgent);
    }
}
