/**
 * Copyright 2016-2018 The Reaktivity Project
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
package org.reaktivity.reaktor.internal.router;

import static java.lang.String.format;

import org.agrona.concurrent.status.AtomicCounter;
import org.reaktivity.reaktor.internal.Counters;

public final class WriteCounters
{
    final AtomicCounter opens;
    final AtomicCounter closes;
    final AtomicCounter aborts;
    final AtomicCounter windows;
    final AtomicCounter resets;
    final AtomicCounter bytes;
    final AtomicCounter frames;

    public WriteCounters(
        Counters counters,
        String nukleus,
        long routeId)
    {
        this.opens = counters.counter(format("%s.%d.opens.written", nukleus, routeId));
        this.closes = counters.counter(format("%s.%d.closes.written", nukleus, routeId));
        this.aborts = counters.counter(format("%s.%d.aborts.written", nukleus, routeId));
        this.windows = counters.counter(format("%s.%d.windows.written", nukleus, routeId));
        this.resets = counters.counter(format("%s.%d.resets.written", nukleus, routeId));
        this.bytes = counters.counter(format("%s.%d.bytes.written", nukleus, routeId));
        this.frames = counters.counter(format("%s.%d.frames.written", nukleus, routeId));
    }
}