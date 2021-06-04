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
package org.reaktivity.reaktor.internal.stream;

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
        String namespace,
        String binding)
    {
        this.opens = counters.counter(format("%s.%s.opens.written", namespace, binding));
        this.closes = counters.counter(format("%s.%s.closes.written", namespace, binding));
        this.aborts = counters.counter(format("%s.%s.aborts.written", namespace, binding));
        this.windows = counters.counter(format("%s.%s.windows.written", namespace, binding));
        this.resets = counters.counter(format("%s.%s.resets.written", namespace, binding));
        this.bytes = counters.counter(format("%s.%s.bytes.written", namespace, binding));
        this.frames = counters.counter(format("%s.%s.frames.written", namespace, binding));
    }
}
