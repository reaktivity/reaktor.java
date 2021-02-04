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

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public final class ReaktorThreadFactory implements ThreadFactory
{
    private final AtomicInteger nextThreadId = new AtomicInteger();
    private final ThreadFactory factory = Executors.defaultThreadFactory();

    private final String nameFormat;

    public ReaktorThreadFactory(
        String kind)
    {
        this.nameFormat = "reaktor/" + kind + "#%d";
    }

    @Override
    public Thread newThread(
        Runnable r)
    {
        Thread t = factory.newThread(r);

        if (t != null)
        {
            t.setName(String.format(nameFormat, nextThreadId.getAndIncrement()));
        }

        return t;
    }
}
