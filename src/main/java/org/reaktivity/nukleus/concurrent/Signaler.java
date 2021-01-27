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
package org.reaktivity.nukleus.concurrent;

import java.util.function.IntConsumer;

public interface Signaler
{
    long NO_CANCEL_ID = 0xffff_ffff_ffff_ffffL;

    long signalAt(long timeMillis, int signalId, IntConsumer handler);

    void signalNow(long routeId, long streamId, int signalId);

    long signalAt(long timeMillis, long routeId, long streamId, int signalId);

    long signalTask(Runnable task, long routeId, long streamId, int signalId);

    boolean cancel(long cancelId);
}
