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

public final class StreamId
{
    public static int replyToIndex(
        long streamId)
    {
        return isInitial(streamId) ? localIndex(streamId) : remoteIndex(streamId);
    }

    public static int localIndex(
        long streamId)
    {
        return (int)(streamId >> 56) & 0x7f;
    }

    public static int remoteIndex(
        long streamId)
    {
        return (int)(streamId >> 48) & 0x7f;
    }

    public static boolean isInitial(
        long streamId)
    {
        return (streamId & 0x8000_0000_0000_0000L) == 0L;
    }

    private StreamId()
    {
    }
}
