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
package org.reaktivity.reaktor.test.internal.k3po.ext.behavior;

import org.jboss.netty.channel.ChannelFuture;

public final class NukleusCorrelation
{
    private final NukleusChannel channel;
    private final ChannelFuture correlatedFuture;

    NukleusCorrelation(
        NukleusChannel channel,
        ChannelFuture correlatedFuture)
    {
        this.channel = channel;
        this.correlatedFuture = correlatedFuture;
    }

    public NukleusChannel channel()
    {
        return channel;
    }

    public ChannelFuture correlatedFuture()
    {
        return correlatedFuture;
    }

    @Override
    public String toString()
    {
        return String.format("%s [channel=%s]", getClass().getSimpleName(), channel);
    }
}
