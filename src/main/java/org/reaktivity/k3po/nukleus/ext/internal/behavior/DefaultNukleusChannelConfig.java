/**
 * Copyright 2016-2017 The Reaktivity Project
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
package org.reaktivity.k3po.nukleus.ext.internal.behavior;

import java.util.Objects;

import org.jboss.netty.channel.DefaultChannelConfig;

public class DefaultNukleusChannelConfig extends DefaultChannelConfig implements NukleusChannelConfig
{
    private long correlation;
    private String readPartition;
    private String writePartition;
    private boolean duplex = false;
    private int window;
    private boolean throttle = true;

    @Override
    public void setCorrelation(
        long correlation)
    {
        this.correlation = correlation;
    }

    @Override
    public long getCorrelation()
    {
        return correlation;
    }

    @Override
    public void setReadPartition(
        String partition)
    {
        this.readPartition = partition;
    }

    @Override
    public String getReadPartition()
    {
        return readPartition;
    }

    @Override
    public void setWritePartition(
        String writePartition)
    {
        this.writePartition = writePartition;
    }

    @Override
    public String getWritePartition()
    {
        return writePartition;
    }

    public void setDuplex(
        boolean duplex)
    {
        this.duplex = duplex;
    }

    public boolean isDuplex()
    {
        return duplex;
    }

    @Override
    public void setWindow(int window)
    {
        this.window = window;
    }

    @Override
    public int getWindow()
    {
        return window;
    }

    @Override
    public void setThrottle(
        boolean throttle)
    {
        this.throttle = throttle;
    }

    @Override
    public boolean hasThrottle()
    {
        return throttle;
    }

    @Override
    public boolean setOption(
        String key,
        Object value)
    {
        if (super.setOption(key, value))
        {
            return true;
        }

        if ("correlation".equals(key))
        {
            setCorrelation(convertToLong(value));
        }
        else if ("readPartition".equals(key))
        {
            setReadPartition(Objects.toString(value, null));
        }
        else if ("writePartition".equals(key))
        {
            setWritePartition(Objects.toString(value, null));
        }
        else if ("transmission".equals(key))
        {
            setDuplex("duplex".equals(value));
        }
        else if ("window".equals(key))
        {
            setWindow(convertToInt(value));
        }
        else if ("throttle".equals(key))
        {
            setThrottle("on".equals(value));
        }
        else
        {
            return false;
        }

        return true;
    }

    private static long convertToLong(Object value)
    {
        if (value instanceof Number)
        {
            return ((Number) value).longValue();
        }
        else
        {
            return Long.parseLong(String.valueOf(value));
        }
    }

    private static int convertToInt(Object value)
    {
        if (value instanceof Number)
        {
            return ((Number) value).intValue();
        }
        else
        {
            return Integer.parseInt(String.valueOf(value));
        }
    }
}
