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

import static org.reaktivity.k3po.nukleus.ext.internal.behavior.NukleusThrottleMode.NONE;
import static org.reaktivity.k3po.nukleus.ext.internal.behavior.NukleusThrottleMode.STREAM;
import static org.reaktivity.k3po.nukleus.ext.internal.behavior.NukleusTransmission.SIMPLEX;

import java.util.Objects;

import org.kaazing.k3po.driver.internal.netty.bootstrap.channel.DefaultServerChannelConfig;

public class DefaultNukleusServerChannelConfig extends DefaultServerChannelConfig implements NukleusServerChannelConfig
{
    private long correlation;
    private String readPartition;
    private String writePartition;
    private NukleusTransmission transmission = SIMPLEX;
    private int window;
    private NukleusThrottleMode throttle = STREAM;
    private boolean update = true;

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

    @Override
    public void setTransmission(
        NukleusTransmission transmission)
    {
        this.transmission = transmission;
    }

    @Override
    public NukleusTransmission getTransmission()
    {
        return transmission;
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
    public void setUpdate(boolean update)
    {
        this.update = update;
    }

    @Override
    public boolean getUpdate()
    {
        return update;
    }

    @Override
    public void setThrottle(
        NukleusThrottleMode throttle)
    {
        this.throttle = throttle;
    }

    @Override
    public boolean hasThrottle()
    {
        return throttle != NONE;
    }

    @Override
    public NukleusThrottleMode getThrottle()
    {
        return throttle;
    }

    @Override
    protected boolean setOption0(
        String key,
        Object value)
    {
        if (super.setOption0(key, value))
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
            setTransmission(NukleusTransmission.decode(Objects.toString(value, "simplex")));
        }
        else if ("window".equals(key))
        {
            setWindow(convertToInt(value));
        }
        else if ("update".equals(key))
        {
            setUpdate(!"none".equals(value));
        }
        else if ("throttle".equals(key))
        {
            setThrottle(NukleusThrottleMode.decode(Objects.toString(value, "stream")));
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
