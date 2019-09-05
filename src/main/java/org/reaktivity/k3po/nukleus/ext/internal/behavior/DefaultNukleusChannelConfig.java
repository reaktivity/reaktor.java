/**
 * Copyright 2016-2019 The Reaktivity Project
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

import static org.reaktivity.k3po.nukleus.ext.internal.behavior.NukleusChannel.NATIVE_BUFFER_FACTORY;
import static org.reaktivity.k3po.nukleus.ext.internal.behavior.NukleusThrottleMode.NONE;
import static org.reaktivity.k3po.nukleus.ext.internal.behavior.NukleusTransmission.SIMPLEX;
import static org.reaktivity.k3po.nukleus.ext.internal.types.NukleusTypeSystem.OPTION_AFFINITY;
import static org.reaktivity.k3po.nukleus.ext.internal.types.NukleusTypeSystem.OPTION_BYTE_ORDER;
import static org.reaktivity.k3po.nukleus.ext.internal.util.Conversions.convertToInt;
import static org.reaktivity.k3po.nukleus.ext.internal.util.Conversions.convertToLong;

import java.util.Objects;

import org.kaazing.k3po.driver.internal.netty.bootstrap.channel.DefaultChannelConfig;

public class DefaultNukleusChannelConfig extends DefaultChannelConfig implements NukleusChannelConfig
{
    private NukleusTransmission transmission = SIMPLEX;
    private int window;
    private long group;
    private int padding;
    private NukleusThrottleMode throttle = NukleusThrottleMode.STREAM;
    private NukleusUpdateMode update = NukleusUpdateMode.STREAM;
    private long affinity;

    public DefaultNukleusChannelConfig()
    {
        super();
        setBufferFactory(NATIVE_BUFFER_FACTORY);
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
    public void setGroup(
        long group)
    {
        this.group = group;
    }

    @Override
    public long getGroup()
    {
        return group;
    }

    @Override
    public void setPadding(int padding)
    {
        this.padding = padding;
    }

    @Override
    public int getPadding()
    {
        return padding;
    }

    @Override
    public void setUpdate(NukleusUpdateMode update)
    {
        this.update = update;
    }

    @Override
    public NukleusUpdateMode getUpdate()
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
    public NukleusThrottleMode getThrottle()
    {
        return throttle;
    }

    @Override
    public boolean hasThrottle()
    {
        return throttle != NONE;
    }

    @Override
    public void setAffinity(
        long affinity)
    {
        this.affinity = affinity;
    }

    @Override
    public long getAffinity()
    {
        return affinity;
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
        else if ("transmission".equals(key))
        {
            setTransmission(NukleusTransmission.decode(Objects.toString(value, null)));
        }
        else if ("window".equals(key))
        {
            setWindow(convertToInt(value));
        }
        else if ("group".equals(key))
        {
            setGroup(convertToLong(value));
        }
        else if ("padding".equals(key))
        {
            setPadding(convertToInt(value));
        }
        else if ("update".equals(key))
        {
            setUpdate(NukleusUpdateMode.decode(Objects.toString(value, null)));
        }
        else if ("throttle".equals(key))
        {
            setThrottle(NukleusThrottleMode.decode(Objects.toString(value, null)));
        }
        else if (OPTION_BYTE_ORDER.getName().equals(key))
        {
            setBufferFactory(NukleusByteOrder.decode(Objects.toString(value, "native")).toBufferFactory());
        }
        else if (OPTION_AFFINITY.getName().equals(key))
        {
            setAffinity(convertToLong(value));
        }
        else
        {
            return false;
        }

        return true;
    }
}
