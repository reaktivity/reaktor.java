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

import static org.reaktivity.reaktor.test.internal.k3po.ext.behavior.NukleusChannel.NATIVE_BUFFER_FACTORY;
import static org.reaktivity.reaktor.test.internal.k3po.ext.behavior.NukleusThrottleMode.NONE;
import static org.reaktivity.reaktor.test.internal.k3po.ext.behavior.NukleusTransmission.SIMPLEX;
import static org.reaktivity.reaktor.test.internal.k3po.ext.types.NukleusTypeSystem.OPTION_BUDGET_ID;
import static org.reaktivity.reaktor.test.internal.k3po.ext.types.NukleusTypeSystem.OPTION_BYTE_ORDER;
import static org.reaktivity.reaktor.test.internal.k3po.ext.types.NukleusTypeSystem.OPTION_CAPABILITIES;
import static org.reaktivity.reaktor.test.internal.k3po.ext.types.NukleusTypeSystem.OPTION_PADDING;
import static org.reaktivity.reaktor.test.internal.k3po.ext.types.NukleusTypeSystem.OPTION_SHARED_WINDOW;
import static org.reaktivity.reaktor.test.internal.k3po.ext.types.NukleusTypeSystem.OPTION_THROTTLE;
import static org.reaktivity.reaktor.test.internal.k3po.ext.types.NukleusTypeSystem.OPTION_TRANSMISSION;
import static org.reaktivity.reaktor.test.internal.k3po.ext.types.NukleusTypeSystem.OPTION_UPDATE;
import static org.reaktivity.reaktor.test.internal.k3po.ext.types.NukleusTypeSystem.OPTION_WINDOW;
import static org.reaktivity.reaktor.test.internal.k3po.ext.util.Conversions.convertToByte;
import static org.reaktivity.reaktor.test.internal.k3po.ext.util.Conversions.convertToInt;
import static org.reaktivity.reaktor.test.internal.k3po.ext.util.Conversions.convertToLong;

import java.util.Objects;

import org.kaazing.k3po.driver.internal.netty.bootstrap.channel.DefaultServerChannelConfig;

public class DefaultNukleusServerChannelConfig extends DefaultServerChannelConfig implements NukleusServerChannelConfig
{
    private NukleusTransmission transmission = SIMPLEX;
    private int window;
    private int sharedWindow;
    private long budgetId;
    private int padding;
    private NukleusThrottleMode throttle = NukleusThrottleMode.STREAM;
    private NukleusUpdateMode update = NukleusUpdateMode.STREAM;
    private long affinity;
    private byte capabilities;

    public DefaultNukleusServerChannelConfig()
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
    public void setSharedWindow(int sharedWindow)
    {
        this.sharedWindow = sharedWindow;
    }

    @Override
    public int getSharedWindow()
    {
        return sharedWindow;
    }

    @Override
    public void setBudgetId(
        long budgetId)
    {
        this.budgetId = budgetId;
    }

    @Override
    public long getBudgetId()
    {
        return budgetId;
    }

    @Override
    public void setPadding(
        int padding)
    {
        this.padding = padding;
    }

    @Override
    public int getPadding()
    {
        return padding;
    }

    @Override
    public void setUpdate(
        NukleusUpdateMode update)
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
    public void setCapabilities(
        byte capabilities)
    {
        this.capabilities = capabilities;
    }

    @Override
    public byte getCapabilities()
    {
        return capabilities;
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

        if (OPTION_TRANSMISSION.getName().equals(key))
        {
            setTransmission(NukleusTransmission.decode(Objects.toString(value, "simplex")));
        }
        else if (OPTION_WINDOW.getName().equals(key))
        {
            setWindow(convertToInt(value));
        }
        else if (OPTION_SHARED_WINDOW.getName().equals(key))
        {
            setSharedWindow(convertToInt(value));
        }
        else if (OPTION_BUDGET_ID.getName().equals(key))
        {
            setBudgetId(convertToLong(value));
        }
        else if (OPTION_PADDING.getName().equals(key))
        {
            setPadding(convertToInt(value));
        }
        else if (OPTION_UPDATE.getName().equals(key))
        {
            setUpdate(NukleusUpdateMode.decode(Objects.toString(value, null)));
        }
        else if (OPTION_THROTTLE.getName().equals(key))
        {
            setThrottle(NukleusThrottleMode.decode(Objects.toString(value, "stream")));
        }
        else if (OPTION_BYTE_ORDER.getName().equals(key))
        {
            setBufferFactory(NukleusByteOrder.decode(Objects.toString(value, "native")).toBufferFactory());
        }
        else if (OPTION_CAPABILITIES.getName().equals(key))
        {
            setCapabilities(convertToByte(value));
        }
        else
        {
            return false;
        }

        return true;
    }

}
