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

import org.kaazing.k3po.driver.internal.netty.bootstrap.channel.ChannelConfig;

public interface NukleusChannelConfig extends ChannelConfig
{
    void setCorrelation(long correlation);

    long getCorrelation();

    void setReadPartition(String readPartition);

    String getReadPartition();

    void setWritePartition(String writePartition);

    String getWritePartition();

    void setTransmission(NukleusTransmission transmission);

    NukleusTransmission getTransmission();

    void setWindow(int window);

    int getWindow();

    void setPadding(int padding);

    int getPadding();

    void setUpdate(boolean update);

    boolean getUpdate();

    void setThrottle(NukleusThrottleMode throttle);

    NukleusThrottleMode getThrottle();

    boolean hasThrottle();
}
