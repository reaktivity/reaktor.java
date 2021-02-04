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
package org.reaktivity.reaktor.internal.context;

import java.util.Set;

import org.agrona.concurrent.Agent;

public class ConfigureAgent implements Agent
{
    private final Set<DispatchAgent> dispatchers;

    public ConfigureAgent(
        Set<DispatchAgent> dispatchers)
    {
        this.dispatchers = dispatchers;
    }

    @Override
    public String roleName()
    {
        return "reaktor/config";
    }

    @Override
    public int doWork() throws Exception
    {
        // TODO: check config
        return 0;
    }

    @Override
    public void onClose()
    {
    }
}
