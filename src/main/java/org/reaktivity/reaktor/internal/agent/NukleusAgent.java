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
package org.reaktivity.reaktor.internal.agent;

import org.agrona.CloseHelper;
import org.agrona.concurrent.Agent;
import org.reaktivity.nukleus.Nukleus;
import org.reaktivity.reaktor.internal.Context;
import org.reaktivity.reaktor.internal.ReaktorConfiguration;
import org.reaktivity.reaktor.internal.State;
import org.reaktivity.reaktor.internal.conductor.Conductor;
import org.reaktivity.reaktor.internal.router.Router;

public class NukleusAgent implements Agent
{
    private final Nukleus nukleus;
    private final Router router;
    private final Conductor conductor;
    private final Context context;
    private Agent[] agents;

    public NukleusAgent(
        Nukleus nukleus,
        State state)
    {
        this.nukleus = nukleus;

        ReaktorConfiguration reaktorConfig = new ReaktorConfiguration(nukleus.config());
        Context context = new Context();
        context.name(nukleus.name()).executor(nukleus.executor()).conclude(reaktorConfig);

        final boolean timestamps = reaktorConfig.timestamps();

        Conductor conductor = new Conductor(context);
        Router router = new Router(context, state, nukleus::supplyElektron);

        conductor.setRouter(router);
        conductor.setCommandHandlerSupplier(nukleus::commandHandler);
        router.setConductor(conductor);
        router.setTimestamps(timestamps);
        router.setRouteHandlerSupplier(nukleus::routeHandler);

        conductor.freezeHandler(this::freeze);

        this.agents = new Agent[] { conductor, router };
        this.conductor = conductor;
        this.router = router;
        this.context = context;
    }

    @Override
    public String roleName()
    {
        return nukleus.name();
    }

    public int doWork() throws Exception
    {
        int workDone = 0;

        for (final Agent agent : agents)
        {
            workDone += agent.doWork();
        }

        return workDone;
    }

    public void onClose()
    {
        for (final Agent agent : agents)
        {
            agent.onClose();
        }

        CloseHelper.quietClose(context::close);
    }

    public void freeze()
    {
        conductor.onClose();
        this.agents = new Agent[] { router };
    }

    public long counter(
        String name)
    {
        return context.counters().readonlyCounter(name).getAsLong();
    }
}
