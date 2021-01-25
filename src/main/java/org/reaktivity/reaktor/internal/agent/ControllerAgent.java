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
package org.reaktivity.reaktor.internal.agent;

import static org.agrona.LangUtil.rethrowUnchecked;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.agrona.CloseHelper;
import org.agrona.collections.ArrayUtil;
import org.agrona.concurrent.Agent;
import org.reaktivity.nukleus.Controller;

public class ControllerAgent implements Agent
{
    private final Map<Class<? extends Controller>, Controller> controllersByKind;

    private volatile Controller[] controllers;

    public ControllerAgent()
    {
        this.controllersByKind = new HashMap<>();
        this.controllers = new Controller[0];
    }

    @Override
    public String roleName()
    {
        return "reaktor/controller";
    }

    public int doWork() throws Exception
    {
        int workDone = 0;

        for (Controller controller : controllers)
        {
            workDone += controller.process();
        }

        return workDone;
    }

    public void onClose()
    {
        List<Throwable> errors = new LinkedList<>();
        for (Controller controller : controllers)
        {
            try
            {
                CloseHelper.close(controller);
            }
            catch (Throwable ex)
            {
                errors.add(ex);
            }
        }

        if (!errors.isEmpty())
        {
            final Throwable ex = errors.remove(0);
            errors.forEach(ex::addSuppressed);
            rethrowUnchecked(ex);
        }
    }

    public void assign(
        Controller controller)
    {
        controllersByKind.put(controller.kind(), controller);
        controllers = ArrayUtil.add(controllers, controller);
    }

    public <T extends Controller> T controller(
        Class<T> kind)
    {
        return kind.cast(controllersByKind.get(kind));
    }

    public Stream<Controller> controllers()
    {
        return controllersByKind.values().stream();
    }

    public boolean isEmpty()
    {
        return controllersByKind.isEmpty();
    }
}
