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
package org.reaktivity.reaktor.config;

import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;

import java.util.List;

public class Route
{
    private static final List<Condition> UNCONDITIONAL = emptyList();

    public transient long id;

    public final String exit;
    public final List<Condition> when;

    public Route(
        String exit)
    {
        this(exit, UNCONDITIONAL);
    }

    public Route(
        String exit,
        List<Condition> when)
    {
        this.exit = requireNonNull(exit);
        this.when = requireNonNull(when);
    }
}
