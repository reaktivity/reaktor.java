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
package org.reaktivity.reaktor.internal.router;

import java.util.Objects;
import java.util.function.Predicate;

import org.reaktivity.reaktor.internal.types.control.RouteFW;

public final class RouteMatchers
{
    public static Predicate<RouteFW> sourceMatches(
        String source)
    {
        Objects.requireNonNull(source);
        return r -> source.equals(r.source().asString());
    }

    public static Predicate<RouteFW> sourceRefMatches(
        long sourceRef)
    {
        return r -> sourceRef == r.sourceRef();
    }

    public static Predicate<RouteFW> targetMatches(
        String target)
    {
        Objects.requireNonNull(target);
        return r -> target.equals(r.target().asString());
    }

    public static Predicate<RouteFW> targetRefMatches(
        long targetRef)
    {
        return r -> targetRef == r.targetRef();
    }

    public static Predicate<RouteFW> authorizationMatches(
        long authorization)
    {
        return r -> authorization == r.authorization();
    }

    private RouteMatchers()
    {
        // no instances
    }
}
