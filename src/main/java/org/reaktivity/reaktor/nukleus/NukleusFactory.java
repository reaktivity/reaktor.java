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
package org.reaktivity.reaktor.nukleus;

import static java.util.Collections.unmodifiableMap;
import static java.util.Objects.requireNonNull;
import static java.util.ServiceLoader.load;

import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;

public final class NukleusFactory
{
    private final Map<String, NukleusFactorySpi> factorySpis;

    public static NukleusFactory instantiate()
    {
        return instantiate(load(NukleusFactorySpi.class));
    }

    public Iterable<String> names()
    {
        return factorySpis.keySet();
    }

    public Nukleus create(
        String name,
        Configuration config)
    {
        requireNonNull(name, "name");

        NukleusFactorySpi factorySpi = requireNonNull(factorySpis.get(name), () -> "Unregonized nukleus name: " + name);

        return factorySpi.create(config);
    }

    private static NukleusFactory instantiate(
        ServiceLoader<NukleusFactorySpi> factories)
    {
        Map<String, NukleusFactorySpi> factorySpisByName = new HashMap<>();
        factories.forEach(factorySpi -> factorySpisByName.put(factorySpi.name(), factorySpi));

        return new NukleusFactory(unmodifiableMap(factorySpisByName));
    }

    private NukleusFactory(
        Map<String, NukleusFactorySpi> factorySpis)
    {
        this.factorySpis = factorySpis;
    }
}
