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
package org.reaktivity.reaktor.internal.config;

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

import java.util.Map;
import java.util.ServiceLoader;
import java.util.function.Supplier;

import javax.json.JsonObject;
import javax.json.bind.adapter.JsonbAdapter;

import org.reaktivity.reaktor.config.Options;
import org.reaktivity.reaktor.config.OptionsAdapterSpi;

public class OptionsAdapter implements JsonbAdapter<Options, JsonObject>
{
    private final Map<String, OptionsAdapterSpi> delegatesByName;

    private OptionsAdapterSpi delegate;

    public OptionsAdapter(
        OptionsAdapterSpi.Kind kind)
    {
        delegatesByName = ServiceLoader
            .load(OptionsAdapterSpi.class)
            .stream()
            .map(Supplier::get)
            .filter(s -> s.kind() == kind)
            .collect(toMap(OptionsAdapterSpi::type, identity()));
    }

    public void adaptType(
        String type)
    {
        delegate = delegatesByName.get(type);
    }

    @Override
    public JsonObject adaptToJson(
        Options options)
    {
        return delegate != null ? delegate.adaptToJson(options) : null;
    }

    @Override
    public Options adaptFromJson(
        JsonObject object)
    {
        return delegate != null ? delegate.adaptFromJson(object) : null;
    }
}
