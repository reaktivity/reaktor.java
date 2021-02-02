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

import static java.util.Collections.emptyList;

import java.util.List;
import java.util.stream.Collectors;

import javax.json.Json;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonValue;
import javax.json.bind.adapter.JsonbAdapter;

import org.reaktivity.reaktor.config.Binding;
import org.reaktivity.reaktor.config.Namespace;

public class NamespaceAdapter implements JsonbAdapter<Namespace, JsonObject>
{
    private static final String NAME_NAME = "name";
    private static final String BINDINGS_NAME = "bindings";

    private static final String NAME_DEFAULT = "default";
    private static final List<Binding> BINDINGS_DEFAULT = emptyList();

    private final BindingAdapter binding;

    public NamespaceAdapter()
    {
        binding = new BindingAdapter();
    }

    @Override
    public JsonObject adaptToJson(
        Namespace namespace) throws Exception
    {
        JsonObjectBuilder object = Json.createObjectBuilder();

        if (!NAME_DEFAULT.equals(namespace.name))
        {
            object.add(NAME_NAME, namespace.name);
        }

        if (!BINDINGS_DEFAULT.equals(namespace.bindings))
        {
            JsonArrayBuilder bindings = Json.createArrayBuilder();
            namespace.bindings.forEach(b -> bindings.add(binding.adaptToJson(b)));
            object.add(BINDINGS_NAME, bindings);
        }

        return object.build();
    }

    @Override
    public Namespace adaptFromJson(
        JsonObject object)
    {
        String name = object.getString(NAME_NAME, NAME_DEFAULT);
        List<Binding> bindings = object.containsKey(BINDINGS_NAME)
            ? object.getJsonArray(BINDINGS_NAME)
                .stream().map(JsonValue::asJsonObject)
                .map(binding::adaptFromJson)
                .collect(Collectors.toList())
            : BINDINGS_DEFAULT;

        return new Namespace(name, bindings);
    }
}
