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
import static java.util.stream.Collectors.toList;

import java.util.List;

import javax.json.Json;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonValue;
import javax.json.bind.adapter.JsonbAdapter;

import org.reaktivity.reaktor.config.Binding;
import org.reaktivity.reaktor.config.Options;
import org.reaktivity.reaktor.config.Role;
import org.reaktivity.reaktor.config.Route;

public class BindingAdapter implements JsonbAdapter<Binding, JsonObject>
{
    private static final String ENTRY_NAME = "entry";
    private static final String TYPE_NAME = "type";
    private static final String KIND_NAME = "kind";
    private static final String OPTIONS_NAME = "options";
    private static final String ROUTES_NAME = "routes";

    private static final List<Route> ROUTES_DEFAULT = emptyList();

    private final RoleAdapter role;
    private final RouteAdapter route;
    private final OptionsAdapter options;

    public BindingAdapter()
    {
        role = new RoleAdapter();
        route = new RouteAdapter();
        options = new OptionsAdapter();
    }

    @Override
    public JsonObject adaptToJson(
        Binding binding)
    {
        route.adaptType(binding.type);
        options.adaptType(binding.type);

        JsonObjectBuilder object = Json.createObjectBuilder();

        if (binding.entry != null)
        {
            object.add(ENTRY_NAME, binding.entry);
        }

        object.add(TYPE_NAME, binding.type);

        object.add(KIND_NAME, role.adaptToJson(binding.kind));

        if (binding.options != null)
        {
            object.add(OPTIONS_NAME, options.adaptToJson(binding.options));
        }

        if (!ROUTES_DEFAULT.equals(binding.routes))
        {
            JsonArrayBuilder routes = Json.createArrayBuilder();
            binding.routes.forEach(r -> routes.add(route.adaptToJson(r)));
            object.add(ROUTES_NAME, routes);
        }

        return object.build();
    }

    @Override
    public Binding adaptFromJson(
        JsonObject object)
    {
        String entry = object.getString(ENTRY_NAME, null);
        String type = object.getString(TYPE_NAME);
        Role kind = role.adaptFromJson(object.getJsonString(KIND_NAME));
        Options opts = object.containsKey(OPTIONS_NAME) ?
                options.adaptFromJson(object.getJsonObject(OPTIONS_NAME)) :
                null;
        List<Route> routes = object.containsKey(ROUTES_NAME)
                ? object.getJsonArray(ROUTES_NAME)
                    .stream()
                    .map(JsonValue::asJsonObject)
                    .map(route::adaptFromJson)
                    .collect(toList())
                : ROUTES_DEFAULT;

        return new Binding(entry, type, kind, opts, routes);
    }
}
