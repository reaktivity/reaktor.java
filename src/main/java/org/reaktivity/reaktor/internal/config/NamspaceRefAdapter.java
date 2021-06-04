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

import static java.util.Collections.emptyMap;
import static java.util.stream.Collectors.toMap;

import java.util.Map;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonString;
import javax.json.JsonValue;
import javax.json.bind.adapter.JsonbAdapter;

public class NamspaceRefAdapter implements JsonbAdapter<NamespaceRef, JsonObject>
{
    private static final String NAME_NAME = "name";
    private static final String LINKS_NAME = "links";

    private static final Map<String, String> LINKS_DEFAULT = emptyMap();

    @Override
    public JsonObject adaptToJson(
        NamespaceRef ref)
    {
        JsonObjectBuilder object = Json.createObjectBuilder();

        object.add(NAME_NAME, ref.name);

        if (!LINKS_DEFAULT.equals(ref.links))
        {
            JsonObjectBuilder links = Json.createObjectBuilder();
            ref.links.forEach(links::add);
            object.add(LINKS_NAME, links);
        }

        return object.build();
    }

    @Override
    public NamespaceRef adaptFromJson(
        JsonObject object)
    {
        String name = object.getString(NAME_NAME);
        Map<String, String> links = object.containsKey(LINKS_NAME)
                ? object.getJsonObject(LINKS_NAME)
                    .entrySet()
                    .stream()
                    .collect(toMap(Map.Entry::getKey, e -> asJsonString(e.getValue())))
                : LINKS_DEFAULT;

        return new NamespaceRef(name, links);
    }

    private static String asJsonString(
        JsonValue value)
    {
        return ((JsonString) value).getString();
    }
}
