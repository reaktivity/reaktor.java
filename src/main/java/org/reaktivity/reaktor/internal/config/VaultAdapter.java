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

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.bind.adapter.JsonbAdapter;

import org.reaktivity.reaktor.config.Options;
import org.reaktivity.reaktor.config.OptionsAdapterSpi;
import org.reaktivity.reaktor.config.Vault;

public class VaultAdapter implements JsonbAdapter<Vault, JsonObject>
{
    private static final String NAME_NAME = "name";
    private static final String TYPE_NAME = "type";
    private static final String OPTIONS_NAME = "options";

    private final OptionsAdapter options;

    public VaultAdapter()
    {
        this.options = new OptionsAdapter(OptionsAdapterSpi.Kind.VAULT);
    }

    @Override
    public JsonObject adaptToJson(
        Vault vault)
    {
        options.adaptType(vault.type);

        JsonObjectBuilder object = Json.createObjectBuilder();

        object.add(NAME_NAME, vault.name);
        object.add(TYPE_NAME, vault.type);

        if (vault.options != null)
        {
            object.add(OPTIONS_NAME, options.adaptToJson(vault.options));
        }

        return object.build();
    }

    @Override
    public Vault adaptFromJson(
        JsonObject object)
    {
        String name = object.getString(NAME_NAME);
        String type = object.getString(TYPE_NAME);

        options.adaptType(type);

        Options opts = object.containsKey(OPTIONS_NAME) ?
                options.adaptFromJson(object.getJsonObject(OPTIONS_NAME)) :
                null;

        return new Vault(name, type, opts);
    }
}
