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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.emptyCollectionOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

import javax.json.bind.Jsonb;
import javax.json.bind.JsonbBuilder;
import javax.json.bind.JsonbConfig;

import org.junit.Before;
import org.junit.Test;
import org.reaktivity.reaktor.config.Binding;
import org.reaktivity.reaktor.config.Reference;

public class ConfigurationAdapterTest
{
    private Jsonb jsonb;

    @Before
    public void initJson()
    {
        JsonbConfig config = new JsonbConfig()
                .withAdapters(new ConfigurationAdapter());
        jsonb = JsonbBuilder.create(config);
    }

    @Test
    public void shouldReadEmptyConfiguration()
    {
        String text =
                "{" +
                "}";

        Configuration config = jsonb.fromJson(text, Configuration.class);

        assertThat(config, not(nullValue()));
        assertThat(config.name, equalTo("default"));
        assertThat(config.bindings, emptyCollectionOf(Binding.class));
        assertThat(config.namespaces, emptyCollectionOf(Reference.class));
    }

    @Test
    public void shouldWriteEmptyConfiguration()
    {
        Configuration config = new Configuration("default", emptyList(), emptyList());

        String text = jsonb.toJson(config);

        assertThat(text, not(nullValue()));
        assertThat(text, equalTo("{}"));
    }

    @Test
    public void shouldReadConfiguration()
    {
        String text =
                "{" +
                    "\"name\": \"test\"," +
                    "\"bindings\":" +
                    "[" +
                    "]," +
                    "\"namespaces\":" +
                    "[" +
                    "]" +
                "}";

        Configuration config = jsonb.fromJson(text, Configuration.class);

        assertThat(config, not(nullValue()));
        assertThat(config.name, equalTo("test"));
        assertThat(config.bindings, emptyCollectionOf(Binding.class));
        assertThat(config.namespaces, emptyCollectionOf(Reference.class));
    }

    @Test
    public void shouldWriteConfiguration()
    {
        Configuration config = new Configuration("test", emptyList(), emptyList());

        String text = jsonb.toJson(config);

        assertThat(text, not(nullValue()));
        assertThat(text, equalTo("{\"name\":\"test\"}"));
    }
}
