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
import static org.reaktivity.reaktor.config.Role.SERVER;

import javax.json.bind.Jsonb;
import javax.json.bind.JsonbBuilder;
import javax.json.bind.JsonbConfig;

import org.junit.Before;
import org.junit.Test;
import org.reaktivity.reaktor.config.Binding;
import org.reaktivity.reaktor.config.Route;

public class BindingAdapterTest
{
    private Jsonb jsonb;

    @Before
    public void initJson()
    {
        JsonbConfig config = new JsonbConfig()
                .withAdapters(new BindingAdapter());
        jsonb = JsonbBuilder.create(config);
    }

    @Test
    public void shouldReadBinding()
    {
        String text =
                "{" +
                    "\"entry\": \"test\"," +
                    "\"type\": \"test\"," +
                    "\"kind\": \"server\"," +
                    "\"routes\":" +
                    "[" +
                    "]" +
                "}";

        Binding binding = jsonb.fromJson(text, Binding.class);

        assertThat(binding, not(nullValue()));
        assertThat(binding.entry, equalTo("test"));
        assertThat(binding.kind, equalTo(SERVER));
        assertThat(binding.routes, emptyCollectionOf(Route.class));
    }


    @Test
    public void shouldWriteBinding()
    {
        Binding binding = new Binding("test", "test", SERVER, null, emptyList(), null);

        String text = jsonb.toJson(binding);

        assertThat(text, not(nullValue()));
        assertThat(text, equalTo("{\"entry\":\"test\",\"type\":\"test\",\"kind\":\"server\"}"));
    }
}
