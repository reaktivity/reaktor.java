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
import org.reaktivity.reaktor.config.Condition;
import org.reaktivity.reaktor.config.Route;

public class RouteAdapterTest
{
    private Jsonb jsonb;

    @Before
    public void initJson()
    {
        JsonbConfig config = new JsonbConfig()
                .withAdapters(new RouteAdapter());
        jsonb = JsonbBuilder.create(config);
    }

    @Test
    public void shouldReadRoute()
    {
        String text =
                "{" +
                    "\"exit\": \"test\"," +
                    "\"when\":" +
                    "[" +
                    "]" +
                "}";

        Route route = jsonb.fromJson(text, Route.class);

        assertThat(route, not(nullValue()));
        assertThat(route.exit, equalTo("test"));
        assertThat(route.when, emptyCollectionOf(Condition.class));
    }


    @Test
    public void shouldWriteRoute()
    {
        Route route = new Route("test", emptyList());

        String text = jsonb.toJson(route);

        assertThat(text, not(nullValue()));
        assertThat(text, equalTo("{\"exit\":\"test\"}"));
    }
}
