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
package org.reaktivity.reaktor.internal.context;

import static java.lang.Thread.currentThread;
import static java.net.http.HttpClient.Redirect.NORMAL;
import static java.net.http.HttpClient.Version.HTTP_2;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.function.ToIntFunction;

import javax.json.bind.Jsonb;
import javax.json.bind.JsonbBuilder;
import javax.json.bind.JsonbConfig;

import org.reaktivity.reaktor.config.Binding;
import org.reaktivity.reaktor.config.Route;
import org.reaktivity.reaktor.internal.config.Configuration;
import org.reaktivity.reaktor.internal.config.ConfigurationAdapter;
import org.reaktivity.reaktor.internal.stream.RouteId;

public class ConfigureTask implements Callable<Void>
{
    private final ToIntFunction<String> supplyId;
    private final URI configURI;
    private final Collection<DispatchAgent> dispatchers;

    public ConfigureTask(
        URI configURI,
        ToIntFunction<String> supplyId,
        Collection<DispatchAgent> dispatchers)
    {
        this.supplyId = supplyId;
        this.configURI = configURI;
        this.dispatchers = dispatchers;
    }

    @Override
    public Void call() throws Exception
    {
        currentThread().setName("reaktor/config");

        String configText;

        if ("https".equals(configURI.getScheme()) || "https".equals(configURI.getScheme()))
        {
            HttpClient client = HttpClient.newBuilder()
                .version(HTTP_2)
                .followRedirects(NORMAL)
                .build();

            HttpRequest request = HttpRequest.newBuilder()
                .GET()
                .uri(configURI)
                .build();

            HttpResponse<String> response = client.send(
                request,
                BodyHandlers.ofString());
            String body = response.body();

            configText = body;
        }
        else if ("file".equals(configURI.getScheme()))
        {
            Path configPath = Paths.get(configURI.getPath());
            configText = Files.readString(configPath);
        }
        else
        {
            throw new IllegalAccessException("Unexpected config scheme: " + configURI);
        }

        JsonbConfig config = new JsonbConfig()
                .withAdapters(new ConfigurationAdapter());
        Jsonb jsonb = JsonbBuilder.create(config);

        Configuration configuration = jsonb.fromJson(configText, Configuration.class);

        configuration.id = supplyId.applyAsInt(configuration.name);
        for (Binding binding : configuration.bindings)
        {
            binding.id = RouteId.routeId(configuration.id, supplyId.applyAsInt(binding.entry));
            for (Route route : binding.routes)
            {
                route.id = RouteId.routeId(configuration.id, supplyId.applyAsInt(route.exit));
            }
        }

        CompletableFuture<Void> future = CompletableFuture.completedFuture(null);
        for (DispatchAgent dispatcher : dispatchers)
        {
            future = CompletableFuture.allOf(future, dispatcher.attach(configuration));
        }
        future.join();

        // TODO: repeat to detect and apply changes

        return null;
    }
}
