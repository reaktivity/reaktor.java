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

import java.util.function.Function;
import java.util.function.ToIntFunction;

import org.agrona.collections.Int2ObjectHashMap;
import org.reaktivity.reaktor.config.Namespace;
import org.reaktivity.reaktor.internal.stream.RouteId;
import org.reaktivity.reaktor.nukleus.Elektron;

public class ConfigurationContext
{
    private final Function<String, Elektron> elektronsByName;
    private final ToIntFunction<String> supplyLabelId;

    private final Int2ObjectHashMap<NamespaceContext> namespacesById;

    public ConfigurationContext(
        Function<String, Elektron> elektronsByName,
        ToIntFunction<String> supplyLabelId)
    {
        this.elektronsByName = elektronsByName;
        this.supplyLabelId = supplyLabelId;
        this.namespacesById = new Int2ObjectHashMap<>();
    }

    public NamespaceTask attach(
        Namespace namespace)
    {
        return new NamespaceTask(namespace, this::attachNamespace);
    }

    public NamespaceTask detach(
        Namespace namespace)
    {
        return new NamespaceTask(namespace, this::detachNamespace);
    }

    public BindingContext resolve(
        long routeId)
    {
        int namespaceId = RouteId.namespaceId(routeId);
        int bindingId = RouteId.bindingId(routeId);

        NamespaceContext namespace = findNamespace(namespaceId);
        return namespace != null ? namespace.findBinding(bindingId) : null;
    }

    private NamespaceContext findNamespace(
        int namespaceId)
    {
        return namespacesById.get(namespaceId);
    }

    private void attachNamespace(
        Namespace namespace)
    {
        NamespaceContext context = new NamespaceContext(namespace, elektronsByName, supplyLabelId);
        context.attach();
        namespacesById.put(context.namespaceId(), context);
    }

    protected void detachNamespace(
        Namespace namespace)
    {
        int namespaceId = supplyLabelId.applyAsInt(namespace.name);
        NamespaceContext context = namespacesById.remove(namespaceId);
        context.detach();
    }
}
