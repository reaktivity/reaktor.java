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
import org.reaktivity.reaktor.config.Binding;
import org.reaktivity.reaktor.config.Namespace;
import org.reaktivity.reaktor.nukleus.Elektron;

public class NamespaceContext
{
    private final Namespace namespace;
    private final Function<String, Elektron> lookupElektron;
    private final ToIntFunction<String> supplyLabelId;
    private final int namespaceId;
    private final Int2ObjectHashMap<BindingContext> bindingsById;

    public NamespaceContext(
        Namespace namespace,
        Function<String, Elektron> lookupElektron,
        ToIntFunction<String> supplyLabelId)
    {
        this.namespace = namespace;
        this.lookupElektron = lookupElektron;
        this.supplyLabelId = supplyLabelId;
        this.namespaceId = supplyLabelId.applyAsInt(namespace.name);
        this.bindingsById = new Int2ObjectHashMap<>();
    }

    public int namespaceId()
    {
        return namespaceId;
    }

    public void attach()
    {
        namespace.bindings.forEach(this::attachBinding);
    }

    public void detach()
    {
        namespace.bindings.forEach(this::detachBinding);
    }

    private void attachBinding(
        Binding binding)
    {
        Elektron elektron = lookupElektron.apply(binding.type);
        int bindingId = supplyLabelId.applyAsInt(binding.entry);
        BindingContext context = new BindingContext(binding, elektron);
        bindingsById.put(bindingId, context);
        context.attach();
    }

    private void detachBinding(
        Binding binding)
    {
        int bindingId = supplyLabelId.applyAsInt(binding.entry);
        BindingContext context = bindingsById.remove(bindingId);
        context.detach();
    }

    BindingContext findBinding(
        int bindingId)
    {
        return bindingsById.get(bindingId);
    }
}
