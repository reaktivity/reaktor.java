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
package org.reaktivity.nukleus.internal;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

public final class CopyOnWriteHashMap<K, V> implements Map<K, V>
{
    private final UnaryOperator<Map<K, V>> copier;

    private volatile Map<K, V> snapshot;

    public CopyOnWriteHashMap()
    {
        this(HashMap::new, HashMap::new);
    }

    private CopyOnWriteHashMap(
        Supplier<Map<K, V>> initializer,
        UnaryOperator<Map<K, V>> copier)
    {
        this.copier = copier;
        this.snapshot = initializer.get();
    }

    @Override
    public int size()
    {
        return snapshot.size();
    }

    @Override
    public boolean isEmpty()
    {
        return snapshot.isEmpty();
    }

    @Override
    public boolean containsKey(
        Object key)
    {
        return snapshot.containsKey(key);
    }

    @Override
    public boolean containsValue(
        Object value)
    {
        return snapshot.containsValue(value);
    }

    @Override
    public V get(
        Object key)
    {
        return snapshot.get(key);
    }

    @Override
    public V put(K key, V value)
    {
        synchronized (this)
        {
            Map<K, V> newSnapshot = copier.apply(snapshot);
            V oldValue = newSnapshot.put(key, value);
            this.snapshot = newSnapshot;
            return oldValue;
        }
    }

    @Override
    public V remove(
        Object key)
    {
        synchronized (this)
        {
            Map<K, V> newSnapshot = copier.apply(snapshot);
            V oldValue = newSnapshot.remove(key);
            this.snapshot = newSnapshot;
            return oldValue;
        }
    }

    @Override
    public void putAll(
        Map<? extends K, ? extends V> m)
    {
        synchronized (this)
        {
            Map<K, V> newSnapshot = copier.apply(snapshot);
            newSnapshot.putAll(m);
            this.snapshot = newSnapshot;
        }
    }

    @Override
    public void clear()
    {
        synchronized (this)
        {
            Map<K, V> newSnapshot = copier.apply(snapshot);
            newSnapshot.clear();
            this.snapshot = newSnapshot;
        }
    }

    @Override
    public Set<K> keySet()
    {
        return snapshot.keySet();
    }

    @Override
    public Collection<V> values()
    {
        return snapshot.values();
    }

    @Override
    public Set<Entry<K, V>> entrySet()
    {
        return snapshot.entrySet();
    }

    @Override
    public V getOrDefault(
        Object key, V defaultValue)
    {
        return snapshot.getOrDefault(key, defaultValue);
    }

    @Override
    public void forEach(
        BiConsumer<? super K, ? super V> action)
    {
        snapshot.forEach(action);
    }

    @Override
    public void replaceAll(
        BiFunction<? super K, ? super V, ? extends V> function)
    {
        synchronized (this)
        {
            Map<K, V> newSnapshot = copier.apply(snapshot);
            newSnapshot.replaceAll(function);
            this.snapshot = newSnapshot;
        }
    }

    @Override
    public V putIfAbsent(
        K key,
        V value)
    {
        synchronized (this)
        {
            Map<K, V> newSnapshot = copier.apply(snapshot);
            V oldValue = newSnapshot.putIfAbsent(key, value);
            this.snapshot = newSnapshot;
            return oldValue;
        }
    }

    @Override
    public boolean remove(
        Object key,
        Object value)
    {
        synchronized (this)
        {
            Map<K, V> newSnapshot = copier.apply(snapshot);
            boolean removed = newSnapshot.remove(key, value);
            this.snapshot = newSnapshot;
            return removed;
        }
    }

    @Override
    public boolean replace(
        K key,
        V oldValue,
        V newValue)
    {
        synchronized (this)
        {
            Map<K, V> newSnapshot = copier.apply(snapshot);
            boolean replaced = newSnapshot.replace(key, oldValue, newValue);
            this.snapshot = newSnapshot;
            return replaced;
        }
    }

    @Override
    public V replace(
        K key,
        V value)
    {
        synchronized (this)
        {
            Map<K, V> newSnapshot = copier.apply(snapshot);
            V oldValue = newSnapshot.replace(key, value);
            this.snapshot = newSnapshot;
            return oldValue;
        }
    }

    @Override
    public V computeIfAbsent(
        K key,
        Function<? super K, ? extends V> mappingFunction)
    {
        synchronized (this)
        {
            Map<K, V> newSnapshot = copier.apply(snapshot);
            V oldValue = newSnapshot.computeIfAbsent(key, mappingFunction);
            this.snapshot = newSnapshot;
            return oldValue;
        }
    }

    @Override
    public V computeIfPresent(
        K key,
        BiFunction<? super K, ? super V, ? extends V> remappingFunction)
    {
        synchronized (this)
        {
            Map<K, V> newSnapshot = copier.apply(snapshot);
            V oldValue = newSnapshot.computeIfPresent(key, remappingFunction);
            this.snapshot = newSnapshot;
            return oldValue;
        }
    }

    @Override
    public V compute(
        K key,
        BiFunction<? super K, ? super V, ? extends V> remappingFunction)
    {
        synchronized (this)
        {
            Map<K, V> newSnapshot = copier.apply(snapshot);
            V oldValue = newSnapshot.compute(key, remappingFunction);
            this.snapshot = newSnapshot;
            return oldValue;
        }
    }

    @Override
    public V merge(
        K key,
        V value,
        BiFunction<? super V, ? super V, ? extends V> remappingFunction)
    {
        synchronized (this)
        {
            Map<K, V> newSnapshot = copier.apply(snapshot);
            V oldValue = newSnapshot.merge(key, value, remappingFunction);
            this.snapshot = newSnapshot;
            return oldValue;
        }
    }

    @Override
    public int hashCode()
    {
        return snapshot.hashCode();
    }

    @Override
    public boolean equals(Object obj)
    {
        return snapshot.equals(obj);
    }

    @Override
    public String toString()
    {
        return snapshot.toString();
    }
}
