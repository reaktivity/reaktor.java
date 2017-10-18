/**
 * Copyright 2016-2017 The Reaktivity Project
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
package org.reaktivity.k3po.nukleus.ext.internal.types;

import static java.lang.Integer.MAX_VALUE;
import static java.util.Collections.emptyList;
import static java.util.Collections.unmodifiableSet;

import java.util.LinkedHashSet;
import java.util.Set;

import org.kaazing.k3po.lang.types.StructuredTypeInfo;
import org.kaazing.k3po.lang.types.TypeInfo;
import org.kaazing.k3po.lang.types.TypeSystemSpi;

public final class NukleusTypeSystem implements TypeSystemSpi
{
    public static final TypeInfo<Long> OPTION_ROUTE = new TypeInfo<>("route", Long.class);
    public static final TypeInfo<String> OPTION_REPLY_TO = new TypeInfo<>("replyTo", String.class);
    public static final TypeInfo<Integer> OPTION_WINDOW = new TypeInfo<>("window", Integer.class);
    public static final TypeInfo<Integer> OPTION_PADDING = new TypeInfo<>("padding", Integer.class);
    public static final TypeInfo<String> OPTION_UPDATE = new TypeInfo<>("update", String.class);
    public static final TypeInfo<String> OPTION_PARTITION = new TypeInfo<>("partition", String.class);
    public static final TypeInfo<Long> OPTION_CORRELATION = new TypeInfo<>("correlation", Long.class);
    public static final TypeInfo<String> OPTION_TRANSMISSION = new TypeInfo<>("transmission", String.class);
    public static final TypeInfo<String> OPTION_THROTTLE = new TypeInfo<>("throttle", String.class);
    public static final TypeInfo<Long> OPTION_AUTHORIZATION = new TypeInfo<>("authorization", Long.class);
    public static final TypeInfo<String> OPTION_BYTE_ORDER = new TypeInfo<>("byteorder", String.class);

    public static final StructuredTypeInfo CONFIG_BEGIN_EXT =
            new StructuredTypeInfo("nukleus", "begin.ext", emptyList(), MAX_VALUE);
    public static final StructuredTypeInfo CONFIG_DATA_EXT =
            new StructuredTypeInfo("nukleus", "data.ext", emptyList(), MAX_VALUE);
    public static final StructuredTypeInfo CONFIG_END_EXT =
            new StructuredTypeInfo("nukleus", "end.ext", emptyList(), MAX_VALUE);

    private final Set<TypeInfo<?>> acceptOptions;
    private final Set<TypeInfo<?>> connectOptions;
    private final Set<TypeInfo<?>> readOptions;
    private final Set<TypeInfo<?>> writeOptions;
    private final Set<StructuredTypeInfo> readConfigs;
    private final Set<StructuredTypeInfo> writeConfigs;

    public NukleusTypeSystem()
    {
        Set<TypeInfo<?>> acceptOptions = new LinkedHashSet<>();
        acceptOptions.add(OPTION_ROUTE);
        acceptOptions.add(OPTION_REPLY_TO);
        acceptOptions.add(OPTION_WINDOW);
        acceptOptions.add(OPTION_PADDING);
        acceptOptions.add(OPTION_UPDATE);
        acceptOptions.add(OPTION_PARTITION);
        acceptOptions.add(OPTION_AUTHORIZATION);
        acceptOptions.add(OPTION_CORRELATION);
        acceptOptions.add(OPTION_THROTTLE);
        acceptOptions.add(OPTION_TRANSMISSION);
        acceptOptions.add(OPTION_BYTE_ORDER);
        this.acceptOptions = unmodifiableSet(acceptOptions);

        Set<TypeInfo<?>> connectOptions = new LinkedHashSet<>();
        connectOptions.add(OPTION_ROUTE);
        connectOptions.add(OPTION_REPLY_TO);
        connectOptions.add(OPTION_WINDOW);
        connectOptions.add(OPTION_PADDING);
        connectOptions.add(OPTION_UPDATE);
        connectOptions.add(OPTION_PARTITION);
        connectOptions.add(OPTION_AUTHORIZATION);
        connectOptions.add(OPTION_CORRELATION);
        connectOptions.add(OPTION_THROTTLE);
        connectOptions.add(OPTION_TRANSMISSION);
        connectOptions.add(OPTION_BYTE_ORDER);
        this.connectOptions = unmodifiableSet(connectOptions);

        Set<TypeInfo<?>> readOptions = new LinkedHashSet<>();
        readOptions.add(OPTION_PARTITION);
        this.readOptions = unmodifiableSet(readOptions);

        Set<TypeInfo<?>> writeOptions = new LinkedHashSet<>();
        writeOptions.add(OPTION_PARTITION);
        this.writeOptions = unmodifiableSet(writeOptions);

        Set<StructuredTypeInfo> readConfigs = new LinkedHashSet<>();
        readConfigs.add(CONFIG_BEGIN_EXT);
        readConfigs.add(CONFIG_DATA_EXT);
        readConfigs.add(CONFIG_END_EXT);
        this.readConfigs = readConfigs;

        Set<StructuredTypeInfo> writeConfigs = new LinkedHashSet<>();
        writeConfigs.add(CONFIG_BEGIN_EXT);
        writeConfigs.add(CONFIG_DATA_EXT);
        writeConfigs.add(CONFIG_END_EXT);
        this.writeConfigs = writeConfigs;
    }

    @Override
    public String getName()
    {
        return "nukleus";
    }

    @Override
    public Set<TypeInfo<?>> acceptOptions()
    {
        return acceptOptions;
    }

    @Override
    public Set<TypeInfo<?>> connectOptions()
    {
        return connectOptions;
    }

    @Override
    public Set<TypeInfo<?>> readOptions()
    {
        return readOptions;
    }

    @Override
    public Set<TypeInfo<?>> writeOptions()
    {
        return writeOptions;
    }

    @Override
    public Set<StructuredTypeInfo> readConfigs()
    {
        return readConfigs;
    }

    @Override
    public Set<StructuredTypeInfo> writeConfigs()
    {
        return writeConfigs;
    }
}
