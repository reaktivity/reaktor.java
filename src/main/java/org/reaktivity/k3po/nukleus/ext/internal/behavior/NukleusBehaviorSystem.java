/**
 * Copyright 2016-2019 The Reaktivity Project
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
package org.reaktivity.k3po.nukleus.ext.internal.behavior;

import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableMap;
import static java.util.stream.Collectors.toList;
import static org.reaktivity.k3po.nukleus.ext.internal.behavior.NukleusExtensionKind.BEGIN;
import static org.reaktivity.k3po.nukleus.ext.internal.behavior.NukleusExtensionKind.CHALLENGE;
import static org.reaktivity.k3po.nukleus.ext.internal.behavior.NukleusExtensionKind.DATA;
import static org.reaktivity.k3po.nukleus.ext.internal.behavior.NukleusExtensionKind.END;
import static org.reaktivity.k3po.nukleus.ext.internal.types.NukleusTypeSystem.CONFIG_BEGIN_EXT;
import static org.reaktivity.k3po.nukleus.ext.internal.types.NukleusTypeSystem.CONFIG_CHALLENGE;
import static org.reaktivity.k3po.nukleus.ext.internal.types.NukleusTypeSystem.CONFIG_DATA_EMPTY;
import static org.reaktivity.k3po.nukleus.ext.internal.types.NukleusTypeSystem.CONFIG_DATA_EXT;
import static org.reaktivity.k3po.nukleus.ext.internal.types.NukleusTypeSystem.CONFIG_DATA_NULL;
import static org.reaktivity.k3po.nukleus.ext.internal.types.NukleusTypeSystem.CONFIG_END_EXT;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import org.jboss.netty.channel.ChannelHandler;
import org.kaazing.k3po.driver.internal.behavior.BehaviorSystemSpi;
import org.kaazing.k3po.driver.internal.behavior.ReadConfigFactory;
import org.kaazing.k3po.driver.internal.behavior.ReadOptionFactory;
import org.kaazing.k3po.driver.internal.behavior.WriteConfigFactory;
import org.kaazing.k3po.driver.internal.behavior.WriteOptionFactory;
import org.kaazing.k3po.driver.internal.behavior.handler.codec.MessageDecoder;
import org.kaazing.k3po.driver.internal.behavior.handler.codec.MessageEncoder;
import org.kaazing.k3po.driver.internal.behavior.handler.command.WriteConfigHandler;
import org.kaazing.k3po.lang.internal.RegionInfo;
import org.kaazing.k3po.lang.internal.ast.AstReadConfigNode;
import org.kaazing.k3po.lang.internal.ast.AstWriteConfigNode;
import org.kaazing.k3po.lang.internal.ast.matcher.AstValueMatcher;
import org.kaazing.k3po.lang.internal.ast.value.AstValue;
import org.kaazing.k3po.lang.types.StructuredTypeInfo;
import org.kaazing.k3po.lang.types.TypeInfo;
import org.reaktivity.k3po.nukleus.ext.internal.behavior.config.NukleusExtensionDecoder;
import org.reaktivity.k3po.nukleus.ext.internal.behavior.config.NukleusExtensionEncoder;
import org.reaktivity.k3po.nukleus.ext.internal.behavior.config.ReadBeginExtHandler;
import org.reaktivity.k3po.nukleus.ext.internal.behavior.config.ReadChallengeHandler;
import org.reaktivity.k3po.nukleus.ext.internal.behavior.config.ReadDataExtHandler;
import org.reaktivity.k3po.nukleus.ext.internal.behavior.config.ReadEndExtHandler;
import org.reaktivity.k3po.nukleus.ext.internal.behavior.config.ReadNullDataHandler;
import org.reaktivity.k3po.nukleus.ext.internal.behavior.config.WriteChallengeHandler;
import org.reaktivity.k3po.nukleus.ext.internal.behavior.config.WriteEmptyDataHandler;

public class NukleusBehaviorSystem implements BehaviorSystemSpi
{
    private final Map<TypeInfo<?>, ReadOptionFactory> readOptionFactories;
    private final Map<TypeInfo<?>, WriteOptionFactory> writeOptionFactories;

    private final Map<StructuredTypeInfo, ReadConfigFactory> readConfigFactories;
    private final Map<StructuredTypeInfo, WriteConfigFactory> writeConfigFactories;

    public NukleusBehaviorSystem()
    {
        this.readOptionFactories = emptyMap();
        this.writeOptionFactories = emptyMap();

        Map<StructuredTypeInfo, ReadConfigFactory> readConfigFactories = new LinkedHashMap<>();
        readConfigFactories.put(CONFIG_BEGIN_EXT, NukleusBehaviorSystem::newReadBeginExtHandler);
        readConfigFactories.put(CONFIG_DATA_EXT, NukleusBehaviorSystem::newReadDataExtHandler);
        readConfigFactories.put(CONFIG_DATA_NULL, NukleusBehaviorSystem::newReadNullDataHandler);
        readConfigFactories.put(CONFIG_END_EXT, NukleusBehaviorSystem::newReadEndExtHandler);
        readConfigFactories.put(CONFIG_CHALLENGE, NukleusBehaviorSystem::newReadChallengeHandler);
        this.readConfigFactories = unmodifiableMap(readConfigFactories);

        Map<StructuredTypeInfo, WriteConfigFactory> writeConfigFactories = new LinkedHashMap<>();
        writeConfigFactories.put(CONFIG_BEGIN_EXT, NukleusBehaviorSystem::newWriteBeginExtHandler);
        writeConfigFactories.put(CONFIG_DATA_EMPTY, NukleusBehaviorSystem::newWriteEmptyDataHandler);
        writeConfigFactories.put(CONFIG_DATA_EXT, NukleusBehaviorSystem::newWriteDataExtHandler);
        writeConfigFactories.put(CONFIG_END_EXT, NukleusBehaviorSystem::newWriteEndExtHandler);
        writeConfigFactories.put(CONFIG_CHALLENGE, NukleusBehaviorSystem::newWriteChallengeHandler);
        this.writeConfigFactories = unmodifiableMap(writeConfigFactories);
    }

    @Override
    public Set<StructuredTypeInfo> getReadConfigTypes()
    {
        return readConfigFactories.keySet();
    }

    @Override
    public Set<StructuredTypeInfo> getWriteConfigTypes()
    {
        return writeConfigFactories.keySet();
    }

    @Override
    public ReadConfigFactory readConfigFactory(
        StructuredTypeInfo configType)
    {
        return readConfigFactories.get(configType);
    }

    @Override
    public WriteConfigFactory writeConfigFactory(
        StructuredTypeInfo configType)
    {
        return writeConfigFactories.get(configType);
    }

    @Override
    public Set<TypeInfo<?>> getReadOptionTypes()
    {
        return readOptionFactories.keySet();
    }

    @Override
    public Set<TypeInfo<?>> getWriteOptionTypes()
    {
        return writeOptionFactories.keySet();
    }

    @Override
    public ReadOptionFactory readOptionFactory(
        TypeInfo<?> optionType)
    {
        return readOptionFactories.get(optionType);
    }

    @Override
    public WriteOptionFactory writeOptionFactory(
        TypeInfo<?> optionType)
    {
        return writeOptionFactories.get(optionType);
    }

    private static ReadBeginExtHandler newReadBeginExtHandler(
        AstReadConfigNode node,
        Function<AstValueMatcher, MessageDecoder> decoderFactory)
    {
        RegionInfo regionInfo = node.getRegionInfo();
        StructuredTypeInfo type = node.getType();
        List<MessageDecoder> decoders = node.getMatchers().stream().map(decoderFactory).collect(toList());

        ReadBeginExtHandler handler = new ReadBeginExtHandler(new NukleusExtensionDecoder(BEGIN, type, decoders));
        handler.setRegionInfo(regionInfo);
        return handler;
    }

    private static ReadDataExtHandler newReadDataExtHandler(
        AstReadConfigNode node,
        Function<AstValueMatcher, MessageDecoder> decoderFactory)
    {
        RegionInfo regionInfo = node.getRegionInfo();
        StructuredTypeInfo type = node.getType();
        List<MessageDecoder> decoders = node.getMatchers().stream().map(decoderFactory).collect(toList());

        ReadDataExtHandler handler = new ReadDataExtHandler(new NukleusExtensionDecoder(DATA, type, decoders));
        handler.setRegionInfo(regionInfo);
        return handler;
    }

    private static ChannelHandler newReadNullDataHandler(
        AstReadConfigNode node,
        Function<AstValueMatcher, MessageDecoder> decoderFactory)
    {
        RegionInfo regionInfo = node.getRegionInfo();
        ReadNullDataHandler handler = new ReadNullDataHandler();
        handler.setRegionInfo(regionInfo);
        return handler;
    }

    private static ReadEndExtHandler newReadEndExtHandler(
        AstReadConfigNode node,
        Function<AstValueMatcher, MessageDecoder> decoderFactory)
    {
        RegionInfo regionInfo = node.getRegionInfo();
        StructuredTypeInfo type = node.getType();
        List<MessageDecoder> decoders = node.getMatchers().stream().map(decoderFactory).collect(toList());

        ReadEndExtHandler handler = new ReadEndExtHandler(new NukleusExtensionDecoder(END, type, decoders));
        handler.setRegionInfo(regionInfo);
        return handler;
    }

    private static ReadChallengeHandler newReadChallengeHandler(
        AstReadConfigNode node,
        Function<AstValueMatcher, MessageDecoder> decoderFactory)
    {
        RegionInfo regionInfo = node.getRegionInfo();
        StructuredTypeInfo type = node.getType();
        List<MessageDecoder> decoders = node.getMatchers().stream().map(decoderFactory).collect(toList());

        ReadChallengeHandler handler = new ReadChallengeHandler(new NukleusExtensionDecoder(CHALLENGE, type, decoders));
        handler.setRegionInfo(regionInfo);
        return handler;
    }

    private static WriteConfigHandler newWriteBeginExtHandler(
        AstWriteConfigNode node,
        Function<AstValue<?>, MessageEncoder> encoderFactory)
    {
        StructuredTypeInfo type = node.getType();
        List<MessageEncoder> encoders = node.getValues().stream().map(encoderFactory).collect(toList());

        WriteConfigHandler handler = new WriteConfigHandler(new NukleusExtensionEncoder(BEGIN, type, encoders));
        handler.setRegionInfo(node.getRegionInfo());
        return handler;
    }

    private static WriteConfigHandler newWriteDataExtHandler(
        AstWriteConfigNode node,
        Function<AstValue<?>, MessageEncoder> encoderFactory)
    {
        StructuredTypeInfo type = node.getType();
        List<MessageEncoder> encoders = node.getValues().stream().map(encoderFactory).collect(toList());

        WriteConfigHandler handler = new WriteConfigHandler(new NukleusExtensionEncoder(DATA, type, encoders));
        handler.setRegionInfo(node.getRegionInfo());
        return handler;
    }

    private static WriteEmptyDataHandler newWriteEmptyDataHandler(
        AstWriteConfigNode node,
        Function<AstValue<?>, MessageEncoder> encoderFactory)
    {
        WriteEmptyDataHandler handler = new WriteEmptyDataHandler();
        handler.setRegionInfo(node.getRegionInfo());
        return handler;
    }

    private static WriteConfigHandler newWriteEndExtHandler(
        AstWriteConfigNode node,
        Function<AstValue<?>, MessageEncoder> encoderFactory)
    {
        StructuredTypeInfo type = node.getType();
        List<MessageEncoder> encoders = node.getValues().stream().map(encoderFactory).collect(toList());

        WriteConfigHandler handler = new WriteConfigHandler(new NukleusExtensionEncoder(END, type, encoders));
        handler.setRegionInfo(node.getRegionInfo());
        return handler;
    }

    private static WriteChallengeHandler newWriteChallengeHandler(
        AstWriteConfigNode node,
        Function<AstValue<?>, MessageEncoder> encoderFactory)
    {
        StructuredTypeInfo type = node.getType();
        List<MessageEncoder> encoders = node.getValues().stream().map(encoderFactory).collect(toList());

        WriteChallengeHandler handler = new WriteChallengeHandler(new NukleusExtensionEncoder(CHALLENGE, type, encoders));
        handler.setRegionInfo(node.getRegionInfo());
        return handler;
    }
}
