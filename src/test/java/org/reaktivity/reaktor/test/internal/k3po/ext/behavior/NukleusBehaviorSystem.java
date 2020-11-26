/**
 * Copyright 2016-2020 The Reaktivity Project
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
package org.reaktivity.reaktor.test.internal.k3po.ext.behavior;

import static java.util.Collections.unmodifiableMap;
import static java.util.stream.Collectors.toList;
import static org.reaktivity.reaktor.test.internal.k3po.ext.behavior.NukleusExtensionKind.BEGIN;
import static org.reaktivity.reaktor.test.internal.k3po.ext.behavior.NukleusExtensionKind.CHALLENGE;
import static org.reaktivity.reaktor.test.internal.k3po.ext.behavior.NukleusExtensionKind.DATA;
import static org.reaktivity.reaktor.test.internal.k3po.ext.behavior.NukleusExtensionKind.END;
import static org.reaktivity.reaktor.test.internal.k3po.ext.behavior.NukleusExtensionKind.FLUSH;
import static org.reaktivity.reaktor.test.internal.k3po.ext.types.NukleusTypeSystem.ADVISORY_CHALLENGE;
import static org.reaktivity.reaktor.test.internal.k3po.ext.types.NukleusTypeSystem.ADVISORY_FLUSH;
import static org.reaktivity.reaktor.test.internal.k3po.ext.types.NukleusTypeSystem.CONFIG_BEGIN_EXT;
import static org.reaktivity.reaktor.test.internal.k3po.ext.types.NukleusTypeSystem.CONFIG_DATA_EMPTY;
import static org.reaktivity.reaktor.test.internal.k3po.ext.types.NukleusTypeSystem.CONFIG_DATA_EXT;
import static org.reaktivity.reaktor.test.internal.k3po.ext.types.NukleusTypeSystem.CONFIG_DATA_NULL;
import static org.reaktivity.reaktor.test.internal.k3po.ext.types.NukleusTypeSystem.CONFIG_END_EXT;
import static org.reaktivity.reaktor.test.internal.k3po.ext.types.NukleusTypeSystem.OPTION_FLAGS;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

import org.jboss.netty.channel.ChannelHandler;
import org.kaazing.k3po.driver.internal.behavior.BehaviorSystemSpi;
import org.kaazing.k3po.driver.internal.behavior.ReadAdviseFactory;
import org.kaazing.k3po.driver.internal.behavior.ReadAdvisedFactory;
import org.kaazing.k3po.driver.internal.behavior.ReadConfigFactory;
import org.kaazing.k3po.driver.internal.behavior.ReadOptionFactory;
import org.kaazing.k3po.driver.internal.behavior.WriteAdviseFactory;
import org.kaazing.k3po.driver.internal.behavior.WriteAdvisedFactory;
import org.kaazing.k3po.driver.internal.behavior.WriteConfigFactory;
import org.kaazing.k3po.driver.internal.behavior.WriteOptionFactory;
import org.kaazing.k3po.driver.internal.behavior.handler.codec.ChannelDecoder;
import org.kaazing.k3po.driver.internal.behavior.handler.codec.ChannelEncoder;
import org.kaazing.k3po.driver.internal.behavior.handler.codec.MessageDecoder;
import org.kaazing.k3po.driver.internal.behavior.handler.codec.MessageEncoder;
import org.kaazing.k3po.driver.internal.behavior.handler.command.ReadAdviseHandler;
import org.kaazing.k3po.driver.internal.behavior.handler.command.WriteAdviseHandler;
import org.kaazing.k3po.driver.internal.behavior.handler.command.WriteConfigHandler;
import org.kaazing.k3po.driver.internal.behavior.handler.event.ReadAdvisedHandler;
import org.kaazing.k3po.driver.internal.behavior.handler.event.WriteAdvisedHandler;
import org.kaazing.k3po.driver.internal.behavior.visitor.GenerateConfigurationVisitor.State;
import org.kaazing.k3po.lang.internal.RegionInfo;
import org.kaazing.k3po.lang.internal.ast.AstReadAdviseNode;
import org.kaazing.k3po.lang.internal.ast.AstReadAdvisedNode;
import org.kaazing.k3po.lang.internal.ast.AstReadConfigNode;
import org.kaazing.k3po.lang.internal.ast.AstReadOptionNode;
import org.kaazing.k3po.lang.internal.ast.AstWriteAdviseNode;
import org.kaazing.k3po.lang.internal.ast.AstWriteAdvisedNode;
import org.kaazing.k3po.lang.internal.ast.AstWriteConfigNode;
import org.kaazing.k3po.lang.internal.ast.AstWriteOptionNode;
import org.kaazing.k3po.lang.internal.ast.matcher.AstValueMatcher;
import org.kaazing.k3po.lang.internal.ast.value.AstExpressionValue;
import org.kaazing.k3po.lang.internal.ast.value.AstLiteralByteValue;
import org.kaazing.k3po.lang.internal.ast.value.AstLiteralBytesValue;
import org.kaazing.k3po.lang.internal.ast.value.AstLiteralIntegerValue;
import org.kaazing.k3po.lang.internal.ast.value.AstLiteralLongValue;
import org.kaazing.k3po.lang.internal.ast.value.AstLiteralShortValue;
import org.kaazing.k3po.lang.internal.ast.value.AstLiteralTextValue;
import org.kaazing.k3po.lang.internal.ast.value.AstLiteralURIValue;
import org.kaazing.k3po.lang.internal.ast.value.AstValue;
import org.kaazing.k3po.lang.types.StructuredTypeInfo;
import org.kaazing.k3po.lang.types.TypeInfo;
import org.reaktivity.reaktor.test.internal.k3po.ext.behavior.handler.NukleusExtensionDecoder;
import org.reaktivity.reaktor.test.internal.k3po.ext.behavior.handler.NukleusExtensionEncoder;
import org.reaktivity.reaktor.test.internal.k3po.ext.behavior.handler.ReadBeginExtHandler;
import org.reaktivity.reaktor.test.internal.k3po.ext.behavior.handler.ReadDataExtHandler;
import org.reaktivity.reaktor.test.internal.k3po.ext.behavior.handler.ReadEmptyDataHandler;
import org.reaktivity.reaktor.test.internal.k3po.ext.behavior.handler.ReadEndExtHandler;
import org.reaktivity.reaktor.test.internal.k3po.ext.behavior.handler.ReadFlagsOptionHandler;
import org.reaktivity.reaktor.test.internal.k3po.ext.behavior.handler.ReadNullDataHandler;
import org.reaktivity.reaktor.test.internal.k3po.ext.behavior.handler.WriteEmptyDataHandler;
import org.reaktivity.reaktor.test.internal.k3po.ext.behavior.handler.WriteFlagsOptionHandler;

public class NukleusBehaviorSystem implements BehaviorSystemSpi
{
    private final Map<TypeInfo<?>, ReadOptionFactory> readOptionFactories;
    private final Map<TypeInfo<?>, WriteOptionFactory> writeOptionFactories;

    private final Map<StructuredTypeInfo, ReadConfigFactory> readConfigFactories;
    private final Map<StructuredTypeInfo, WriteConfigFactory> writeConfigFactories;

    private final Map<StructuredTypeInfo, ReadAdviseFactory> readAdviseFactories;
    private final Map<StructuredTypeInfo, WriteAdviseFactory> writeAdviseFactories;

    private final Map<StructuredTypeInfo, ReadAdvisedFactory> readAdvisedFactories;
    private final Map<StructuredTypeInfo, WriteAdvisedFactory> writeAdvisedFactories;

    public NukleusBehaviorSystem()
    {
        Map<TypeInfo<?>, ReadOptionFactory> readOptionFactories = new LinkedHashMap<>();
        readOptionFactories.put(OPTION_FLAGS, NukleusBehaviorSystem::newReadFlagsHandler);
        this.readOptionFactories = unmodifiableMap(readOptionFactories);

        Map<TypeInfo<?>, WriteOptionFactory> writeOptionsFactories = new LinkedHashMap<>();
        writeOptionsFactories.put(OPTION_FLAGS, NukleusBehaviorSystem::newWriteFlagsHandler);
        this.writeOptionFactories = unmodifiableMap(writeOptionsFactories);

        Map<StructuredTypeInfo, ReadConfigFactory> readConfigFactories = new LinkedHashMap<>();
        readConfigFactories.put(CONFIG_BEGIN_EXT, NukleusBehaviorSystem::newReadBeginExtHandler);
        readConfigFactories.put(CONFIG_DATA_EMPTY, NukleusBehaviorSystem::newReadEmptyDataHandler);
        readConfigFactories.put(CONFIG_DATA_EXT, NukleusBehaviorSystem::newReadDataExtHandler);
        readConfigFactories.put(CONFIG_DATA_NULL, NukleusBehaviorSystem::newReadNullDataHandler);
        readConfigFactories.put(CONFIG_END_EXT, NukleusBehaviorSystem::newReadEndExtHandler);
        this.readConfigFactories = unmodifiableMap(readConfigFactories);

        Map<StructuredTypeInfo, WriteConfigFactory> writeConfigFactories = new LinkedHashMap<>();
        writeConfigFactories.put(CONFIG_BEGIN_EXT, NukleusBehaviorSystem::newWriteBeginExtHandler);
        writeConfigFactories.put(CONFIG_DATA_EMPTY, NukleusBehaviorSystem::newWriteEmptyDataHandler);
        writeConfigFactories.put(CONFIG_DATA_EXT, NukleusBehaviorSystem::newWriteDataExtHandler);
        writeConfigFactories.put(CONFIG_END_EXT, NukleusBehaviorSystem::newWriteEndExtHandler);
        this.writeConfigFactories = unmodifiableMap(writeConfigFactories);

        Map<StructuredTypeInfo, ReadAdviseFactory> readAdviseFactories = new LinkedHashMap<>();
        readAdviseFactories.put(ADVISORY_CHALLENGE, NukleusBehaviorSystem::newReadAdviseChallengeHandler);
        this.readAdviseFactories = unmodifiableMap(readAdviseFactories);

        Map<StructuredTypeInfo, WriteAdvisedFactory> writeAdvisedFactories = new LinkedHashMap<>();
        writeAdvisedFactories.put(ADVISORY_CHALLENGE, NukleusBehaviorSystem::newWriteAdvisedChallengeHandler);
        this.writeAdvisedFactories = unmodifiableMap(writeAdvisedFactories);

        Map<StructuredTypeInfo, WriteAdviseFactory> writeAdviseFactories = new LinkedHashMap<>();
        writeAdviseFactories.put(ADVISORY_FLUSH, NukleusBehaviorSystem::newWriteAdviseFlushHandler);
        this.writeAdviseFactories = unmodifiableMap(writeAdviseFactories);

        Map<StructuredTypeInfo, ReadAdvisedFactory> readAdvisedFactories = new LinkedHashMap<>();
        readAdvisedFactories.put(ADVISORY_FLUSH, NukleusBehaviorSystem::newReadAdvisedFlushHandler);
        this.readAdvisedFactories = unmodifiableMap(readAdvisedFactories);

        final Set<StructuredTypeInfo> readAdviseKeys = readAdviseFactories.keySet();
        final Set<StructuredTypeInfo> writeAdviseKeys = writeAdviseFactories.keySet();
        assert Objects.equals(readAdviseKeys, writeAdvisedFactories.keySet());
        assert Objects.equals(writeAdviseKeys, readAdvisedFactories.keySet());
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

    @Override
    public Set<StructuredTypeInfo> getReadAdvisoryTypes()
    {
        return readAdviseFactories.keySet();
    }

    @Override
    public Set<StructuredTypeInfo> getWriteAdvisoryTypes()
    {
        return writeAdviseFactories.keySet();
    }

    @Override
    public ReadAdviseFactory readAdviseFactory(
        StructuredTypeInfo advisoryType)
    {
        return readAdviseFactories.get(advisoryType);
    }

    @Override
    public ReadAdvisedFactory readAdvisedFactory(
        StructuredTypeInfo advisoryType)
    {
        return readAdvisedFactories.get(advisoryType);
    }

    @Override
    public WriteAdviseFactory writeAdviseFactory(
        StructuredTypeInfo advisoryType)
    {
        return writeAdviseFactories.get(advisoryType);
    }

    @Override
    public WriteAdvisedFactory writeAdvisedFactory(
        StructuredTypeInfo advisoryType)
    {
        return writeAdvisedFactories.get(advisoryType);
    }

    private static ChannelHandler newReadFlagsHandler(
        AstReadOptionNode node)
    {
        AstValue<?> flagsValue = node.getOptionValue();
        int value = flagsValue.accept(new GenerateFlagsOptionValueVisitor(), null);
        ReadFlagsOptionHandler handler = new ReadFlagsOptionHandler(value);
        handler.setRegionInfo(node.getRegionInfo());
        return handler;
    }

    private static ChannelHandler newWriteFlagsHandler(
        AstWriteOptionNode node)
    {
        AstValue<?> flagsValue = node.getOptionValue();
        int value = flagsValue.accept(new GenerateFlagsOptionValueVisitor(), null);
        WriteFlagsOptionHandler handler = new WriteFlagsOptionHandler(value);
        handler.setRegionInfo(node.getRegionInfo());
        return handler;
    }

    private static ReadBeginExtHandler newReadBeginExtHandler(
        AstReadConfigNode node,
        Function<AstValueMatcher, MessageDecoder> decoderFactory)
    {
        RegionInfo regionInfo = node.getRegionInfo();
        StructuredTypeInfo type = node.getType();
        List<MessageDecoder> decoders = node.getMatchers().stream().map(decoderFactory).collect(toList());

        ChannelDecoder decoder = new NukleusExtensionDecoder(BEGIN, type, decoders);
        ReadBeginExtHandler handler = new ReadBeginExtHandler(decoder);
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

        ChannelDecoder decoder = new NukleusExtensionDecoder(DATA, type, decoders);
        ReadDataExtHandler handler = new ReadDataExtHandler(decoder);
        handler.setRegionInfo(regionInfo);
        return handler;
    }

    private static ReadEmptyDataHandler newReadEmptyDataHandler(
        AstReadConfigNode node,
        Function<AstValueMatcher, MessageDecoder> decoderFactory)
    {
        RegionInfo regionInfo = node.getRegionInfo();
        ReadEmptyDataHandler handler = new ReadEmptyDataHandler();
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

        ChannelDecoder decoder = new NukleusExtensionDecoder(END, type, decoders);
        ReadEndExtHandler handler = new ReadEndExtHandler(decoder);
        handler.setRegionInfo(regionInfo);
        return handler;
    }

    private static WriteConfigHandler newWriteBeginExtHandler(
        AstWriteConfigNode node,
        Function<AstValue<?>, MessageEncoder> encoderFactory)
    {
        StructuredTypeInfo type = node.getType();
        List<MessageEncoder> encoders = node.getValues().stream().map(encoderFactory).collect(toList());

        ChannelEncoder encoder = new NukleusExtensionEncoder(BEGIN, type, encoders);
        WriteConfigHandler handler = new WriteConfigHandler(encoder);
        handler.setRegionInfo(node.getRegionInfo());
        return handler;
    }

    private static WriteConfigHandler newWriteDataExtHandler(
        AstWriteConfigNode node,
        Function<AstValue<?>, MessageEncoder> encoderFactory)
    {
        StructuredTypeInfo type = node.getType();
        List<MessageEncoder> encoders = node.getValues().stream().map(encoderFactory).collect(toList());

        ChannelEncoder encoder = new NukleusExtensionEncoder(DATA, type, encoders);
        WriteConfigHandler handler = new WriteConfigHandler(encoder);
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

        ChannelEncoder encoder = new NukleusExtensionEncoder(END, type, encoders);
        WriteConfigHandler handler = new WriteConfigHandler(encoder);
        handler.setRegionInfo(node.getRegionInfo());
        return handler;
    }

    private static WriteAdviseHandler newWriteAdviseFlushHandler(
        AstWriteAdviseNode node,
        Function<AstValue<?>, MessageEncoder> encoderFactory)
    {
        StructuredTypeInfo type = node.getType();
        List<MessageEncoder> encoders = node.getValues().stream().map(encoderFactory).collect(toList());

        ChannelEncoder encoder = new NukleusExtensionEncoder(FLUSH, type, encoders);
        WriteAdviseHandler handler = new WriteAdviseHandler(ADVISORY_FLUSH, encoder);
        handler.setRegionInfo(node.getRegionInfo());
        return handler;
    }

    private static ReadAdvisedHandler newReadAdvisedFlushHandler(
        AstReadAdvisedNode node,
        Function<AstValueMatcher, MessageDecoder> decoderFactory)
    {
        RegionInfo regionInfo = node.getRegionInfo();
        StructuredTypeInfo type = node.getType();
        List<MessageDecoder> decoders = node.getMatchers().stream().map(decoderFactory).collect(toList());

        ChannelDecoder decoder = new NukleusExtensionDecoder(FLUSH, type, decoders);
        ReadAdvisedHandler handler = new ReadAdvisedHandler(ADVISORY_FLUSH, decoder);
        handler.setRegionInfo(regionInfo);
        return handler;
    }

    private static ReadAdviseHandler newReadAdviseChallengeHandler(
        AstReadAdviseNode node,
        Function<AstValue<?>, MessageEncoder> encoderFactory)
    {
        StructuredTypeInfo type = node.getType();
        List<MessageEncoder> encoders = node.getValues().stream().map(encoderFactory).collect(toList());

        ChannelEncoder encoder = new NukleusExtensionEncoder(CHALLENGE, type, encoders);
        ReadAdviseHandler handler = new ReadAdviseHandler(ADVISORY_CHALLENGE, encoder);
        handler.setRegionInfo(node.getRegionInfo());
        return handler;
    }

    private static WriteAdvisedHandler newWriteAdvisedChallengeHandler(
        AstWriteAdvisedNode node,
        Function<AstValueMatcher, MessageDecoder> decoderFactory)
    {
        RegionInfo regionInfo = node.getRegionInfo();
        StructuredTypeInfo type = node.getType();
        List<MessageDecoder> decoders = node.getMatchers().stream().map(decoderFactory).collect(toList());

        ChannelDecoder decoder = new NukleusExtensionDecoder(CHALLENGE, type, decoders);
        WriteAdvisedHandler handler = new WriteAdvisedHandler(ADVISORY_CHALLENGE, decoder);
        handler.setRegionInfo(regionInfo);
        return handler;
    }

    private static final class GenerateFlagsOptionValueVisitor implements AstValue.Visitor<Integer, State>
    {
        @Override
        public Integer visit(
            AstExpressionValue<?> value,
            State state)
        {
            return (int) value.getValue();
        }

        @Override
        public Integer visit(
            AstLiteralTextValue value,
            State state)
        {
            int flagValue = 0;
            String literal = value.getValue();
            String[] flags = literal.split("\\s+");
            for (String flag : flags)
            {
                switch (flag)
                {
                case "init":
                    flagValue |= 2;
                    break;
                case "fin":
                    flagValue |= 1;
                    break;
                case "auto":
                    flagValue = -1;
                    break;
                }
            }

            return flagValue;
        }

        @Override
        public Integer visit(
            AstLiteralBytesValue value,
            State state)
        {
            return -1;
        }

        @Override
        public Integer visit(
            AstLiteralByteValue value,
            State state)
        {
            return -1;
        }

        @Override
        public Integer visit(
            AstLiteralShortValue value,
            State state)
        {
            return -1;
        }

        @Override
        public Integer visit(
            AstLiteralIntegerValue value,
            State state)
        {
            return value.getValue();
        }

        @Override
        public Integer visit(
            AstLiteralLongValue value,
            State state)
        {
            return -1;
        }

        @Override
        public Integer visit(
            AstLiteralURIValue value,
            State state)
        {
            return -1;
        }
    }
}
