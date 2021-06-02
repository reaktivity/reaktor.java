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
package org.reaktivity.reaktor.nukleus;

import java.net.InetAddress;
import java.net.URL;
import java.nio.channels.SelectableChannel;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;

import org.agrona.MutableDirectBuffer;
import org.reaktivity.reaktor.config.Binding;
import org.reaktivity.reaktor.config.Namespace;
import org.reaktivity.reaktor.nukleus.budget.BudgetCreditor;
import org.reaktivity.reaktor.nukleus.budget.BudgetDebitor;
import org.reaktivity.reaktor.nukleus.buffer.BufferPool;
import org.reaktivity.reaktor.nukleus.concurrent.Signaler;
import org.reaktivity.reaktor.nukleus.function.MessageConsumer;
import org.reaktivity.reaktor.nukleus.poller.PollerKey;
import org.reaktivity.reaktor.nukleus.stream.StreamFactory;
import org.reaktivity.reaktor.nukleus.vault.BindingVault;

public interface ElektronContext
{
    int index();

    Signaler signaler();

    int supplyTypeId(
        String name);

    long supplyInitialId(
        long routeId);

    long supplyReplyId(
        long initialId);

    long supplyBudgetId();

    long supplyTraceId();

    MessageConsumer supplyReceiver(
        long streamId);

    void detachSender(
        long replyId);

    BudgetCreditor creditor();

    BudgetDebitor supplyDebitor(
        long budgetId);

    MutableDirectBuffer writeBuffer();

    BufferPool bufferPool();

    LongSupplier supplyCounter(
        String name);

    LongConsumer supplyAccumulator(
        String name);

    MessageConsumer droppedFrameHandler();

    int supplyRemoteIndex(
        long streamId);

    InetAddress[] resolveHost(
        String host);

    PollerKey supplyPollerKey(
        SelectableChannel channel);

    long supplyRouteId(
        Namespace namespace,
        Binding binding);

    String supplyNamespace(
        long routeId);

    String supplyLocalName(
        long routeId);

    StreamFactory streamFactory();

    BindingVault supplyVault(
        long vaultId);

    URL resolvePath(
        String path);
}
