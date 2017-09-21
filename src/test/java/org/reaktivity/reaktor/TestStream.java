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
package org.reaktivity.reaktor;

import java.util.function.LongSupplier;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Long2ObjectHashMap;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.function.MessagePredicate;
import org.reaktivity.nukleus.route.RouteManager;
import org.reaktivity.reaktor.internal.types.control.RouteFW;
import org.reaktivity.reaktor.internal.types.stream.BeginFW;
import org.reaktivity.reaktor.internal.types.stream.DataFW;
import org.reaktivity.reaktor.internal.types.stream.EndFW;
import org.reaktivity.reaktor.internal.types.stream.FrameFW;
import org.reaktivity.reaktor.internal.types.stream.ResetFW;

final class TestStream implements MessageConsumer
{
    private final BeginFW beginRO = new BeginFW();
    private final FrameFW frameRO = new FrameFW();
    private final RouteFW routeRO = new RouteFW();

    private final BeginFW.Builder beginRW = new BeginFW.Builder();
    private final ResetFW.Builder resetRW = new ResetFW.Builder();

    private RouteManager router;
    private LongSupplier supplyStreamId;
    private LongSupplier supplyCorrelationId;
    private MutableDirectBuffer writeBuffer;
    private final Long2ObjectHashMap<Accepted> correlations;
    private final MessageConsumer throttle;

    TestStream(
        RouteManager router,
        LongSupplier supplyStreamId,
        LongSupplier supplyCorrelationId,
        MutableDirectBuffer writeBuffer,
        MessageConsumer throttle,
        Long2ObjectHashMap<Accepted> correlations)
    {
        this.router = router;
        this.supplyStreamId = supplyStreamId;
        this.supplyCorrelationId = supplyCorrelationId;
        this.writeBuffer = writeBuffer;
        this.throttle = throttle;
        this.correlations = correlations;
    }

    @Override
    public void accept(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        switch(msgTypeId)
        {
        case BeginFW.TYPE_ID:
            BeginFW begin = beginRO.wrap(buffer, index, index + length);
            processBegin(begin);
            break;
        case DataFW.TYPE_ID:
            break;
        case EndFW.TYPE_ID:
            break;
        default:
            processUnexpected(buffer, index, length);
            break;
        }
    }

    private void processBegin(
        BeginFW begin)
    {
        final long sourceRef = begin.sourceRef();
        if (sourceRef != 0L)
        {
            MessagePredicate filter = (m, b, i, l) ->
            {
                RouteFW route = routeRO.wrap(b, i, i + l);
                final long routeSourceRef = route.sourceRef();
                return sourceRef == routeSourceRef;
            };
            RouteFW route = router.resolve(filter, (m, b, i, l) -> routeRO.wrap(b, i, i + l));
            if (route != null)
            {
                MessageConsumer target = router.supplyTarget(route.target().asString());
                long newConnectId = supplyStreamId.getAsLong();
                long newCorrelationId = supplyCorrelationId.getAsLong();
                correlations.put(newCorrelationId,
                        new Accepted(begin.correlationId(), begin.source().asString()));
                final BeginFW newBegin = beginRW.wrap(writeBuffer,  0, writeBuffer.capacity())
                        .streamId(newConnectId)
                        .source("example")
                        .sourceRef(route.targetRef())
                        .correlationId(newCorrelationId)
                        .build();
                target.accept(BeginFW.TYPE_ID, writeBuffer, newBegin.offset(), newBegin.sizeof());
            }
        }
        else
        {
            Accepted accepted = correlations.get(begin.correlationId());
            if (accepted != null)
            {
                MessageConsumer acceptReply = router.supplyTarget(accepted.acceptName);
                long newReplyId = supplyStreamId.getAsLong();
                final BeginFW beginOut = beginRW.wrap(writeBuffer,  0, writeBuffer.capacity())
                        .streamId(newReplyId)
                        .source("example")
                        .sourceRef(0L)
                        .correlationId(accepted.correlationId)
                        .build();
                acceptReply.accept(BeginFW.TYPE_ID, writeBuffer, beginOut.offset(), beginOut.sizeof());
            }
            else
            {
                processUnexpected(begin.buffer(), begin.offset(), begin.sizeof());
            }
        }
    }

    private void processUnexpected(
        DirectBuffer buffer,
        int index,
        int length)
    {
        FrameFW frame = frameRO.wrap(buffer, index, index + length);
        long streamId = frame.streamId();

        final ResetFW reset = resetRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .streamId(streamId)
                .build();

        throttle.accept(reset.typeId(), reset.buffer(), reset.offset(), reset.sizeof());
    }

    static class Accepted
    {
        long correlationId;
        String acceptName;

        Accepted(long correlationId, String acceptName)
        {
            this.correlationId = correlationId;
            this.acceptName = acceptName;
        }
    }

}
