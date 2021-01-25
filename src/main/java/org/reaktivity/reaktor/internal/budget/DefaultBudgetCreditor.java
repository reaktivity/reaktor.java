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
package org.reaktivity.reaktor.internal.budget;

import static java.lang.System.currentTimeMillis;
import static org.reaktivity.reaktor.internal.layouts.BudgetsLayout.budgetIdOffset;
import static org.reaktivity.reaktor.internal.layouts.BudgetsLayout.budgetRemainingOffset;
import static org.reaktivity.reaktor.internal.layouts.BudgetsLayout.budgetWatchersOffset;
import static org.reaktivity.reaktor.internal.router.BudgetId.budgetMask;

import java.util.function.LongSupplier;

import org.agrona.collections.Hashing;
import org.agrona.collections.Long2LongHashMap;
import org.agrona.concurrent.AtomicBuffer;
import org.reaktivity.nukleus.budget.BudgetCreditor;
import org.reaktivity.reaktor.ReaktorConfiguration;
import org.reaktivity.reaktor.internal.layouts.BudgetsLayout;
import org.reaktivity.reaktor.internal.util.function.LongObjectBiConsumer;

public class DefaultBudgetCreditor implements BudgetCreditor, AutoCloseable
{
    public interface BudgetFlusher
    {
        void flush(long traceId, long budgetId, long watchers);
    }

    private final long budgetMask;
    private final BudgetsLayout layout;
    private final AtomicBuffer storage;
    private final int entries;
    private final BudgetFlusher flusher;
    private final LongSupplier supplyBudgetId;
    private final LongObjectBiConsumer<Runnable> executor;
    private final long childCleanupLinger;
    private final Long2LongHashMap budgetIndexById;
    private final Long2LongHashMap parentBudgetIds;

    public DefaultBudgetCreditor(
        int ownerIndex,
        BudgetsLayout layout,
        BudgetFlusher flusher)
    {
        this(ownerIndex, layout, flusher, null, null, 0L);
    }

    public DefaultBudgetCreditor(
        int ownerIndex,
        BudgetsLayout layout,
        BudgetFlusher flusher,
        LongSupplier supplyBudgetId,
        LongObjectBiConsumer<Runnable> executor,
        long childCleanupLinger)
    {
        this.budgetMask = budgetMask(ownerIndex);
        this.layout = layout;
        this.storage = layout.buffer();
        this.entries = layout.entries();
        this.flusher = flusher;
        this.supplyBudgetId = supplyBudgetId;
        this.executor = executor;
        this.childCleanupLinger = childCleanupLinger;
        this.budgetIndexById = new Long2LongHashMap(NO_CREDITOR_INDEX);
        this.parentBudgetIds = new Long2LongHashMap(NO_BUDGET_ID);
    }

    @Override
    public void close() throws Exception
    {
        layout.close();
    }

    @Override
    public long acquire(
        long budgetId)
    {
        assert (budgetId & budgetMask) == budgetMask;

        long budgetIndex = NO_CREDITOR_INDEX;

        final int entriesMask = entries - 1;
        int index = Hashing.hash(budgetId, entriesMask);
        for (int i = 0; i < entries; i++)
        {
            final int budgetIdOffset = budgetIdOffset(index);
            if (storage.compareAndSetLong(budgetIdOffset, 0L, budgetId))
            {
                storage.putLong(budgetRemainingOffset(index), 0L);
                storage.putLong(budgetWatchersOffset(index), 0L);
                budgetIndex = budgetMask | (long) index;
                break;
            }

            assert storage.getLongVolatile(budgetIdOffset) != budgetId;

            index = ++index & entriesMask;
        }

        if (budgetIndex != NO_CREDITOR_INDEX)
        {
            budgetIndexById.put(budgetId, budgetIndex);
        }

        return budgetIndex;
    }

    @Override
    public long credit(
        long traceId,
        long budgetIndex,
        long credit)
    {
        assert (budgetIndex & budgetMask) == budgetMask;
        final int index = (int) (budgetIndex & ~budgetMask);
        final long previous = storage.getAndAddLong(budgetRemainingOffset(index), credit);

        if (ReaktorConfiguration.DEBUG_BUDGETS && credit != 0L)
        {
            final long budgetId = storage.getLongVolatile(budgetIdOffset(index));
            System.out.format("[%d] [0x%016x] [0x%016x] credit %d @ %d => %d\n",
                    System.nanoTime(), traceId, budgetId, credit, previous, previous + credit);
        }

        final long watchers = storage.getLongVolatile(budgetWatchersOffset(index));
        if (watchers != 0)
        {
            final long budgetId = storage.getLong(budgetIdOffset(index));
            flusher.flush(traceId, budgetId, watchers);
        }

        return previous;
    }

    @Override
    public void release(
        long budgetIndex)
    {
        if (ReaktorConfiguration.DEBUG_BUDGETS)
        {
            System.out.format("[%d] release creditor  budgetIndex=%d \n", System.nanoTime(), budgetIndex);
        }

        assert (budgetIndex & budgetMask) == budgetMask;
        final int index = (int) (budgetIndex & ~budgetMask);

        final long budgetId = storage.getAndSetLong(budgetIdOffset(index), 0L);
        storage.putLong(budgetRemainingOffset(index), 0L);
        storage.putLongOrdered(budgetWatchersOffset(index), 0L);

        assert budgetId != 0L;

        budgetIndexById.remove(budgetId, budgetIndex);
    }

    public void creditById(
        long traceId,
        long budgetId,
        long credit)
    {
        final long budgetIndex = budgetIndexById.get(budgetId);

        if (ReaktorConfiguration.DEBUG_BUDGETS)
        {
            System.out.format("[%d] creditById credit=%d budgetId=%d budgetIndex=%d %s \n",
                System.nanoTime(), credit, budgetId, budgetIndex, budgetIndexById.toString());
        }

        if (budgetIndex != NO_CREDITOR_INDEX)
        {
            credit(traceId, budgetIndex, credit);
        }
    }

    @Override
    public long supplyChild(
        long budgetId)
    {
        final long childBudgetId = supplyBudgetId.getAsLong();
        parentBudgetIds.put(childBudgetId, budgetId);

        return childBudgetId;
    }

    public int acquired()
    {
        return budgetIndexById.size();
    }

    public long parentBudgetId(
        long budgetId)
    {
        return parentBudgetIds.get(budgetId);
    }

    @Override
    public void cleanupChild(
        long budgetId)
    {
        if (ReaktorConfiguration.DEBUG_BUDGETS)
        {
            System.out.format("[%d] cleanupChild childBudgetId=%d budgetParentChildRelation=%s \n",
                System.nanoTime(), budgetId, parentBudgetIds.toString());
        }
        executor.apply(currentTimeMillis() + childCleanupLinger, () -> parentBudgetIds.remove(budgetId));
    }

    long available(
        long budgetIndex)
    {
        assert (budgetIndex & budgetMask) == budgetMask;
        final int index = (int) (budgetIndex & ~budgetMask);
        return storage.getLongVolatile(budgetRemainingOffset(index));
    }

    long budgetId(
        long budgetIndex)
    {
        assert (budgetIndex & budgetMask) == budgetMask;
        final int index = (int) (budgetIndex & ~budgetMask);
        return storage.getLongVolatile(budgetIdOffset(index));
    }

    void watchers(
        long budgetIndex,
        long watchers)
    {
        assert (budgetIndex & budgetMask) == budgetMask;
        final int index = (int) (budgetIndex & ~budgetMask);
        storage.putLongVolatile(budgetWatchersOffset(index), watchers);
    }
}
