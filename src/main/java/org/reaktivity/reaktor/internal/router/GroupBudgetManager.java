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
package org.reaktivity.reaktor.internal.router;

import org.agrona.collections.Long2LongHashMap;

import java.util.function.IntUnaryOperator;

public class GroupBudgetManager
{
    private static final IntUnaryOperator NOOP_CLAIM = IntUnaryOperator.identity();
    private static final IntUnaryOperator NOOP_RELEASE = groupId -> Integer.MAX_VALUE;

    private final Long2LongHashMap budgets;

    public GroupBudgetManager()
    {
        budgets = new Long2LongHashMap(0L);
    }

    public IntUnaryOperator claim(
        long groupId)
    {
        return groupId == 0
            ? NOOP_CLAIM :
            bytes -> doClaim(groupId, bytes);
    }

    public IntUnaryOperator release(
        long groupId)
    {
        return groupId == 0
                ? NOOP_RELEASE :
                bytes -> doRelease(groupId, bytes);
    }

    private int doClaim(
        long groupId,
        long bytes)
    {
        long budget = budgets.get(groupId);
        long claimed = Math.min(budget, bytes);
        budgets.computeIfPresent(groupId, (k, v) -> budget - claimed);
        return (int) claimed;
    }

    private int doRelease(
        long groupId,
        long bytes)
    {
        long budget = budgets.get(groupId);
        long newBudget = budget + bytes;
        budgets.computeIfPresent(groupId, (k, v) -> newBudget);
        return (int) newBudget;
    }
}
