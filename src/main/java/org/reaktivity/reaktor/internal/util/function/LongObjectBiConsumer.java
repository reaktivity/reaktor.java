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
package org.reaktivity.reaktor.internal.util.function;

import java.util.Objects;
import java.util.function.BiConsumer;

@FunctionalInterface
public interface LongObjectBiConsumer<T> extends BiConsumer<Long, T>
{
    @Override
    default void accept(Long value, T t)
    {
        this.accept(value.longValue(), t);
    }

    default LongObjectBiConsumer<T> andThen(
        LongObjectBiConsumer<? super T> after)
    {
        Objects.requireNonNull(after);

        return (l, r) ->
        {
            apply(l, r);
            after.apply(l, r);
        };
    }

    void apply(long l, T t);
}
