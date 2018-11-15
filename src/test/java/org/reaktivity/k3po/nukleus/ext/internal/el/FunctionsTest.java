/**
 * Copyright 2016-2018 The Reaktivity Project
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
package org.reaktivity.k3po.nukleus.ext.internal.el;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;
import static org.kaazing.k3po.lang.internal.el.ExpressionFactoryUtils.newExpressionFactory;

import javax.el.ExpressionFactory;
import javax.el.ValueExpression;

import org.junit.Test;
import org.kaazing.k3po.lang.internal.el.ExpressionContext;

public final class FunctionsTest
{
    @Test
    public void shouldInvokeNewRouteRef() throws Exception
    {
        ExpressionFactory factory = newExpressionFactory();
        ExpressionContext environment = new ExpressionContext();

        String expressionText = "${nukleus:newRouteRef()}";
        ValueExpression expression = factory.createValueExpression(environment, expressionText, long.class);

        Object routeRef = expression.getValue(environment);

        assertThat(routeRef, instanceOf(Long.class));
        assertNotEquals(0L, Long.class.cast(routeRef).longValue());
    }

    @Test
    public void shouldInvokeNewCorrelationId() throws Exception
    {
        ExpressionFactory factory = newExpressionFactory();
        ExpressionContext environment = new ExpressionContext();

        String expressionText = "${nukleus:newCorrelationId()}";
        ValueExpression expression = factory.createValueExpression(environment, expressionText, long.class);

        Object correlationId = expression.getValue(environment);

        assertThat(correlationId, instanceOf(Long.class));
    }
}
