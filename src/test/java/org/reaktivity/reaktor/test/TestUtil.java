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
package org.reaktivity.reaktor.test;

import java.lang.reflect.Field;
import org.junit.internal.runners.statements.InvokeMethod;
import org.junit.rules.MethodRule;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.Statement;

public final class TestUtil
{

    public static TestRule toTestRule(final MethodRule in)
    {
        return new TestRule()
        {

            @Override
            public Statement apply(Statement base, Description description)
            {
                if (base instanceof InvokeMethod)
                {
                    return doApplyInvokeMethod(in, base, (InvokeMethod) base);
                }

                return in.apply(base, null, description);
            }

            private Statement doApplyInvokeMethod(MethodRule in, Statement base, InvokeMethod invokeMethod)
            {
                try
                {
                    FrameworkMethod frameworkMethod = (FrameworkMethod) FIELD_FRAMEWORK_METHOD.get(invokeMethod);
                    Object target = FIELD_TARGET.get(invokeMethod);

                    return in.apply(base, frameworkMethod, target);
                }
                catch (IllegalArgumentException ex)
                {
                    throw new RuntimeException(ex);
                }
                catch (IllegalAccessException ex)
                {
                    throw new RuntimeException(ex);
                }
            }

        };
    }

    private TestUtil()
    {

    }

    private static final Field FIELD_TARGET;
    private static final Field FIELD_FRAMEWORK_METHOD;

    static
    {
        try
        {
            final Field target = InvokeMethod.class.getDeclaredField("target");
            final Field frameworkMethod = InvokeMethod.class.getDeclaredField("testMethod");

            target.setAccessible(true);
            frameworkMethod.setAccessible(true);

            FIELD_TARGET = target;
            FIELD_FRAMEWORK_METHOD = frameworkMethod;
        }
        catch (NoSuchFieldException ex)
        {
            throw new RuntimeException(ex);
        }
        catch (SecurityException ex)
        {
            throw new RuntimeException(ex);
        }
    }
}
