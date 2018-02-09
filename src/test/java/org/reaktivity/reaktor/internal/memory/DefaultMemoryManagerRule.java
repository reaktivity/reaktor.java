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
package org.reaktivity.reaktor.internal.memory;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.nio.file.Path;

import org.agrona.LangUtil;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.reaktivity.nukleus.buffer.MemoryManager;
import org.reaktivity.reaktor.internal.layouts.MemoryLayout;

public class DefaultMemoryManagerRule implements TestRule
{

    final Path outputFile = new File("target/nukleus-itests/memory0").toPath();
    private MemoryLayout layout;
    private DefaultMemoryManager memoryManager;

    @Override
    public Statement apply(
        Statement base,
        Description description)
    {
        final String testMethod = description.getMethodName().replaceAll("\\[.*\\]", "");
        MemoryLayout.Builder mlb = new MemoryLayout.Builder()
                .path(outputFile);
        try
        {
            ConfigureMemoryLayout configures = description.getTestClass()
                    .getDeclaredMethod(testMethod)
                    .getAnnotation(ConfigureMemoryLayout.class);
            mlb.maximumBlockSize(configures.capacity())
               .minimumBlockSize(configures.smallestBlockSize())
               .create(true);
        }
        catch (Exception e)
        {
            LangUtil.rethrowUnchecked(e);
        }
        this.layout = mlb.build();
        this.memoryManager = new DefaultMemoryManager(layout);
        return new Statement()
        {
            @Override
            public void evaluate() throws Throwable
            {
                base.evaluate();
            }
        };
    }

    public MemoryManager memoryManager()
    {
        return this.memoryManager;
    }

    public MemoryLayout layout()
    {
        return layout;
    }

    public void assertReleased()
    {
        assertTrue(memoryManager.released());
    }

    public void assertNotReleased()
    {
        assertFalse(memoryManager.released());
    }
}
