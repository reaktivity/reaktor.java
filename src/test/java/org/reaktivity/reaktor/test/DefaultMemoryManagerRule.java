package org.reaktivity.reaktor.test;

import java.io.File;
import java.nio.file.Path;

import org.agrona.LangUtil;
import org.junit.Assert;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.reaktivity.nukleus.buffer.MemoryManager;
import org.reaktivity.reaktor.internal.memory.DefaultMemoryManager;
import org.reaktivity.reaktor.internal.memory.MemoryLayout;

public class DefaultMemoryManagerRule implements TestRule
{

    final Path outputFile = new File("target/nukleus-itests/memory").toPath();
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
            mlb.capacity(configures.capacity())
               .smallestBlockSize(configures.smallestBlockSize());
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
        Assert.assertTrue(memoryManager.released());
    }

    public void assertNotReleased()
    {
        Assert.assertTrue(!memoryManager.released());
    }

}
