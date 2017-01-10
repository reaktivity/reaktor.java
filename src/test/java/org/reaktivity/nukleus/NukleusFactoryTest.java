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
package org.reaktivity.nukleus;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThat;

import java.io.IOException;

import org.junit.Test;

public final class NukleusFactoryTest
{

    @Test
    public void shouldLoadAndCreate() throws IOException
    {
        NukleusFactory factory = NukleusFactory.instantiate();
        Nukleus nukleus = factory.create("test", new Configuration());
        assertThat(nukleus, instanceOf(TestNukleus.class));
    }

    public static final class TestNukleusFactory implements NukleusFactorySpi
    {
        @Override
        public String name()
        {
            return "test";
        }

        @Override
        public Nukleus create(Configuration options)
        {
            return new TestNukleus();
        }
    }

    static final class TestNukleus implements Nukleus
    {
        @Override
        public int process()
        {
            // no-op
            return 0;
        }

        @Override
        public void close() throws Exception
        {
            // no-op
        }
    }
}
