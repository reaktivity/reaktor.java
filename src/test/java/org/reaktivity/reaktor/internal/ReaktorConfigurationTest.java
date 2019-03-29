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
package org.reaktivity.reaktor.internal;

import static org.junit.Assert.assertEquals;
import static org.reaktivity.reaktor.internal.ReaktorConfiguration.REAKTOR_BUFFER_POOL_CAPACITY;
import static org.reaktivity.reaktor.internal.ReaktorConfiguration.REAKTOR_BUFFER_SLOT_CAPACITY;
import static org.reaktivity.reaktor.test.ReaktorRule.REAKTOR_BUFFER_POOL_CAPACITY_NAME;
import static org.reaktivity.reaktor.test.ReaktorRule.REAKTOR_BUFFER_SLOT_CAPACITY_NAME;

import org.junit.Test;

public class ReaktorConfigurationTest
{
    @Test
    public void shouldVerifyConstants() throws Exception
    {
        assertEquals(REAKTOR_BUFFER_POOL_CAPACITY.name(), REAKTOR_BUFFER_POOL_CAPACITY_NAME);
        assertEquals(REAKTOR_BUFFER_SLOT_CAPACITY.name(), REAKTOR_BUFFER_SLOT_CAPACITY_NAME);
    }
}
