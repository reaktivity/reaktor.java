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
package org.reaktivity.reaktor.internal.security;

import static org.junit.Assert.assertEquals;

import org.jmock.integration.junit4.JUnitRuleMockery;
import org.junit.Rule;
import org.junit.Test;

public class RealmsTest
{
    @Rule
    public JUnitRuleMockery context = new JUnitRuleMockery();

    @Test
    public void shouldAssignAllAvailableNewRealmIds() throws Exception
    {
        Realms realms = new Realms();
        assertEquals(0x0001L, realms.supplyRealmId("realm1"));
        assertEquals(0x0002L, realms.supplyRealmId("realm2"));
        assertEquals(0x0004L, realms.supplyRealmId("realm3"));
        assertEquals(0x0008L, realms.supplyRealmId("realm4"));
        assertEquals(0x0010L, realms.supplyRealmId("realm5"));
        assertEquals(0x0020L, realms.supplyRealmId("realm6"));
        assertEquals(0x0040L, realms.supplyRealmId("realm7"));
        assertEquals(0x0080L, realms.supplyRealmId("realm8"));
        assertEquals(0x0100L, realms.supplyRealmId("realm9"));
        assertEquals(0x0200L, realms.supplyRealmId("realm10"));
        assertEquals(0x0400L, realms.supplyRealmId("realm11"));
        assertEquals(0x0800L, realms.supplyRealmId("realm12"));
        assertEquals(0x1000L, realms.supplyRealmId("realm13"));
        assertEquals(0x2000L, realms.supplyRealmId("realm14"));
        assertEquals(0x4000L, realms.supplyRealmId("realm15"));
        assertEquals(0x8000L, realms.supplyRealmId("realm16"));
    }

    @Test
    public void shouldReportZeroWhenNoMoreAvailableRealmIds() throws Exception
    {
        Realms realms = new Realms();
        for (int i=0; i < 16; i++)
        {
            realms.supplyRealmId("realm" + i);
        }
        assertEquals(0L, realms.supplyRealmId("realmToMany"));
        assertEquals(0L, realms.supplyRealmId("realmToManyAgain"));
    }

    @Test
    public void shouldRetrieveExistingRealmIds() throws Exception
    {
        Realms realms = new Realms();
        assertEquals(0x0001L, realms.supplyRealmId("realm1"));
        assertEquals(0x0002L, realms.supplyRealmId("realm2"));
        assertEquals(0x0004L, realms.supplyRealmId("realm3"));

        assertEquals(0x0001L, realms.supplyRealmId("realm1"));
        assertEquals(0x0002L, realms.supplyRealmId("realm2"));
        assertEquals(0x0004L, realms.supplyRealmId("realm3"));
    }

}
