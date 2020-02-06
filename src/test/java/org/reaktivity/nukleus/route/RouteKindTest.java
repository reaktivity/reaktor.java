/**
 * Copyright 2016-2020 The Reaktivity Project
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
package org.reaktivity.nukleus.route;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public final class RouteKindTest
{
    @Test
    public void shouldReturnServerFromValueOf() throws Exception
    {
        assertEquals(RouteKind.SERVER, RouteKind.valueOf(0));
    }

    @Test
    public void shouldReturnClientFromValueOf() throws Exception
    {
        assertEquals(RouteKind.CLIENT, RouteKind.valueOf(1));
    }

    @Test
    public void shouldReturnProxyFromValueOf() throws Exception
    {
        assertEquals(RouteKind.PROXY, RouteKind.valueOf(2));
    }

    @Test
    public void shouldReturnServerReverseFromValueOf() throws Exception
    {
        assertEquals(RouteKind.SERVER_REVERSE, RouteKind.valueOf(3));
    }

    @Test
    public void shouldReturnClientReverseFromValueOf() throws Exception
    {
        assertEquals(RouteKind.CLIENT_REVERSE, RouteKind.valueOf(4));
    }

    @Test
    public void shouldReturnCacheServerFromValueOf() throws Exception
    {
        assertEquals(RouteKind.CACHE_SERVER, RouteKind.valueOf(5));
    }

    @Test
    public void shouldReturnCacheClientFromValueOf() throws Exception
    {
        assertEquals(RouteKind.CACHE_CLIENT, RouteKind.valueOf(6));
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionFromValueOfWithInvalidOrdinal() throws Exception
    {
        assertEquals(RouteKind.SERVER, RouteKind.valueOf(-1));
    }
}
