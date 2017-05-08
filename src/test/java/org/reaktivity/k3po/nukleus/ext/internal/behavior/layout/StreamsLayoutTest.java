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
package org.reaktivity.k3po.nukleus.ext.internal.behavior.layout;

import static org.junit.Assert.assertTrue;

import java.io.File;

import org.agrona.IoUtil;
import org.junit.Test;

public final class StreamsLayoutTest
{

    @Test
    public void shouldUnlockStreamsFile()
    {
        File streams = new File("target\\nukleus-itests\\client\\streams\\server");
        StreamsLayout streamsLayout = new StreamsLayout.Builder()
                .path(streams.toPath())
                .streamsCapacity(8192)
                .throttleCapacity(8192)
                .readonly(false)
                .build();
        streamsLayout.close();
        assertTrue(streams.exists());
        IoUtil.delete(streams, false);
    }

}
