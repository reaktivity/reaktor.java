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

import static java.util.stream.Collectors.joining;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLStreamHandler;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;

import org.agrona.LangUtil;
import org.reaktivity.nukleus.NukleusFactorySpi;

public class NukleusClassLoader extends ClassLoader
{
    private final List<URL> urls;

    public NukleusClassLoader(String... factorySpiClassNames)
    {
        final String contents = Arrays.stream(factorySpiClassNames).collect(joining("\n"));
        URI uri = URI.create("data:," + factorySpiClassNames[0]);
        URL url = null;
        try
        {
            url = new URL(null, uri.toString(), new URLStreamHandler()
            {

                @Override
                protected URLConnection openConnection(URL url) throws IOException
                {
                    return new URLConnection(url)
                            {

                                @Override
                                public void connect() throws IOException
                                {
                                    // no-op
                                }

                                @Override
                                public InputStream getInputStream() throws IOException
                                {
                                    return new ByteArrayInputStream(contents.getBytes());
                                }
                            };
                }

            });
        }
        catch(IOException ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }
        urls = Collections.singletonList(url);
    }

    @Override
    public Enumeration<URL> getResources(String name) throws IOException
    {
        return name.equals("META-INF/services/" + NukleusFactorySpi.class.getName()) ?
                    Collections.enumeration(urls)
                    :  super.getResources(name);
    }

}