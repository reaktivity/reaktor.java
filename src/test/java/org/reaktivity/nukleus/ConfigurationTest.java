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

import static org.junit.Assert.assertEquals;
import static org.reaktivity.nukleus.Configuration.COMMAND_BUFFER_CAPACITY_PROPERTY_NAME;
import static org.reaktivity.nukleus.Configuration.DIRECTORY_PROPERTY_NAME;

import java.util.Properties;

import org.junit.Test;

public final class ConfigurationTest
{
    @Test
    public void shouldUseSystemProperties()
    {
        System.setProperty(DIRECTORY_PROPERTY_NAME, "/path/to/reaktivity");

        Configuration config = new Configuration();

        assertEquals("/path/to/reaktivity", config.directory().toString());
    }

    @Test
    public void shouldUseProperties()
    {
        Properties properties = new Properties();
        properties.setProperty(DIRECTORY_PROPERTY_NAME, "/path/to/reaktivity");

        Configuration config = new Configuration(properties);

        assertEquals("/path/to/reaktivity", config.directory().toString());
    }

    @Test
    public void shouldUseDefaultOverridesProperties()
    {
        Properties defaultOverrides = new Properties();
        defaultOverrides.setProperty(DIRECTORY_PROPERTY_NAME, "/path/to/reaktivity");

        Configuration system = new Configuration();
        Configuration config = new Configuration(system, defaultOverrides);

        assertEquals("/path/to/reaktivity", config.directory().toString());
    }

    @Test
    public void shouldUseSystemPropertiesBeforeDefaultProperties()
    {
        System.setProperty(DIRECTORY_PROPERTY_NAME, "/system/path/to/reaktivity");

        Properties defaults = new Properties();
        defaults.setProperty(DIRECTORY_PROPERTY_NAME, "/path/to/reaktivity");

        Configuration system = new Configuration();
        Configuration config = new Configuration(system, defaults);

        assertEquals("/system/path/to/reaktivity", config.directory().toString());
    }

    @Test
    public void shouldUseDefaultProperties()
    {
        System.setProperty(COMMAND_BUFFER_CAPACITY_PROPERTY_NAME, Integer.toString(1048576));

        Properties defaults = new Properties();
        defaults.setProperty(COMMAND_BUFFER_CAPACITY_PROPERTY_NAME, Integer.toString(65536));

        Configuration system = new Configuration();
        Configuration config = new Configuration(system, defaults);

        assertEquals(".", config.directory().toString());
    }

    @Test
    public void shouldUseDefaultOverridesPropertiesWhenWrapped()
    {
        Properties defaultOverrides = new Properties();
        defaultOverrides.setProperty(DIRECTORY_PROPERTY_NAME, "/path/to/reaktivity");

        Configuration system = new Configuration();
        Configuration config = new Configuration(system, defaultOverrides);
        Configuration wrapped = new Configuration(config);

        assertEquals("/path/to/reaktivity", wrapped.directory().toString());
    }

    @Test
    public void shouldGetIntegerProperty()
    {
        System.setProperty("integer.property.name", Integer.toString(1234));

        Configuration config = new Configuration();

        assertEquals(1234, config.getInteger("integer.property.name", 5678));
    }

    @Test
    public void shouldDefaultIntegerProperty()
    {
        Configuration config = new Configuration();

        assertEquals(1234, config.getInteger("integer.property.name", 1234));
    }

    @Test
    public void shouldGetBooleanProperty()
    {
        System.setProperty("boolean.property.name", Boolean.TRUE.toString());

        Configuration config = new Configuration();

        assertEquals(Boolean.TRUE, config.getBoolean("boolean.property.name", Boolean.FALSE));
    }

    @Test
    public void shouldDefaultBooleanProperty()
    {
        Configuration config = new Configuration();

        assertEquals(Boolean.TRUE, config.getBoolean("boolean.property.name", Boolean.TRUE));
    }
}
