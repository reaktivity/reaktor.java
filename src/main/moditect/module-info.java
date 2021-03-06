/**
 * Copyright 2016-2021 The Reaktivity Project
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
module org.reaktivity.reaktor
{
    exports org.reaktivity.reaktor;
    exports org.reaktivity.reaktor.config;
    exports org.reaktivity.reaktor.ext;
    exports org.reaktivity.reaktor.nukleus;
    exports org.reaktivity.reaktor.nukleus.budget;
    exports org.reaktivity.reaktor.nukleus.buffer;
    exports org.reaktivity.reaktor.nukleus.concurrent;
    exports org.reaktivity.reaktor.nukleus.function;
    exports org.reaktivity.reaktor.nukleus.poller;
    exports org.reaktivity.reaktor.nukleus.stream;
    exports org.reaktivity.reaktor.nukleus.vault;

    requires transitive java.json;
    requires transitive java.json.bind;
    requires transitive org.agrona.core;
    requires jdk.unsupported;
    requires java.net.http;

    uses org.reaktivity.reaktor.config.ConditionAdapterSpi;
    uses org.reaktivity.reaktor.config.OptionsAdapterSpi;
    uses org.reaktivity.reaktor.config.WithAdapterSpi;
    uses org.reaktivity.reaktor.ext.ReaktorExtSpi;
    uses org.reaktivity.reaktor.nukleus.NukleusFactorySpi;
    uses org.reaktivity.reaktor.nukleus.vault.BindingVault;
}
