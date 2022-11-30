/*
 * Copyright Terracotta, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.ehcache.xml.provider;

import org.ehcache.impl.config.serializer.DefaultSerializationProviderConfiguration;
import org.ehcache.xml.exceptions.XmlConfigurationException;
import org.ehcache.xml.model.ConfigType;
import org.ehcache.xml.model.SerializerType;

import java.util.List;

import static java.util.stream.Collectors.toList;
import static org.ehcache.xml.XmlConfiguration.getClassForName;

public class DefaultSerializationProviderConfigurationParser
  extends SimpleCoreServiceCreationConfigurationParser<SerializerType, DefaultSerializationProviderConfiguration> {

  @SuppressWarnings("unchecked")
  public DefaultSerializationProviderConfigurationParser() {
    super(DefaultSerializationProviderConfiguration.class,
      ConfigType::getDefaultSerializers, ConfigType::setDefaultSerializers,
      (config, loader) -> {
        DefaultSerializationProviderConfiguration configuration = new DefaultSerializationProviderConfiguration();
        for (SerializerType.Serializer serializer : config.getSerializer()) {
          configuration.addSerializerFor(getClassForName(serializer.getType(), loader), (Class) getClassForName(serializer.getValue(), loader));
        }
        return configuration;
      },
      config -> new SerializerType()
        .withSerializer(config.getDefaultSerializers().entrySet().stream().map(entry -> new SerializerType.Serializer()
          .withType(entry.getKey().getName())
          .withValue(entry.getValue().getName())).collect(toList()))
    );
  }
}
