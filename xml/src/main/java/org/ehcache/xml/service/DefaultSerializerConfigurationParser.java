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

package org.ehcache.xml.service;

import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.impl.config.serializer.DefaultSerializerConfiguration;
import org.ehcache.xml.CoreServiceConfigurationParser;
import org.ehcache.xml.exceptions.XmlConfigurationException;
import org.ehcache.xml.model.CacheTemplate;
import org.ehcache.xml.model.CacheType;

import java.util.Collection;

import static org.ehcache.core.spi.service.ServiceUtils.findAmongst;
import static org.ehcache.xml.XmlConfiguration.getClassForName;

public class DefaultSerializerConfigurationParser implements CoreServiceConfigurationParser {

  @Override @SuppressWarnings({"rawtypes", "unchecked"})
  public <K, V> CacheConfigurationBuilder<K, V> parseServiceConfiguration(CacheTemplate cacheDefinition, ClassLoader cacheClassLoader,
                                                                          CacheConfigurationBuilder<K, V> cacheBuilder) throws ClassNotFoundException {
    if (cacheDefinition.keySerializer() != null) {
      Class keySerializer = getClassForName(cacheDefinition.keySerializer(), cacheClassLoader);
      cacheBuilder = cacheBuilder.withService(new DefaultSerializerConfiguration(keySerializer, DefaultSerializerConfiguration.Type.KEY));
    }

    if (cacheDefinition.valueSerializer() != null) {
      Class valueSerializer = getClassForName(cacheDefinition.valueSerializer(), cacheClassLoader);
      cacheBuilder = cacheBuilder.withService(new DefaultSerializerConfiguration(valueSerializer, DefaultSerializerConfiguration.Type.VALUE));
    }

    return cacheBuilder;
  }

  @Override @SuppressWarnings({"rawtypes", "unchecked"})
  public CacheType unparseServiceConfiguration(CacheConfiguration<?, ?> cacheConfiguration, CacheType cacheType) {
    Collection<DefaultSerializerConfiguration> serializerConfigs =
      findAmongst(DefaultSerializerConfiguration.class, cacheConfiguration.getServiceConfigurations());
    for (DefaultSerializerConfiguration serializerConfig : serializerConfigs) {
      if(serializerConfig.getInstance() == null) {
        if (serializerConfig.getType() == DefaultSerializerConfiguration.Type.KEY) {
          cacheType.getKeyType().setSerializer(serializerConfig.getClazz().getName());
        } else {
          cacheType.getValueType().setSerializer(serializerConfig.getClazz().getName());
        }
      } else {
        throw new XmlConfigurationException("XML translation for instance based initialization for DefaultSerializerConfiguration is not supported");
      }
    }
    return cacheType;
  }
}
