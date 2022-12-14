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
import org.ehcache.impl.config.copy.DefaultCopierConfiguration;
import org.ehcache.xml.CoreServiceConfigurationParser;
import org.ehcache.xml.exceptions.XmlConfigurationException;
import org.ehcache.xml.model.CacheTemplate;
import org.ehcache.xml.model.CacheType;

import java.util.Collection;

import static org.ehcache.core.spi.service.ServiceUtils.findAmongst;
import static org.ehcache.xml.XmlConfiguration.getClassForName;

public class DefaultCopierConfigurationParser implements CoreServiceConfigurationParser {

  @Override @SuppressWarnings({"unchecked", "rawtypes"})
  public <K, V> CacheConfigurationBuilder<K, V> parseServiceConfiguration(CacheTemplate cacheDefinition, ClassLoader cacheClassLoader,
                                                                          CacheConfigurationBuilder<K, V> cacheBuilder) throws ClassNotFoundException {
    if (cacheDefinition.keyCopier() != null) {
      Class<?> keyCopier = getClassForName(cacheDefinition.keyCopier(), cacheClassLoader);
      cacheBuilder = cacheBuilder.withService(new DefaultCopierConfiguration(keyCopier, DefaultCopierConfiguration.Type.KEY));
    }

    if (cacheDefinition.valueCopier() != null) {
      Class<?> valueCopier = getClassForName(cacheDefinition.valueCopier(), cacheClassLoader);
      cacheBuilder = cacheBuilder.withService(new DefaultCopierConfiguration(valueCopier, DefaultCopierConfiguration.Type.VALUE));
    }

    return cacheBuilder;
  }

  @Override
  @SuppressWarnings("rawtypes")
  public CacheType unparseServiceConfiguration(CacheConfiguration<?, ?> cacheConfiguration, CacheType cacheType) {
    Collection<DefaultCopierConfiguration> copierConfigs =
      findAmongst(DefaultCopierConfiguration.class, cacheConfiguration.getServiceConfigurations());
    for (DefaultCopierConfiguration copierConfig : copierConfigs) {
      if(copierConfig.getInstance() == null) {
        if (copierConfig.getType() == DefaultCopierConfiguration.Type.KEY) {
          cacheType.getKeyType().setCopier(copierConfig.getClazz().getName());
        } else {
          cacheType.getValueType().setCopier(copierConfig.getClazz().getName());
        }
      } else {
        throw new XmlConfigurationException("XML translation for instance based initialization for DefaultCopierConfiguration is not supported");
      }
    }
    return cacheType;
  }
}
