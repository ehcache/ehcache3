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
package org.ehcache.config.xml;

import org.ehcache.config.CacheConfigurationBuilder;
import org.ehcache.config.Configuration;
import org.ehcache.config.ConfigurationBuilder;
import org.ehcache.spi.service.ServiceConfiguration;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.net.URL;

/**
 * @author cdennis
 */
public class XmlConfiguration {


  private static final URL CORE_SCHEMA_URL = XmlConfiguration.class.getResource("/ehcache-core.xsd");

  public Configuration parseConfiguration(URL xml) throws ClassNotFoundException, IOException, SAXException {
    ConfigurationParser configurationParser = new ConfigurationParser(xml.toExternalForm(), CORE_SCHEMA_URL);
    ConfigurationBuilder configBuilder = new ConfigurationBuilder();

    for (ServiceConfiguration serviceConfiguration : configurationParser.getServiceConfigurations()) {
      configBuilder.addService(serviceConfiguration);
    }

    for (ConfigurationParser.CacheElement cacheElement : configurationParser.getCacheElements()) {
      CacheConfigurationBuilder builder = new CacheConfigurationBuilder();

      Class<?> keyType = getClassForName(cacheElement.keyType());
      Class<?> valueType = getClassForName(cacheElement.valueType());
      Long capacityConstraint = cacheElement.capacityConstraint();
      for (ServiceConfiguration<?> serviceConfig : cacheElement.serviceConfigs()) {
        builder.addServiceConfig(serviceConfig);
      }
      configBuilder.addCache(cacheElement.alias(), builder.buildConfig(keyType, valueType, capacityConstraint));
    }

    return configBuilder.build();
  }

  private static Class<?> getClassForName(String name) throws ClassNotFoundException {
    ClassLoader tccl = Thread.currentThread().getContextClassLoader();
    if (tccl == null) {
      return Class.forName(name);
    } else {
      try {
        return Class.forName(name, true, tccl);
      } catch (ClassNotFoundException e) {
        return Class.forName(name);
      }
    }
  }

}
