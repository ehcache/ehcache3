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
import org.ehcache.config.Eviction;
import org.ehcache.config.EvictionPrioritizer;
import org.ehcache.config.EvictionVeto;
import org.ehcache.spi.service.ServiceConfiguration;
import org.ehcache.util.ClassLoading;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.net.URL;
import java.util.Collections;
import java.util.Map;

/**
 * @author cdennis
 */
public class XmlConfiguration {

  private static final URL CORE_SCHEMA_URL = XmlConfiguration.class.getResource("/ehcache-core.xsd");

  public Configuration parseConfiguration(URL xml) throws ClassNotFoundException, IOException, SAXException, InstantiationException, IllegalAccessException {
    return parseConfiguration(xml, null);
  }
  
  public Configuration parseConfiguration(URL xml, ClassLoader classLoader)
      throws ClassNotFoundException, IOException, SAXException, InstantiationException, IllegalAccessException {
    Map<String, ClassLoader> emptyMap = Collections.emptyMap();
    return parseConfiguration(xml, classLoader, emptyMap);
  }
  
  public Configuration parseConfiguration(URL xml, ClassLoader classLoader, Map<String, ClassLoader> cacheClassLoaders)
      throws ClassNotFoundException, IOException, SAXException, InstantiationException, IllegalAccessException {
    ConfigurationParser configurationParser = new ConfigurationParser(xml.toExternalForm(), CORE_SCHEMA_URL);
    ConfigurationBuilder configBuilder = new ConfigurationBuilder();
    
    if (classLoader != null) {
      configBuilder.withClassLoader(classLoader);
    }

    for (ServiceConfiguration serviceConfiguration : configurationParser.getServiceConfigurations()) {
      configBuilder.addService(serviceConfiguration);
    }

    for (ConfigurationParser.CacheElement cacheElement : configurationParser.getCacheElements()) {
      CacheConfigurationBuilder builder = new CacheConfigurationBuilder();
      String alias = cacheElement.alias();
      
      
      ClassLoader cacheClassLoader = cacheClassLoaders.get(alias);
      if (cacheClassLoader != null) {
        builder.withClassLoader(cacheClassLoader);
      }
      
      if (cacheClassLoader == null) {
        if (classLoader != null) {
          cacheClassLoader = classLoader;
        } else {
          cacheClassLoader = ClassLoading.getDefaultClassLoader();
        }
      }
      
      Class keyType = getClassForName(cacheElement.keyType(), cacheClassLoader);
      Class valueType = getClassForName(cacheElement.valueType(), cacheClassLoader);
      Long capacityConstraint = cacheElement.capacityConstraint();
      EvictionVeto evictionVeto = getInstanceOfName(cacheElement.evictionVeto(), cacheClassLoader, EvictionVeto.class);
      EvictionPrioritizer evictionPrioritizer;
      try {
        evictionPrioritizer = Eviction.Prioritizer.valueOf(cacheElement.evictionPrioritizer());
      } catch (IllegalArgumentException e) {
        evictionPrioritizer = getInstanceOfName(cacheElement.evictionPrioritizer(), cacheClassLoader, EvictionPrioritizer.class);
      } catch (NullPointerException e) {
        evictionPrioritizer = null;
      }
      for (ServiceConfiguration<?> serviceConfig : cacheElement.serviceConfigs()) {
        builder.addServiceConfig(serviceConfig);
      }
      if (capacityConstraint != null) {
        builder.maxEntriesInCache(capacityConstraint);
      }
      configBuilder.addCache(alias, builder.buildConfig(keyType, valueType, evictionVeto, evictionPrioritizer));
    }

    return configBuilder.build();
  }

  private static <T> T getInstanceOfName(String name, ClassLoader classLoader, Class<T> type) throws ClassNotFoundException, InstantiationException, IllegalAccessException {
    Class<?> klazz = getClassForName(name, classLoader, null);
    if (klazz == null) {
      return null;
    } else {
      return klazz.asSubclass(type).newInstance();
    }
  }
  
  private static Class<?> getClassForName(String name, ClassLoader classLoader, Class<?> or) {
    if (name == null) {
      return or;
    } else {
      try {
        return getClassForName(name, classLoader);
      } catch (ClassNotFoundException e) {
        return or;
      }
    }
  }
  
  private static Class<?> getClassForName(String name, ClassLoader classLoader) throws ClassNotFoundException {
    return Class.forName(name, true, classLoader);
  }

}
