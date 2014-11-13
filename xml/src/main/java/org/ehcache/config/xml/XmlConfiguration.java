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
import java.util.HashMap;
import java.util.Map;

/**
 * @author cdennis
 */
public class XmlConfiguration {

  private static final URL CORE_SCHEMA_URL = XmlConfiguration.class.getResource("/ehcache-core.xsd");

  private final Map<String, ConfigurationParser.CacheTemplate> templates = new HashMap<String, ConfigurationParser.CacheTemplate>();
  private final URL xml;
  private final ClassLoader classLoader;
  private final Map<String, ClassLoader> cacheClassLoaders;

<<<<<<< HEAD
=======
  private boolean parsedXML = false;

  /**
   * Constructs an instance of XmlConfiguration mapping to the XML file located at {@code url}
   * <p>
   * This constructor will not parse nor even touch the configuration URL.
   *
   * @param url URL pointing to the XML file's location
   * @see #parseConfiguration()
   */
>>>>>>> af6cde1... Stateful
  public XmlConfiguration(URL url) {
    this(url, null);
  }

  public XmlConfiguration(URL url, final ClassLoader classLoader) {
    this(url, classLoader, Collections.<String, ClassLoader>emptyMap());
  }

  public XmlConfiguration(URL url, final ClassLoader classLoader, final Map<String, ClassLoader> cacheClassLoaders) {
    this.xml = url;
    this.classLoader = classLoader;
    this.cacheClassLoaders = new HashMap<String, ClassLoader>(cacheClassLoaders);
  }

  public URL getURL() {
    return xml;
  }

  public Configuration parseConfiguration()
      throws ClassNotFoundException, IOException, SAXException, InstantiationException, IllegalAccessException {
    templates.clear();
    ConfigurationParser configurationParser = new ConfigurationParser(xml.toExternalForm(), CORE_SCHEMA_URL);
    ConfigurationBuilder configBuilder = new ConfigurationBuilder();

    if (classLoader != null) {
      configBuilder = configBuilder.withClassLoader(classLoader);
    }

    for (ServiceConfiguration serviceConfiguration : configurationParser.getServiceConfigurations()) {
      configBuilder = configBuilder.addService(serviceConfiguration);
    }

    for (ConfigurationParser.CacheDefinition cacheDefinition : configurationParser.getCacheElements()) {
      CacheConfigurationBuilder<Object, Object> builder = new CacheConfigurationBuilder<Object, Object>();
      String alias = cacheDefinition.id();

      ClassLoader cacheClassLoader = cacheClassLoaders.get(alias);
      if (cacheClassLoader != null) {
        builder = builder.withClassLoader(cacheClassLoader);
      }
      
      if (cacheClassLoader == null) {
        if (classLoader != null) {
          cacheClassLoader = classLoader;
        } else {
          cacheClassLoader = ClassLoading.getDefaultClassLoader();
        }
      }
      
      Class keyType = getClassForName(cacheDefinition.keyType(), cacheClassLoader);
      Class valueType = getClassForName(cacheDefinition.valueType(), cacheClassLoader);
      Long capacityConstraint = cacheDefinition.capacityConstraint();
      EvictionVeto evictionVeto = getInstanceOfName(cacheDefinition.evictionVeto(), cacheClassLoader, EvictionVeto.class);
      EvictionPrioritizer evictionPrioritizer;
      try {
        evictionPrioritizer = Eviction.Prioritizer.valueOf(cacheDefinition.evictionPrioritizer());
      } catch (IllegalArgumentException e) {
        evictionPrioritizer = getInstanceOfName(cacheDefinition.evictionPrioritizer(), cacheClassLoader, EvictionPrioritizer.class);
      } catch (NullPointerException e) {
        evictionPrioritizer = null;
      }
      for (ServiceConfiguration<?> serviceConfig : cacheDefinition.serviceConfigs()) {
        builder = builder.addServiceConfig(serviceConfig);
      }
      if (capacityConstraint != null) {
        builder = builder.maxEntriesInCache(capacityConstraint);
      }
      configBuilder = configBuilder.addCache(alias, builder.buildConfig(keyType, valueType, evictionVeto, evictionPrioritizer));
    }

    templates.putAll(configurationParser.getTemplates());
    parsedXML = true;
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

  public CacheConfigurationBuilder<Object, Object> newCacheConfigurationBuilderFromTemplate(final String name) throws InstantiationException, IllegalAccessException, ClassNotFoundException {
    return newCacheConfigurationBuilderFromTemplate(name, null, null);
  }

  @SuppressWarnings("unchecked")
  public <K, V> CacheConfigurationBuilder<K, V> newCacheConfigurationBuilderFromTemplate(final String name,
                                                                                         final Class<K> keyType,
                                                                                         final Class<V> valueType)
      throws InstantiationException, IllegalAccessException, ClassNotFoundException {

    if(!parsedXML) {
      throw new IllegalStateException("XmlConfiguration.parseConfiguration has yet been (successfully?) invoked");
    }

    final ConfigurationParser.CacheTemplate cacheTemplate = templates.get(name);
    if (cacheTemplate == null) {
      return null;
    }

    if(keyType != null && cacheTemplate.keyType() != null && !keyType.getName().equals(cacheTemplate.keyType())) {
      throw new IllegalArgumentException("CacheTemplate '" + name + "' declares key type of " + cacheTemplate.keyType());
    }

    if(valueType != null && cacheTemplate.valueType() != null && !valueType.getName().equals(cacheTemplate.valueType())) {
      throw new IllegalArgumentException("CacheTemplate '" + name + "' declares value type of " + cacheTemplate.valueType());
    }

    CacheConfigurationBuilder<K, V> builder = new CacheConfigurationBuilder<K, V>();
    if (cacheTemplate.capacityConstraint() != null) {
      builder = builder
          .maxEntriesInCache(cacheTemplate.capacityConstraint());
    }
    builder = builder
        .maxEntriesInCache(cacheTemplate.capacityConstraint())
        .usingEvictionPrioritizer(getInstanceOfName(cacheTemplate.evictionPrioritizer(), ClassLoading.getDefaultClassLoader(), EvictionPrioritizer.class))
        .evitionVeto(getInstanceOfName(cacheTemplate.evictionVeto(), ClassLoading.getDefaultClassLoader(), EvictionVeto.class));
    for (ServiceConfiguration<?> serviceConfiguration : cacheTemplate.serviceConfigs()) {
      builder = builder.addServiceConfig(serviceConfiguration);
    }
    return builder;
  }
}
