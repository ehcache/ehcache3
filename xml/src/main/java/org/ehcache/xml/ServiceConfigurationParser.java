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

package org.ehcache.xml;

import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.xml.model.CacheTemplate;
import org.ehcache.xml.model.CacheType;
import org.ehcache.xml.service.DefaultCacheEventDispatcherConfigurationParser;
import org.ehcache.xml.service.DefaultCacheEventListenerConfigurationParser;
import org.ehcache.xml.service.DefaultCacheLoaderWriterConfigurationParser;
import org.ehcache.xml.service.DefaultCopierConfigurationParser;
import org.ehcache.xml.service.DefaultResilienceStrategyConfigurationParser;
import org.ehcache.xml.service.DefaultSerializerConfigurationParser;
import org.ehcache.xml.service.DefaultSizeOfEngineConfigurationParser;
import org.ehcache.xml.service.DefaultWriteBehindConfigurationParser;
import org.ehcache.xml.service.OffHeapDiskStoreConfigurationParser;
import org.w3c.dom.Element;

import java.net.URI;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

public class ServiceConfigurationParser {

  static final Collection<CoreServiceConfigurationParser> CORE_SERVICE_CONFIGURATION_PARSERS = asList(
    new DefaultSerializerConfigurationParser(),
    new DefaultCopierConfigurationParser(),
    new DefaultCacheLoaderWriterConfigurationParser(),
    new DefaultResilienceStrategyConfigurationParser(),
    new DefaultSizeOfEngineConfigurationParser(),
    new DefaultWriteBehindConfigurationParser(),
    new OffHeapDiskStoreConfigurationParser(),
    new DefaultCacheEventDispatcherConfigurationParser(),
    new DefaultCacheEventListenerConfigurationParser()
  );

  private final Map<Class<?>, CacheServiceConfigurationParser<?>> extensionParsers;

  public ServiceConfigurationParser(Map<Class<?>, CacheServiceConfigurationParser<?>> extensionParsers) {
    this.extensionParsers = extensionParsers;
  }

  public <K, V> CacheConfigurationBuilder<K, V> parseConfiguration(CacheTemplate cacheDefinition, ClassLoader cacheClassLoader,
                                                                   CacheConfigurationBuilder<K, V> cacheBuilder) throws ClassNotFoundException, IllegalAccessException, InstantiationException {
    for (CoreServiceConfigurationParser coreServiceConfigParser : CORE_SERVICE_CONFIGURATION_PARSERS) {
      cacheBuilder = coreServiceConfigParser.parseServiceConfiguration(cacheDefinition, cacheClassLoader, cacheBuilder);
    }

    Map<URI, CacheServiceConfigurationParser<?>> parsers = extensionParsers.values().stream().
      collect(toMap(CacheServiceConfigurationParser::getNamespace, identity()));
    for (Element element : cacheDefinition.serviceConfigExtensions()) {
      URI namespace = URI.create(element.getNamespaceURI());
      final CacheServiceConfigurationParser<?> xmlConfigurationParser = parsers.get(namespace);
      if(xmlConfigurationParser == null) {
        throw new IllegalArgumentException("Can't find parser for namespace: " + namespace);
      }
      cacheBuilder = cacheBuilder.add(xmlConfigurationParser.parseServiceConfiguration(element, cacheClassLoader));
    }

    return cacheBuilder;
  }

  CacheType unparseServiceConfiguration(CacheConfiguration<?, ?> cacheConfiguration, CacheType cacheType) {
    for (CoreServiceConfigurationParser parser : CORE_SERVICE_CONFIGURATION_PARSERS) {
      parser.unparseServiceConfiguration(cacheConfiguration, cacheType);
    }

    List<Element> serviceConfigs = cacheType.getServiceConfiguration();
    cacheConfiguration.getServiceConfigurations().forEach(config -> {
      @SuppressWarnings("rawtypes")
      CacheServiceConfigurationParser parser = extensionParsers.get(config.getServiceType());
      if (parser != null) {
        @SuppressWarnings("unchecked")
        Element element = parser.unparseServiceConfiguration(config);
        serviceConfigs.add(element);
      }
    });

    return cacheType;
  }
}
