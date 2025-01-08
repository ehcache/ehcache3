/*
 * Copyright Terracotta, Inc.
 * Copyright IBM Corp. 2024, 2025
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
import org.ehcache.xml.service.DefaultWriteBehindConfigurationParser;
import org.ehcache.xml.service.OffHeapDiskStoreConfigurationParser;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import java.net.URI;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toMap;
import static org.ehcache.xml.XmlUtil.findMatchingNodeInDocument;

public class ServiceConfigurationParser {

  @SuppressWarnings("deprecation")
  static final Collection<CoreServiceConfigurationParser<CacheTemplate, CacheType>> CORE_SERVICE_CONFIGURATION_PARSERS = asList(
    new DefaultSerializerConfigurationParser(),
    new DefaultCopierConfigurationParser(),
    new DefaultCacheLoaderWriterConfigurationParser(),
    new DefaultResilienceStrategyConfigurationParser(),
    new org.ehcache.xml.service.DefaultSizeOfEngineConfigurationParser(),
    new DefaultWriteBehindConfigurationParser(),
    new OffHeapDiskStoreConfigurationParser(),
    new DefaultCacheEventDispatcherConfigurationParser(),
    new DefaultCacheEventListenerConfigurationParser()
  );

  private final Map<Class<?>, CacheServiceConfigurationParser<?, ?>> extensionParsers;

  public ServiceConfigurationParser(Map<Class<?>, CacheServiceConfigurationParser<?, ?>> extensionParsers) {
    this.extensionParsers = extensionParsers;
  }

  public <K, V> CacheConfigurationBuilder<K, V> parse(Document document, CacheTemplate cacheDefinition, ClassLoader cacheClassLoader,
                                                      CacheConfigurationBuilder<K, V> cacheBuilder) throws ClassNotFoundException {
    for (CoreServiceConfigurationParser<CacheTemplate, CacheType> coreServiceConfigParser : CORE_SERVICE_CONFIGURATION_PARSERS) {
      cacheBuilder = coreServiceConfigParser.parseServiceConfiguration(cacheDefinition, cacheClassLoader, cacheBuilder);
    }

    Map<URI, ? extends CacheServiceConfigurationParser<?, ?>> parsers = extensionParsers.values().stream()
      .flatMap(parser -> parser.getTargetNamespaces().stream().map(ns -> new AbstractMap.SimpleImmutableEntry<>(ns, parser)))
      .collect(toMap(Map.Entry::getKey, Map.Entry::getValue));

    for (Element element : cacheDefinition.serviceConfigExtensions()) {
      Element typedElement = (Element) findMatchingNodeInDocument(document, element).cloneNode(true);
      URI namespace = URI.create(typedElement.getNamespaceURI());
      final CacheServiceConfigurationParser<?, ?> xmlConfigurationParser = parsers.get(namespace);
      if(xmlConfigurationParser == null) {
        throw new IllegalArgumentException("Can't find parser for namespace: " + namespace);
      }
      cacheBuilder = cacheBuilder.withService(xmlConfigurationParser.parse(typedElement, cacheClassLoader));
    }

    return cacheBuilder;
  }

  CacheType unparse(Document target, CacheConfiguration<?, ?> cacheConfiguration, CacheType cacheType) {
    for (CoreServiceConfigurationParser<CacheTemplate, CacheType> parser : CORE_SERVICE_CONFIGURATION_PARSERS) {
      parser.unparseServiceConfiguration(cacheConfiguration, cacheType);
    }

    List<Element> serviceConfigs = cacheType.getServiceConfiguration();
    cacheConfiguration.getServiceConfigurations().forEach(config -> {
      @SuppressWarnings("rawtypes")
      CacheServiceConfigurationParser parser = extensionParsers.get(config.getServiceType());
      if (parser != null) {
        @SuppressWarnings("unchecked")
        Element element = parser.unparse(target, config);
        serviceConfigs.add(element);
      }
    });

    return cacheType;
  }
}
