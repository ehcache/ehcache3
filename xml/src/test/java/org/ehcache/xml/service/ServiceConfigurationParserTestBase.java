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
import org.ehcache.spi.service.ServiceConfiguration;
import org.ehcache.xml.ConfigurationParser;
import org.ehcache.xml.CoreServiceConfigurationParser;
import org.ehcache.xml.model.CacheDefinition;
import org.ehcache.xml.model.ConfigType;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.net.URL;
import java.util.Map;
import java.util.stream.StreamSupport;

import javax.xml.bind.JAXBException;
import javax.xml.parsers.ParserConfigurationException;

import static java.util.Collections.emptyMap;
import static org.ehcache.config.builders.CacheConfigurationBuilder.newCacheConfigurationBuilder;
import static org.ehcache.config.builders.ResourcePoolsBuilder.heap;

public class ServiceConfigurationParserTestBase {

  ClassLoader classLoader = this.getClass().getClassLoader();
  CacheConfigurationBuilder<?, ?> cacheConfigurationBuilder = newCacheConfigurationBuilder(Object.class, Object.class, heap(10));
  CoreServiceConfigurationParser parser;

  public ServiceConfigurationParserTestBase(CoreServiceConfigurationParser parser) {
    this.parser = parser;
  }

  protected CacheConfiguration<?, ?> getCacheDefinitionFrom(String resourcePath, String cacheName) throws SAXException, JAXBException, ParserConfigurationException, IOException, ClassNotFoundException, IllegalAccessException, InstantiationException {
    final URL resource = this.getClass().getResource(resourcePath);
    ConfigurationParser rootParser = new ConfigurationParser();
    ConfigType configType = rootParser.parseXml(resource.toExternalForm());

    CacheDefinition cacheDefinition = StreamSupport.stream(ConfigurationParser.getCacheElements(configType).spliterator(), false)
      .filter(def -> def.id().equals(cacheName)).findAny().get();
    return parser.parseServiceConfiguration(cacheDefinition, classLoader, cacheConfigurationBuilder).build();
  }

  protected CacheConfiguration<?, ?> buildCacheConfigWithServiceConfig(ServiceConfiguration<?> serviceConfig) {
    return cacheConfigurationBuilder.add(serviceConfig).build();
  }
}
