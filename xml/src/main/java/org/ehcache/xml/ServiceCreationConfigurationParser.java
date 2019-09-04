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

import org.ehcache.config.Configuration;
import org.ehcache.config.FluentConfigurationBuilder;
import org.ehcache.spi.service.ServiceCreationConfiguration;
import org.ehcache.xml.model.ConfigType;
import org.ehcache.xml.model.ServiceType;
import org.ehcache.xml.provider.CacheEventDispatcherFactoryConfigurationParser;
import org.ehcache.xml.provider.CacheManagerPersistenceConfigurationParser;
import org.ehcache.xml.provider.DefaultCopyProviderConfigurationParser;
import org.ehcache.xml.provider.DefaultSerializationProviderConfigurationParser;
import org.ehcache.xml.provider.DefaultSizeOfEngineProviderConfigurationParser;
import org.ehcache.xml.provider.OffHeapDiskStoreProviderConfigurationParser;
import org.ehcache.xml.provider.PooledExecutionServiceConfigurationParser;
import org.ehcache.xml.provider.WriteBehindProviderConfigurationParser;
import org.w3c.dom.Element;

import java.net.URI;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toMap;
import static java.util.function.Function.identity;

public class ServiceCreationConfigurationParser {

  static final Collection<CoreServiceCreationConfigurationParser> CORE_SERVICE_CREATION_CONFIGURATION_PARSERS = asList(
    new DefaultCopyProviderConfigurationParser(),
    new DefaultSerializationProviderConfigurationParser(),
    new OffHeapDiskStoreProviderConfigurationParser(),
    new CacheEventDispatcherFactoryConfigurationParser(),
    new DefaultSizeOfEngineProviderConfigurationParser(),
    new CacheManagerPersistenceConfigurationParser(),
    new PooledExecutionServiceConfigurationParser(),
    new WriteBehindProviderConfigurationParser()
  );

  private final Map<Class<?>, CacheManagerServiceConfigurationParser<?>> extensionParsers;

  public ServiceCreationConfigurationParser(Map<Class<?>, CacheManagerServiceConfigurationParser<?>> extensionParsers) {
    this.extensionParsers = extensionParsers;
  }

  FluentConfigurationBuilder<?> parseServiceCreationConfiguration(ConfigType configRoot, ClassLoader classLoader, FluentConfigurationBuilder<?> managerBuilder) throws ClassNotFoundException {
    for (CoreServiceCreationConfigurationParser parser : CORE_SERVICE_CREATION_CONFIGURATION_PARSERS) {
      managerBuilder = parser.parseServiceCreationConfiguration(configRoot, classLoader, managerBuilder);
    }

    Map<URI, CacheManagerServiceConfigurationParser<?>> parsers = extensionParsers.values().stream().
     collect(toMap(CacheManagerServiceConfigurationParser::getNamespace, identity()));
    for (ServiceType serviceType : configRoot.getService()) {
      Element element = serviceType.getServiceCreationConfiguration();
      URI namespace = URI.create(element.getNamespaceURI());
      CacheManagerServiceConfigurationParser<?> cacheManagerServiceConfigurationParser = parsers.get(namespace);
      if(cacheManagerServiceConfigurationParser == null) {
        throw new IllegalArgumentException("Can't find parser for namespace: " + namespace);
      }
      ServiceCreationConfiguration<?, ?> serviceConfiguration = cacheManagerServiceConfigurationParser.parseServiceCreationConfiguration(element, classLoader);
      managerBuilder = managerBuilder.withService(serviceConfiguration);
    }

    return managerBuilder;
  }


  ConfigType unparseServiceCreationConfiguration(Configuration configuration, ConfigType configType) {
    for (CoreServiceCreationConfigurationParser parser : CORE_SERVICE_CREATION_CONFIGURATION_PARSERS) {
      parser.unparseServiceCreationConfiguration(configuration, configType);
    }

    List<ServiceType> services = configType.getService();
    configuration.getServiceCreationConfigurations().forEach(config -> {
      @SuppressWarnings("rawtypes")
      CacheManagerServiceConfigurationParser parser = extensionParsers.get(config.getServiceType());
      if (parser != null) {
        ServiceType serviceType = new ServiceType();
        @SuppressWarnings("unchecked")
        Element element = parser.unparseServiceCreationConfiguration(config);
        serviceType.setServiceCreationConfiguration(element);
        services.add(serviceType);
      }
    });

    return configType;
  }
}
