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

package org.ehcache.xml.model;

import org.ehcache.config.ResourcePool;
import org.ehcache.config.ResourceUnit;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.core.config.SizedResourcePoolImpl;
import org.ehcache.spi.service.ServiceConfiguration;
import org.ehcache.xml.CacheServiceConfigurationParser;
import org.ehcache.xml.JaxbHelper;
import org.ehcache.xml.ResourceConfigurationParser;
import org.w3c.dom.Element;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;

import static java.util.Arrays.asList;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static java.util.Optional.ofNullable;
import static java.util.function.Function.identity;

public class CacheSpec implements CacheTemplate {

  protected final List<BaseCacheType> sources;
  private final Map<URI, CacheServiceConfigurationParser<?>> serviceConfigParsers;

  ResourceConfigurationParser resourceConfigurationParser;

  public CacheSpec(Map<URI, CacheServiceConfigurationParser<?>> serviceConfigParsers,
                   Marshaller marshaller, Unmarshaller unmarshaller, BaseCacheType... sources) {
    this.serviceConfigParsers = serviceConfigParsers;
    this.resourceConfigurationParser = new ResourceConfigurationParser(marshaller, unmarshaller);
    this.sources = asList(sources);
  }

  @Override
  public String keyType() {
    return key().map(CacheEntryType::getValue).orElseGet(() -> extract(source -> JaxbHelper.findDefaultValue(source, "keyType")).orElse(null));
  }

  @Override
  public String keySerializer() {
    return key().map(CacheEntryType::getSerializer).orElse(null);
  }

  @Override
  public String keyCopier() {
    return key().map(CacheEntryType::getCopier).orElse(null);
  }

  private Optional<CacheEntryType> key() {
    return extract(BaseCacheType::getKeyType);
  }

  @Override
  public String valueType() {
    return value().map(CacheEntryType::getValue).orElseGet(() -> extract(source -> JaxbHelper.findDefaultValue(source, "keyType")).orElse(null));
  }

  @Override
  public String valueSerializer() {
    return value().map(CacheEntryType::getSerializer).orElse(null);
  }

  @Override
  public String valueCopier() {
    return value().map(CacheEntryType::getCopier).orElse(null);
  }

  private Optional<CacheEntryType> value() {
    return extract(BaseCacheType::getValueType);
  }

  @Override
  public String evictionAdvisor() {
    return extract(BaseCacheType::getEvictionAdvisor).orElse(null);
  }

  @Override
  public Expiry expiry() {
    return extract(BaseCacheType::getExpiry).map(Expiry::new).orElse(null);
  }

  @Override
  public String loaderWriter() {
    return extract(BaseCacheType::getLoaderWriter).map(CacheLoaderWriterType::getClazz).orElse(null);
  }

  @Override
  public String resilienceStrategy() {
    return extract(BaseCacheType::getResilience).orElse(null);
  }

  @Override
  public ListenersConfig listenersConfig() {
    ListenersType base = null;
    ArrayList<ListenersType> additionals = new ArrayList<>();
    for (BaseCacheType source : sources) {
      if (source.getListeners() != null) {
        if (base == null) {
          base = source.getListeners();
        } else {
          additionals.add(source.getListeners());
        }
      }
    }
    return base != null ? new ListenersConfig(base, additionals.toArray(new ListenersType[0])) : null;
  }


  @Override
  public Iterable<? extends ServiceConfiguration<?>> serviceConfigs() {
    return sources.stream().flatMap(s -> s.getServiceConfiguration().stream()).map(this::parseCacheExtension)
      .collect(Collectors.toMap(Object::getClass, identity(), (a, b) -> a)).values();
  }

  ServiceConfiguration<?> parseCacheExtension(final Element element) {
    URI namespace = URI.create(element.getNamespaceURI());
    final CacheServiceConfigurationParser<?> xmlConfigurationParser = serviceConfigParsers.get(namespace);
    if(xmlConfigurationParser == null) {
      throw new IllegalArgumentException("Can't find parser for namespace: " + namespace);
    }
    return xmlConfigurationParser.parseServiceConfiguration(element);
  }

  @Override
  public Collection<ResourcePool> resourcePools() {
    return extract(s ->
      ofNullable(s.getHeap()).<Collection<ResourcePool>>map(h -> singleton(parseResource(h))).orElseGet(() ->
        ofNullable(s.getResources()).map(this::parseResources).orElse(null))).orElse(emptySet());
  }

  private Collection<ResourcePool> parseResources(ResourcesType resources) {
    Collection<ResourcePool> resourcePools = new ArrayList<>();
    for (Element resource : resources.getResource()) {
      resourcePools.add(parseResource(resource));
    }
    return resourcePools;
  }

  private ResourcePool parseResource(Heap resource) {
    ResourceType heapResource = resource.getValue();
    return new SizedResourcePoolImpl<>(org.ehcache.config.ResourceType.Core.HEAP,
      heapResource.getValue().longValue(), parseUnit(heapResource), false);
  }

  private static ResourceUnit parseUnit(ResourceType resourceType) {
    if (resourceType.getUnit().value().equalsIgnoreCase("entries")) {
      return EntryUnit.ENTRIES;
    } else {
      return org.ehcache.config.units.MemoryUnit.valueOf(resourceType.getUnit().value().toUpperCase());
    }
  }

  private ResourcePool parseResource(Element element) {
    return resourceConfigurationParser.parseResourceConfiguration(element);
  }

  @Override
  public CacheLoaderWriterType.WriteBehind writeBehind() {
    return extract(BaseCacheType::getLoaderWriter).map(CacheLoaderWriterType::getWriteBehind).orElse(null);
  }

  @Override
  public DiskStoreSettingsType diskStoreSettings() {
    return extract(BaseCacheType::getDiskStoreSettings).orElse(null);
  }

  @Override
  public SizeOfEngineLimits heapStoreSettings() {
    return extract(BaseCacheType::getHeapStoreSettings).map(SizeOfEngineLimits::new).orElse(null);
  }

  private <T> Optional<T> extract(Function<BaseCacheType, T> extractor) {
    return sources.stream().map(s -> ofNullable(extractor.apply(s))).filter(Optional::isPresent).map(Optional::get).findFirst();
  }
}
