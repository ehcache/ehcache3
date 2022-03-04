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

import org.ehcache.xml.JaxbHelper;
import org.w3c.dom.Element;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Optional.ofNullable;
import static java.util.function.Function.identity;

public class CacheSpec implements CacheTemplate {

  protected final List<BaseCacheType> sources;
  private final String id;

  public CacheSpec(String id, BaseCacheType... sources) {
    this.id = id;
    this.sources = asList(sources);
  }

  public String id() {
    return id;
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
  public List<Element> getResources() {
    return extract(BaseCacheType::getResources).map(ResourcesType::getResource).orElse(emptyList());
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
  public Collection<Element> serviceConfigExtensions() {
    return sources.stream().flatMap(s -> s.getServiceConfiguration().stream())
      .collect(Collectors.toMap(Element::getTagName, identity(), (a, b) -> a)).values();
  }

  @Override
  public Heap getHeap() {
    return extract(BaseCacheType::getHeap).orElse(null);
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
