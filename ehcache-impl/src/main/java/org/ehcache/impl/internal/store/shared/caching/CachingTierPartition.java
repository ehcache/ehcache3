/*
 * Copyright Terracotta, Inc.
 * Copyright Super iPaaS Integration LLC, an IBM Company 2024
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

package org.ehcache.impl.internal.store.shared.caching;

import org.ehcache.Cache;
import org.ehcache.config.ResourceType;
import org.ehcache.core.CacheConfigurationChangeListener;
import org.ehcache.core.EhcachePrefixLoggerFactory;
import org.ehcache.core.spi.store.Store;
import org.ehcache.core.spi.store.Store.ValueHolder;
import org.ehcache.core.spi.store.tiering.CachingTier;
import org.ehcache.impl.internal.store.shared.AbstractPartition;
import org.ehcache.impl.internal.store.shared.composites.CompositeValue;
import org.ehcache.impl.store.HashUtils;
import org.ehcache.spi.resilience.StoreAccessException;
import org.slf4j.Logger;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

public class CachingTierPartition<K, V> extends AbstractPartition<CachingTier<CompositeValue<K>, CompositeValue<V>>> implements CachingTier<K, V> {

  private final Logger logger = EhcachePrefixLoggerFactory.getLogger(CachingTierPartition.class);
  private final Map<Integer, CachingTier.InvalidationListener<?, ?>> invalidationListenerMap;

  public CachingTierPartition(ResourceType<?> type, int id, CachingTier<CompositeValue<K>, CompositeValue<V>> store, Map<Integer, CachingTier.InvalidationListener<?, ?>> invalidationListenerMap) {
    super(type, id, store);
    this.invalidationListenerMap = invalidationListenerMap;
  }

  @Override
  public void clear() throws StoreAccessException {
    boolean completeRemoval = true;
    Store<CompositeValue<K>, CompositeValue<V>> realStore = (Store<CompositeValue<K>, CompositeValue<V>>) shared();
    Store.Iterator<Cache.Entry<CompositeValue<K>, ValueHolder<CompositeValue<V>>>> iterator = realStore.iterator();
    while (iterator.hasNext()) {
      try {
        Cache.Entry<CompositeValue<K>, ValueHolder<CompositeValue<V>>> next = iterator.next();
        if (next.getKey().getStoreId() == id()) {
          realStore.remove(next.getKey());
        }
      } catch (StoreAccessException cae) {
        completeRemoval = false;
      }
    }
    if (!completeRemoval) {
      logger.error("Iteration failures may have prevented a complete removal");
    }
  }

  @Override
  public List<CacheConfigurationChangeListener> getConfigurationChangeListeners() {
    return shared().getConfigurationChangeListeners();
  }

  @Override
  public ValueHolder<V> getOrComputeIfAbsent(K key, Function<K, ValueHolder<V>> source) throws StoreAccessException {
    return decode(shared().getOrComputeIfAbsent(composite(key), k -> encode(source.apply(k.getValue()))));
  }

  @Override
  public ValueHolder<V> getOrDefault(K key, Function<K, ValueHolder<V>> source) throws StoreAccessException {
    return decode(shared().getOrDefault(composite(key), k -> encode(source.apply(k.getValue()))));
  }

  @Override
  public void invalidate(K key) throws StoreAccessException {
    shared().invalidate(composite(key));
  }

  @Override
  public void invalidateAll() throws StoreAccessException {
    Store<CompositeValue<K>, CompositeValue<V>> realStore = (Store<CompositeValue<K>, CompositeValue<V>>) shared();
    boolean invalidate = true;
    Store.Iterator<Cache.Entry<CompositeValue<K>, ValueHolder<CompositeValue<V>>>> iterator = realStore.iterator();
    while (iterator.hasNext()) {
      try {
        Cache.Entry<CompositeValue<K>, ValueHolder<CompositeValue<V>>> next = iterator.next();
        if (next.getKey().getStoreId() == id()) {
          shared().invalidate(next.getKey());
        }
      } catch (StoreAccessException cae) {
        invalidate = false;
      }
    }
    if (!invalidate) {
      logger.error("Could not invalidate one or more cache entries");
    }
  }

  @Override
  public void invalidateAllWithHash(long keyValueHash) throws StoreAccessException {
    shared().invalidateAllWithHash(CompositeValue.compositeHash(id(), HashUtils.longHashToInt(keyValueHash)));
  }

  @Override
  public void setInvalidationListener(CachingTier.InvalidationListener<K, V> invalidationListener) {
    invalidationListenerMap.put(id(), invalidationListener);
  }

  @Override
  public Map<K, ValueHolder<V>> bulkGetOrComputeIfAbsent(Iterable<? extends K> keys, Function<Set<? extends K>, Iterable<? extends Map.Entry<? extends K, ? extends ValueHolder<V>>>> mappingFunction) throws StoreAccessException {
    Map<CompositeValue<K>, ValueHolder<CompositeValue<V>>> results = shared()
      .bulkGetOrComputeIfAbsent(compositeSet(keys), compositeKeys -> {
        Set<K> extractedKeys = new HashSet<>();
        compositeKeys.forEach(k -> extractedKeys.add(k.getValue()));
        Map<CompositeValue<K>, ValueHolder<CompositeValue<V>>> encodedResults = new HashMap<>();
        Iterable<? extends Map.Entry<? extends K, ? extends ValueHolder<V>>> extractedResults = mappingFunction.apply(extractedKeys);
        extractedResults.forEach(entry -> {
          encodedResults.put(composite(entry.getKey()), encode(entry.getValue()));
        });
        return encodedResults.entrySet();
      });

    Map<K, ValueHolder<V>> decodedResults = new HashMap<>();
    results.forEach((k, v) -> decodedResults.put(k.getValue(), decode(v)));
    return decodedResults;
  }
}
