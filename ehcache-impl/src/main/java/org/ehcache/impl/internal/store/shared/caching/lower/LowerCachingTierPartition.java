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

package org.ehcache.impl.internal.store.shared.caching.lower;

import org.ehcache.Cache;
import org.ehcache.config.ResourceType;
import org.ehcache.core.CacheConfigurationChangeListener;
import org.ehcache.core.spi.store.Store;
import org.ehcache.core.spi.store.Store.ValueHolder;
import org.ehcache.core.spi.store.tiering.CachingTier;
import org.ehcache.core.spi.store.tiering.LowerCachingTier;
import org.ehcache.impl.internal.store.shared.AbstractPartition;
import org.ehcache.impl.internal.store.shared.composites.CompositeValue;
import org.ehcache.impl.store.HashUtils;
import org.ehcache.spi.resilience.StoreAccessException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class LowerCachingTierPartition<K, V> extends AbstractPartition<LowerCachingTier<CompositeValue<K>, CompositeValue<V>>> implements LowerCachingTier<K, V> {

  private static final Logger LOGGER = LoggerFactory.getLogger(LowerCachingTierPartition.class);
    private final Map<Integer, CachingTier.InvalidationListener<?, ?>> invalidationListenerMap;

  public LowerCachingTierPartition(ResourceType<?> type, int id, LowerCachingTier<CompositeValue<K>, CompositeValue<V>> store, Map<Integer, CachingTier.InvalidationListener<?, ?>> invalidationListenerMap) {
    super(type, id, store);
    this.invalidationListenerMap = invalidationListenerMap;
  }

  @Override
  public void clear() throws StoreAccessException {
    Store<CompositeValue<K>, CompositeValue<V>> realStore = (Store<CompositeValue<K>, CompositeValue<V>>) shared();
    Store.Iterator<Cache.Entry<CompositeValue<K>, ValueHolder<CompositeValue<V>>>> iterator = realStore.iterator();
    while (iterator.hasNext()) {
      Cache.Entry<CompositeValue<K>, ValueHolder<CompositeValue<V>>> next = iterator.next();
      if (next.getKey().getStoreId() == id()) {
        realStore.remove(next.getKey());
      }
    }
  }

  @Override
  public void setInvalidationListener(CachingTier.InvalidationListener<K, V> invalidationListener) {
    invalidationListenerMap.put(id(), invalidationListener);
  }

  @Override
  public List<CacheConfigurationChangeListener> getConfigurationChangeListeners() {
    return shared().getConfigurationChangeListeners();
  }

  @Override
  public ValueHolder<V> installMapping(K key, Function<K, ValueHolder<V>> source) throws StoreAccessException {
    return decode(shared().installMapping(composite(key), k -> encode(source.apply(k.getValue()))));
  }

  @Override
  public ValueHolder<V> get(K key) throws StoreAccessException {
    return decode(shared().get(composite(key)));
  }

  @Override
  public ValueHolder<V> getAndRemove(K key) throws StoreAccessException {
    return decode(shared().getAndRemove(composite(key)));
  }

  @Override
  public void invalidate(K key) throws StoreAccessException {
    shared().invalidate(composite(key));
  }

  @SuppressWarnings("unchecked")
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
      LOGGER.error("Could not invalidate one or more cache entries");
    }
  }

  @Override
  public void invalidateAllWithHash(long keyValueHash) throws StoreAccessException {
    shared().invalidateAllWithHash(CompositeValue.compositeHash(id(), HashUtils.longHashToInt(keyValueHash)));
  }
}
