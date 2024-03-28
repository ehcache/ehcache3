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

package org.ehcache.impl.internal.store.shared.caching.higher;

import org.ehcache.core.spi.store.Store.ValueHolder;
import org.ehcache.core.spi.store.tiering.HigherCachingTier;
import org.ehcache.impl.internal.store.shared.composites.CompositeValue;
import org.ehcache.impl.internal.store.shared.caching.CachingTierPartition;
import org.ehcache.spi.resilience.StoreAccessException;

import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

public class HigherCachingTierPartition<K, V> extends CachingTierPartition<K, V> implements HigherCachingTier<K, V> {

  public HigherCachingTierPartition(int id, HigherCachingTier<CompositeValue<K>, CompositeValue<V>> store, Map<Integer, InvalidationListener<?, ?>> invalidationListenerMap) {
    super(id, store, invalidationListenerMap);
  }

  @Override
  protected HigherCachingTier<CompositeValue<K>, CompositeValue<V>> shared() {
    return (HigherCachingTier<CompositeValue<K>, CompositeValue<V>>) super.shared();
  }

  @Override
  public void silentInvalidate(K key, Function<ValueHolder<V>, Void> function) throws StoreAccessException {
    shared().silentInvalidate(composite(key), v -> function.apply(decode(v)));
  }

  @Override
  public void silentInvalidateAll(BiFunction<K, ValueHolder<V>, Void> biFunction) throws StoreAccessException {
    shared().silentInvalidateAll((k, v) -> biFunction.apply(k.getValue(), decode(v)));
  }

  @Override
  public void silentInvalidateAllWithHash(long hash, BiFunction<K, ValueHolder<V>, Void> biFunction) throws StoreAccessException {
    shared().silentInvalidateAllWithHash(hash, (k, v) -> biFunction.apply(k.getValue(), decode(v)));
  }
}
