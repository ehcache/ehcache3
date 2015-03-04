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
package org.ehcache.internal.store.tiering;

import org.ehcache.spi.cache.Store;
import org.ehcache.spi.cache.tiering.CachingTier;

/**
 * A {@link CachingTier.InvalidationListener} implementation that doesn't do anything.
 *
 * @author Ludovic Orban
 */
public class NullInvalidationListener<K, V> implements CachingTier.InvalidationListener<K, V> {

  private static final CachingTier.InvalidationListener<?, ?> INSTANCE = new NullInvalidationListener<Object, Object>();

  @SuppressWarnings("unchecked")
  public static <K, V> CachingTier.InvalidationListener<K, V> instance() {
    return (CachingTier.InvalidationListener<K, V>) INSTANCE;
  }

  @Override
  public void onInvalidation(K key, Store.ValueHolder<V> valueHolder) {
  }
}
