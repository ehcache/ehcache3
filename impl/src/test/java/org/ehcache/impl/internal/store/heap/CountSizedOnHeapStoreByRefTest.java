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
package org.ehcache.impl.internal.store.heap;

import org.ehcache.core.CacheConfigurationChangeEvent;
import org.ehcache.core.CacheConfigurationChangeListener;
import org.ehcache.core.CacheConfigurationProperty;
import org.ehcache.config.EvictionAdvisor;
import org.ehcache.config.ResourcePools;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.expiry.Expiry;
import org.ehcache.impl.copy.IdentityCopier;
import org.ehcache.impl.internal.sizeof.NoopSizeOfEngine;
import org.ehcache.core.spi.time.TimeSource;
import org.ehcache.core.spi.store.Store;
import org.ehcache.spi.copy.Copier;
import org.ehcache.spi.serialization.Serializer;

import static org.ehcache.config.builders.ResourcePoolsBuilder.newResourcePoolsBuilder;

public class CountSizedOnHeapStoreByRefTest extends OnHeapStoreByRefTest {

  private static final Copier DEFAULT_COPIER = new IdentityCopier();

  @Override
  protected void updateStoreCapacity(OnHeapStore<?, ?> store, int newCapacity) {
    CacheConfigurationChangeListener listener = store.getConfigurationChangeListeners().get(0);
    listener.cacheConfigurationChange(new CacheConfigurationChangeEvent(CacheConfigurationProperty.UPDATE_SIZE,
        newResourcePoolsBuilder().heap(100, EntryUnit.ENTRIES).build(),
        newResourcePoolsBuilder().heap(newCapacity, EntryUnit.ENTRIES).build()));
  }

  @Override
  protected <K, V> OnHeapStore<K, V> newStore(final TimeSource timeSource,
      final Expiry<? super K, ? super V> expiry,
      final EvictionAdvisor<? super K, ? super V> evictionAdvisor, final int capacity) {

    return new OnHeapStore<K, V>(new Store.Configuration<K, V>() {
      @SuppressWarnings("unchecked")
      @Override
      public Class<K> getKeyType() {
        return (Class<K>) String.class;
      }

      @SuppressWarnings("unchecked")
      @Override
      public Class<V> getValueType() {
        return (Class<V>) String.class;
      }

      @Override
      public EvictionAdvisor<? super K, ? super V> getEvictionAdvisor() {
        return evictionAdvisor;
      }

      @Override
      public ClassLoader getClassLoader() {
        return getClass().getClassLoader();
      }

      @Override
      public Expiry<? super K, ? super V> getExpiry() {
        return expiry;
      }

      @Override
      public ResourcePools getResourcePools() {
        return newResourcePoolsBuilder().heap(capacity, EntryUnit.ENTRIES).build();
      }

      @Override
      public Serializer<K> getKeySerializer() {
        throw new AssertionError("By-ref heap store using serializers!");
      }

      @Override
      public Serializer<V> getValueSerializer() {
        throw new AssertionError("By-ref heap store using serializers!");
      }

      @Override
      public int getDispatcherConcurrency() {
        return 0;
      }
    }, timeSource, DEFAULT_COPIER, DEFAULT_COPIER, new NoopSizeOfEngine(), eventDispatcher);
  }

}
