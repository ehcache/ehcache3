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
package org.ehcache.internal.store.heap.bytesized;

import static org.ehcache.config.ResourcePoolsBuilder.newResourcePoolsBuilder;

import java.io.Serializable;

import org.ehcache.CacheConfigurationChangeEvent;
import org.ehcache.CacheConfigurationChangeListener;
import org.ehcache.CacheConfigurationProperty;
import org.ehcache.config.EvictionVeto;
import org.ehcache.config.ResourcePools;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.expiry.Expiry;
import org.ehcache.internal.TimeSource;
import org.ehcache.internal.serialization.JavaSerializer;
import org.ehcache.internal.sizeof.DefaultSizeOfEngine;
import org.ehcache.internal.store.heap.OnHeapStore;
import org.ehcache.internal.store.heap.OnHeapStoreByValueTest;
import org.ehcache.spi.cache.Store;
import org.ehcache.spi.copy.Copier;
import org.ehcache.spi.serialization.Serializer;

public class ByteSizedOnHeapStoreByValueTest extends OnHeapStoreByValueTest {
  
  private static final int MAGIC_NUM = 500;

  @Override
  protected void updateStoreCapacity(OnHeapStore<?, ?> store, int newCapacity) {
    CacheConfigurationChangeListener listener = store.getConfigurationChangeListeners().get(0);
    listener.cacheConfigurationChange(new CacheConfigurationChangeEvent(CacheConfigurationProperty.UPDATESIZE,
        newResourcePoolsBuilder().heap(100, MemoryUnit.KB).build(),
        newResourcePoolsBuilder().heap(newCapacity * MAGIC_NUM, MemoryUnit.KB).build()));
  }

  @Override
  protected <K, V> OnHeapStore<K, V> newStore(final TimeSource timeSource,
      final Expiry<? super K, ? super V> expiry,
      final EvictionVeto<? super K, ? super V> veto, final Copier<K> keyCopier,
      final Copier<V> valueCopier, final int capacity) {
    return new OnHeapStore<K, V>(new Store.Configuration<K, V>() {

      @SuppressWarnings("unchecked")
      @Override
      public Class<K> getKeyType() {
        return (Class<K>) Serializable.class;
      }

      @SuppressWarnings("unchecked")
      @Override
      public Class<V> getValueType() {
        return (Class<V>) Serializable.class;
      }

      @Override
      public EvictionVeto<? super K, ? super V> getEvictionVeto() {
        return veto;
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
        return newResourcePoolsBuilder().heap(capacity, MemoryUnit.KB).build();
      }

      @Override
      public Serializer<K> getKeySerializer() {
        return new JavaSerializer<K>(getClass().getClassLoader());
      }

      @Override
      public Serializer<V> getValueSerializer() {
        return new JavaSerializer<V>(getClass().getClassLoader());
      }
    }, timeSource, keyCopier, valueCopier, new DefaultSizeOfEngine(Long.MAX_VALUE, Long.MAX_VALUE, true));
  }

}
