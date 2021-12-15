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
package org.ehcache.impl.internal.store.heap.bytesized;

import org.ehcache.config.EvictionAdvisor;
import org.ehcache.config.ResourcePools;
import org.ehcache.config.builders.ExpiryPolicyBuilder;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.core.spi.time.TimeSource;
import org.ehcache.expiry.ExpiryPolicy;
import org.ehcache.impl.internal.sizeof.DefaultSizeOfEngine;
import org.ehcache.core.spi.store.Store;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.ehcache.spi.serialization.Serializer;

import java.io.Serializable;

import static org.ehcache.config.builders.ResourcePoolsBuilder.newResourcePoolsBuilder;

public class OnHeapStoreEvictionTest extends org.ehcache.impl.internal.store.heap.OnHeapStoreEvictionTest {

  protected <K, V> OnHeapStoreForTests<K, V> newStore(final TimeSource timeSource,
      final EvictionAdvisor<? super K, ? super V> evictionAdvisor) {
    return new OnHeapStoreForTests<>(new Store.Configuration<K, V>() {
      @SuppressWarnings("unchecked")
      @Override
      public Class<K> getKeyType() {
        return (Class<K>) String.class;
      }

      @SuppressWarnings("unchecked")
      @Override
      public Class<V> getValueType() {
        return (Class<V>) Serializable.class;
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
      public ExpiryPolicy<? super K, ? super V> getExpiry() {
        return ExpiryPolicyBuilder.noExpiration();
      }

      @Override
      public ResourcePools getResourcePools() {
        return newResourcePoolsBuilder().heap(500, MemoryUnit.B).build();
      }

      @Override
      public Serializer<K> getKeySerializer() {
        throw new AssertionError();
      }

      @Override
      public Serializer<V> getValueSerializer() {
        throw new AssertionError();
      }

      @Override
      public int getDispatcherConcurrency() {
        return 0;
      }

      @Override
      public CacheLoaderWriter<? super K, V> getCacheLoaderWriter() {
        return null;
      }
    }, timeSource, new DefaultSizeOfEngine(Long.MAX_VALUE, Long.MAX_VALUE));
  }

}
