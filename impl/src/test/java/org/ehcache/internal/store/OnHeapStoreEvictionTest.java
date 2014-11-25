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
package org.ehcache.internal.store;

import org.ehcache.config.EvictionPrioritizer;
import org.ehcache.config.EvictionVeto;
import org.ehcache.exceptions.SerializerException;
import org.ehcache.expiry.Expirations;
import org.ehcache.expiry.Expiry;
import org.ehcache.function.BiFunction;
import org.ehcache.function.Function;
import org.ehcache.internal.SystemTimeSource;
import org.ehcache.internal.TimeSource;
import org.ehcache.internal.serialization.JavaSerializationProvider;
import org.ehcache.spi.cache.Store;
import org.ehcache.spi.serialization.SerializationProvider;
import org.junit.Test;

import java.io.Serializable;
import java.util.ArrayList;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.fail;

public class OnHeapStoreEvictionTest {

  protected <K, V> OnHeapStoreForTests<K, V> newStore() {
    return newStore(SystemTimeSource.INSTANCE, Expirations.noExpiration());
  }

  /** eviction tests : asserting the evict method is called **/

  @Test
  public void testComputeCalledEnforceCapacity() throws Exception {
    OnHeapStoreForTests<String, String> store = newStore();

    store.put("key", "value");
    store.compute("key", new BiFunction<String, String, String>() {
      @Override
      public String apply(String mappedKey, String mappedValue) {
        return "value2";
      }
    });

    assertThat(store.enforceCapacityWasCalled(), is(true));
  }

  @Test
  public void testComputeIfAbsentCalledEnforceCapacity() throws Exception {
    OnHeapStoreForTests<String, String> store = newStore();

    store.put("key", "value");
    store.computeIfAbsent("key", new Function<String, String>() {
      @Override
      public String apply(String mappedKey) {
        return "value2";
      }
    });

    assertThat(store.enforceCapacityWasCalled(), is(true));
  }

  protected <K, V> OnHeapStoreForTests<K, V> newStore(final TimeSource timeSource,
      final Expiry<? super K, ? super V> expiry) {
    return new OnHeapStoreForTests<K, V>(new Store.Configuration<K, V>() {
      @Override
      public Class<K> getKeyType() {
        return (Class<K>) String.class;
      }

      @Override
      public Class<V> getValueType() {
        return (Class<V>) Serializable.class;
      }

      @Override
      public Comparable<Long> getCapacityConstraint() {
        return null;
      }

      @Override
      public EvictionVeto<? super K, ? super V> getEvictionVeto() {
        return null;
      }

      @Override
      public EvictionPrioritizer<? super K, ? super V> getEvictionPrioritizer() {
        return null;
      }

      @Override
      public SerializationProvider getSerializationProvider() {
        return new JavaSerializationProvider();
      }

      @Override
      public ClassLoader getClassLoader() {
        return getClass().getClassLoader();
      }

      @Override
      public Expiry<? super K, ? super V> getExpiry() {
        return expiry;
      }
    }, timeSource);
  }

  static class OnHeapStoreForTests<K, V> extends OnHeapStore<K, V> {
    public OnHeapStoreForTests(final Configuration config, final TimeSource timeSource) {
      super(config, timeSource, false);
    }

    private boolean enforceCapacityWasCalled = false;

    ValueHolder<V> enforceCapacityIfValueNotNull(final OnHeapValueHolder<V> computeResult) {
      enforceCapacityWasCalled = true;
      return super.enforceCapacityIfValueNotNull(computeResult);
    }

    boolean enforceCapacityWasCalled() {
      return enforceCapacityWasCalled;
    }

  }

}
