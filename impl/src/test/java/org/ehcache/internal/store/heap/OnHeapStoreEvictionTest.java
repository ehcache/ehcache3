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
package org.ehcache.internal.store.heap;

import org.ehcache.config.EvictionVeto;
import org.ehcache.config.ResourcePools;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.exceptions.CacheAccessException;
import org.ehcache.expiry.Expirations;
import org.ehcache.expiry.Expiry;
import org.ehcache.function.BiFunction;
import org.ehcache.function.Function;
import org.ehcache.internal.SystemTimeSource;
import org.ehcache.internal.TimeSource;
import org.ehcache.internal.copy.IdentityCopier;
import org.ehcache.internal.store.heap.holders.OnHeapValueHolder;
import org.ehcache.spi.cache.Store;
import org.ehcache.spi.cache.Store.ValueHolder;
import org.ehcache.spi.copy.Copier;
import org.ehcache.spi.serialization.Serializer;
import org.junit.Test;

import java.io.Serializable;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import org.ehcache.config.Eviction;

import static org.ehcache.config.ResourcePoolsBuilder.newResourcePoolsBuilder;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class OnHeapStoreEvictionTest {

  protected <K, V> OnHeapStoreForTests<K, V> newStore() {
    return newStore(SystemTimeSource.INSTANCE, null);
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

  @Test
  public void testFaultsDoNotGetToEvictionVeto() throws CacheAccessException {
    final Semaphore semaphore = new Semaphore(0);

    final OnHeapStoreForTests<String, String> store = newStore(SystemTimeSource.INSTANCE, Eviction.none());

    ExecutorService executor = Executors.newCachedThreadPool();
    try {
      executor.submit(new Callable<Store.ValueHolder<String>>() {
        @Override
        public Store.ValueHolder<String> call() throws Exception {
          return store.getOrComputeIfAbsent("prime", new Function<String, ValueHolder<String>>() {
            @Override
            public ValueHolder<String> apply(final String key) {
              semaphore.acquireUninterruptibly();
              return new OnHeapValueHolder<String>(0, 0, false) {
                @Override
                public String value() {
                  return key;
                }
              };
            }
          });
        }
      });

      while (!semaphore.hasQueuedThreads());
      store.put("boom", "boom");
    } finally {
      semaphore.release(1);
      executor.shutdown();
    }
  }

  protected <K, V> OnHeapStoreForTests<K, V> newStore(final TimeSource timeSource,
      final EvictionVeto<? super K, ? super V> veto) {
    return new OnHeapStoreForTests<K, V>(new Store.Configuration<K, V>() {
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
      public EvictionVeto<? super K, ? super V> getEvictionVeto() {
        return veto;
      }

      @Override
      public ClassLoader getClassLoader() {
        return getClass().getClassLoader();
      }

      @Override
      public Expiry<? super K, ? super V> getExpiry() {
        return Expirations.noExpiration();
      }

      @Override
      public ResourcePools getResourcePools() {
        return newResourcePoolsBuilder().heap(1, EntryUnit.ENTRIES).build();
      }

      @Override
      public Serializer<K> getKeySerializer() {
        throw new AssertionError();
      }

      @Override
      public Serializer<V> getValueSerializer() {
        throw new AssertionError();
      }
    }, timeSource);
  }

  static class OnHeapStoreForTests<K, V> extends OnHeapStore<K, V> {

    private static final Copier DEFAULT_COPIER = new IdentityCopier();

    public OnHeapStoreForTests(final Configuration<K, V> config, final TimeSource timeSource) {
      super(config, timeSource, DEFAULT_COPIER, DEFAULT_COPIER);
    }

    private boolean enforceCapacityWasCalled = false;

    @Override
    ValueHolder<V> enforceCapacityIfValueNotNull(final OnHeapValueHolder<V> computeResult, boolean hasListeners) {
      enforceCapacityWasCalled = true;
      return super.enforceCapacityIfValueNotNull(computeResult, hasListeners);
    }

    boolean enforceCapacityWasCalled() {
      return enforceCapacityWasCalled;
    }

  }

}
