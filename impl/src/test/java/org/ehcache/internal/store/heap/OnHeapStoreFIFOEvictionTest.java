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

import org.ehcache.config.Eviction;
import org.ehcache.config.EvictionPrioritizer;
import org.ehcache.config.EvictionVeto;
import org.ehcache.config.ResourcePools;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.exceptions.CacheAccessException;
import org.ehcache.expiry.Expirations;
import org.ehcache.expiry.Expiry;
import org.ehcache.internal.TestTimeSource;
import org.ehcache.internal.TimeSource;
import org.ehcache.spi.cache.Store;
import org.hamcrest.core.Is;
import org.junit.Test;

import java.io.Serializable;

import static org.ehcache.config.ResourcePoolsBuilder.newResourcePoolsBuilder;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

/**
 * @author rism
 */
public class OnHeapStoreFIFOEvictionTest {

  @Test
  public void testFIFOEvictionWithEntriesLessThanSampleSize() throws CacheAccessException {
    TestTimeSource timeSource = new TestTimeSource();
    OnHeapStoreForTests<Number, String> store = newStore(timeSource, OnHeapStore.SAMPLE_SIZE - 1);

//    Adding elements after time intervals
    store.put(1, "a");
    timeSource.advanceTime(1);

    store.put(2, "b");
    timeSource.advanceTime(1);

    store.put(3, "c");
    timeSource.advanceTime(1);

    store.put(4, "d");
    timeSource.advanceTime(1);

    store.put(5, "e");
    timeSource.advanceTime(1);

    store.put(6, "f");
    timeSource.advanceTime(1);

    store.put(7, "g");
    timeSource.advanceTime(1);

    store.put(8, "h");

//    asserting store content
    assertNull(store.get(1));
    assertThat(store.get(2).value(), Is.is("b"));
    assertThat(store.get(3).value(), Is.is("c"));
    assertThat(store.get(4).value(), Is.is("d"));
    assertThat(store.get(5).value(), Is.is("e"));
    assertThat(store.get(6).value(), Is.is("f"));
    assertThat(store.get(7).value(), Is.is("g"));
    assertThat(store.get(8).value(), Is.is("h"));
  }

  @Test
  public void testFIFOEvictionWithEntriesLargerThanSampleSize () throws CacheAccessException {
    TestTimeSource timeSource = new TestTimeSource();
    OnHeapStoreForTests<Number, Number> store = newStore(timeSource, 100);

    for (int i = 0; i < 100; i++) {
      store.put(i, i);
      timeSource.advanceTime(1);
    }

    store.put(100, 100);

//    asserting last-in elements are never evicted
    for (int i = 101 - (store.SAMPLE_SIZE - 1); i < 101; i++) {
      assertThat(store.get(i).value(), Is.<Number>is(i));
    }
  }

  protected <K, V> OnHeapStoreForTests<K, V> newStore(final TimeSource timeSource,
                                                      final long storeSize) {
    return new OnHeapStoreForTests<K, V>(new Store.Configuration<K, V>() {
      @SuppressWarnings("unchecked")
      @Override
      public Class<K> getKeyType() {
        return (Class<K>) Number.class;
      }

      @SuppressWarnings("unchecked")
      @Override
      public Class<V> getValueType() {
        return (Class<V>) Serializable.class;
      }

      @Override
      public EvictionVeto<? super K, ? super V> getEvictionVeto() {
        return null;
      }

      @Override
      public EvictionPrioritizer<? super K, ? super V> getEvictionPrioritizer() {
        return Eviction.Prioritizer.FIFO;
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
        return newResourcePoolsBuilder().heap(storeSize, EntryUnit.ENTRIES).build();
      }
    }, timeSource);
  }

  static class OnHeapStoreForTests<K, V> extends OnHeapStore<K, V> {
    public OnHeapStoreForTests(final Configuration<K, V> config, final TimeSource timeSource) {
      super(config, timeSource, false, null, null);
    }
  }
}
