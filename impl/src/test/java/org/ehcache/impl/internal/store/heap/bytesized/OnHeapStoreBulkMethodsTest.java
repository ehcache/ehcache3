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

import org.ehcache.config.builders.ExpiryPolicyBuilder;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.core.statistics.DefaultStatisticsService;
import org.ehcache.impl.copy.IdentityCopier;
import org.ehcache.impl.internal.concurrent.ConcurrentHashMap;
import org.ehcache.core.events.NullStoreEventDispatcher;
import org.ehcache.impl.internal.sizeof.DefaultSizeOfEngine;
import org.ehcache.impl.internal.store.heap.OnHeapStore;
import org.ehcache.core.spi.time.SystemTimeSource;
import org.ehcache.core.spi.store.Store;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

import static org.ehcache.config.builders.ResourcePoolsBuilder.newResourcePoolsBuilder;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class OnHeapStoreBulkMethodsTest extends org.ehcache.impl.internal.store.heap.OnHeapStoreBulkMethodsTest {

  @SuppressWarnings("unchecked")
  protected <K, V> Store.Configuration<K, V> mockStoreConfig() {
    @SuppressWarnings("rawtypes")
    Store.Configuration config = mock(Store.Configuration.class);
    when(config.getExpiry()).thenReturn(ExpiryPolicyBuilder.noExpiration());
    when(config.getKeyType()).thenReturn(Number.class);
    when(config.getValueType()).thenReturn(CharSequence.class);
    when(config.getResourcePools()).thenReturn(newResourcePoolsBuilder().heap(100, MemoryUnit.KB).build());
    return config;
  }

  @SuppressWarnings("unchecked")
  protected OnHeapStore<Number, CharSequence> newStore() {
    Store.Configuration<Number, CharSequence> configuration = mockStoreConfig();
    return new OnHeapStore<>(configuration, SystemTimeSource.INSTANCE, IdentityCopier.identityCopier(), IdentityCopier.identityCopier(),
        new DefaultSizeOfEngine(Long.MAX_VALUE, Long.MAX_VALUE), NullStoreEventDispatcher.nullStoreEventDispatcher(), new DefaultStatisticsService());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testBulkComputeFunctionGetsValuesOfEntries() throws Exception {
    @SuppressWarnings("rawtypes")
    Store.Configuration config = mock(Store.Configuration.class);
    when(config.getExpiry()).thenReturn(ExpiryPolicyBuilder.noExpiration());
    when(config.getKeyType()).thenReturn(Number.class);
    when(config.getValueType()).thenReturn(Number.class);
    when(config.getResourcePools()).thenReturn(newResourcePoolsBuilder().heap(100, MemoryUnit.KB).build());
    Store.Configuration<Number, Number> configuration = config;

    OnHeapStore<Number, Number> store = new OnHeapStore<>(configuration, SystemTimeSource.INSTANCE, IdentityCopier.identityCopier(), IdentityCopier.identityCopier(),
        new DefaultSizeOfEngine(Long.MAX_VALUE, Long.MAX_VALUE), NullStoreEventDispatcher.<Number, Number>nullStoreEventDispatcher(), new DefaultStatisticsService());
    store.put(1, 2);
    store.put(2, 3);
    store.put(3, 4);

    Map<Number, Store.ValueHolder<Number>> result = store.bulkCompute(new HashSet<Number>(Arrays.asList(1, 2, 3, 4, 5, 6)), entries -> {
      Map<Number, Number> newValues = new HashMap<>();
      for (Map.Entry<? extends Number, ? extends Number> entry : entries) {
        final Number currentValue = entry.getValue();
        if(currentValue == null) {
          if(entry.getKey().equals(4)) {
            newValues.put(entry.getKey(), null);
          } else {
            newValues.put(entry.getKey(), 0);
          }
        } else {
          newValues.put(entry.getKey(), currentValue.intValue() * 2);
        }

      }
      return newValues.entrySet();
    });

    ConcurrentMap<Number, Number> check = new ConcurrentHashMap<>();
    check.put(1, 4);
    check.put(2, 6);
    check.put(3, 8);
    check.put(4, 0);
    check.put(5, 0);
    check.put(6, 0);

    assertThat(result.get(1).get(), Matchers.is(check.get(1)));
    assertThat(result.get(2).get(), Matchers.is(check.get(2)));
    assertThat(result.get(3).get(), Matchers.is(check.get(3)));
    assertThat(result.get(4), nullValue());
    assertThat(result.get(5).get(), Matchers.is(check.get(5)));
    assertThat(result.get(6).get(), Matchers.is(check.get(6)));

    for (Number key : check.keySet()) {
      final Store.ValueHolder<Number> holder = store.get(key);
      if(holder != null) {
        check.remove(key, holder.get());
      }
    }
    assertThat(check.size(), is(1));
    assertThat(check.containsKey(4), is(true));

  }

}
