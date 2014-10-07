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

import org.ehcache.Cache;
import org.ehcache.config.StoreConfigurationImpl;
import org.ehcache.eviction.EvictionPrioritizer;
import org.ehcache.function.Predicate;
import org.ehcache.function.Predicates;
import org.ehcache.internal.HeapResourceCacheConfiguration;
import org.ehcache.spi.cache.Store;
import org.ehcache.spi.service.ServiceConfiguration;
import org.junit.Before;
import org.junit.Test;

import java.util.Comparator;

/**
 * Test the {@link org.ehcache.internal.store.OnHeapStore} compliance to the
 * {@link org.ehcache.spi.cache.Store} contract.
 *
 * @author Aurelien Broszniowski
 */

public class OnHeapStoreSPITest {

  private StoreFactory<Object, Object> storeFactory;

  @Before
  public void setUp() {
    storeFactory = new StoreFactory<Object, Object>() {

      @Override
      public Store<Object, Object> newStore(final Store.Configuration<Object, Object> config) {
        return new OnHeapStore<Object, Object>(config);
      }

      @Override
      public Store.ValueHolder<Object> newValueHolder(final Object value) {
        return new OnHeapStoreValueHolder<Object>(value);
      }

      @Override
      public Store.Provider newProvider() {
        return new OnHeapStore.Provider();
      }

      @Override
      public Store.Configuration<Object, Object> newConfiguration(
          final Class<Object> keyType, final Class<Object> valueType, final Comparable<Long> capacityConstraint,
          final Predicate<Cache.Entry<Object, Object>> evictionVeto, final Comparator<Cache.Entry<Object, Object>> evictionPrioritizer) {
        return new StoreConfigurationImpl<Object, Object>(keyType, valueType, capacityConstraint,
            evictionVeto, evictionPrioritizer);
      }

      @Override
      public Class<Object> getKeyType() {
        return Object.class;
      }

      @Override
      public Class<Object> getValueType() {
        return Object.class;
      }

      @Override
      public ServiceConfiguration[] getServiceConfigurations() {
        return new ServiceConfiguration[] { new HeapResourceCacheConfiguration(100) };
      }
    };
  }

  @Test
  public void testGet() throws Exception {
    StoreGetTest<Object, Object> testSuite = new StoreGetTest<Object, Object>(storeFactory);
    testSuite.runTestSuite().reportAndThrow();
  }

  @Test
  public void testContainsKey() throws Exception {
    StoreContainsKeyTest<Object, Object> testSuite = new StoreContainsKeyTest<Object, Object>(storeFactory);
    testSuite.runTestSuite().reportAndThrow();
  }

  @Test
  public void testPutKey() throws Exception {
    StorePutTest<Object, Object> testSuite = new StorePutTest<Object, Object>(storeFactory);
    testSuite.runTestSuite().reportAndThrow();
  }

  @Test
  public void testPutIfAbsentKey() throws Exception {
    StorePutIfAbsentTest<Object, Object> testSuite = new StorePutIfAbsentTest<Object, Object>(storeFactory);
    testSuite.runTestSuite().reportAndThrow();
  }

  @Test
  public void testRemoveKey() throws Exception {
    StoreRemoveKeyTest<Object, Object> testSuite = new StoreRemoveKeyTest<Object, Object>(storeFactory);
    testSuite.runTestSuite().reportAndThrow();
  }

  @Test
  public void testRemoveKeyValue() throws Exception {
    StoreRemoveKeyValueTest<Object, Object> testSuite = new StoreRemoveKeyValueTest<Object, Object>(storeFactory);
    testSuite.runTestSuite().reportAndThrow();
  }

  @Test
  public void testReplaceKeyValue() throws Exception {
    StoreReplaceKeyValueTest<Object, Object> testSuite = new StoreReplaceKeyValueTest<Object, Object>(storeFactory);
    testSuite.runTestSuite().reportAndThrow();
  }

  @Test
  public void testReplaceKeyValueValue() throws Exception {
    StoreReplaceKeyValueValueTest<Object, Object> testSuite =
        new StoreReplaceKeyValueValueTest<Object, Object>(storeFactory);
    testSuite.runTestSuite().reportAndThrow();
  }

  @Test
  public void testClear() throws Exception {
    StoreClearTest<Object, Object> testSuite = new StoreClearTest<Object, Object>(storeFactory);
    testSuite.runTestSuite().reportAndThrow();
  }

  @Test
  public void testDestroy() throws Exception {
    StoreDestroyTest<Object, Object> testSuite = new StoreDestroyTest<Object, Object>(storeFactory);
    testSuite.runTestSuite().reportAndThrow();
  }

  @Test
  public void testClose() throws Exception {
    StoreCloseTest<Object, Object> testSuite = new StoreCloseTest<Object, Object>(storeFactory);
    testSuite.runTestSuite().reportAndThrow();
  }

  @Test
  public void testIterator() throws Exception {
    StoreIteratorTest<Object, Object> testSuite = new StoreIteratorTest<Object, Object>(storeFactory);
    testSuite.runTestSuite().reportAndThrow();
  }

  @Test
  public void testValueHolderValue() throws Exception {
    StoreValueHolderValueTest<Object, Object> testSuite = new StoreValueHolderValueTest<Object, Object>(storeFactory);
    testSuite.runTestSuite().reportAndThrow();
  }

  @Test
  public void testValueHolderCreationTime() throws Exception {
    StoreValueHolderCreationTimeTest<Object, Object> testSuite =
        new StoreValueHolderCreationTimeTest<Object, Object>(storeFactory);
    testSuite.runTestSuite().reportAndThrow();
  }

  @Test
  public void testValueHolderLastAccessTime() throws Exception {
    StoreValueHolderLastAccessTimeTest<Object, Object> testSuite =
        new StoreValueHolderLastAccessTimeTest<Object, Object>(storeFactory);
    testSuite.runTestSuite().reportAndThrow();
  }

  @Test
  public void testValueHolderHitRate() throws Exception {
    StoreValueHolderHitRateTest<Object, Object> testSuite =
        new StoreValueHolderHitRateTest<Object, Object>(storeFactory);
    testSuite.runTestSuite().reportAndThrow();
  }

  @Test
  public void testProviderCreateStore() throws Exception {
    StoreProviderCreateStoreTest<Object, Object> testSuite =
        new StoreProviderCreateStoreTest<Object, Object>(storeFactory);
    testSuite.runTestSuite().reportAndThrow();
  }

  @Test
  public void testProviderReleaseStore() throws Exception {
    StoreProviderReleaseStoreTest<Object, Object> testSuite =
        new StoreProviderReleaseStoreTest<Object, Object>(storeFactory);
    testSuite.runTestSuite().reportAndThrow();
  }

  @Test
  public void testConfigurationGetKeyType() throws Exception {
    StoreConfigurationGetKeyTypeTest<Object, Object> testSuite =
        new StoreConfigurationGetKeyTypeTest<Object, Object>(storeFactory);
    testSuite.runTestSuite().reportAndThrow();
  }

  @Test
  public void testConfigurationGetValueType() throws Exception {
    StoreConfigurationGetValueTypeTest<Object, Object> testSuite =
        new StoreConfigurationGetValueTypeTest<Object, Object>(storeFactory);
    testSuite.runTestSuite().reportAndThrow();
  }
}
