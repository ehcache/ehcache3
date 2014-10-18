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

  private StoreFactory<String, String> storeFactory;

  @Before
  public void setUp() {
    storeFactory = new StoreFactory<String, String>() {

      @Override
      public Store<String, String> newStore(final Store.Configuration<String, String> config) {
        return new OnHeapStore<String, String>(config);
      }

      @Override
      public Store.ValueHolder<String> newValueHolder(final String value) {
        return new OnHeapStoreValueHolder<String>(value);
      }

      @Override
      public Store.Provider newProvider() {
        return new OnHeapStore.Provider();
      }

      @Override
      public Store.Configuration<String, String> newConfiguration(
          final Class<String> keyType, final Class<String> valueType, final Comparable<Long> capacityConstraint,
          final Predicate<Cache.Entry<String, String>> evictionVeto, final Comparator<Cache.Entry<String, String>> evictionPrioritizer) {
        return new StoreConfigurationImpl<String, String>(keyType, valueType, capacityConstraint,
            evictionVeto, evictionPrioritizer, ClassLoader.getSystemClassLoader());
      }

      @Override
      public Class<String> getKeyType() {
        return String.class;
      }

      @Override
      public Class<String> getValueType() {
        return String.class;
      }

      @Override
      public ServiceConfiguration[] getServiceConfigurations() {
        return new ServiceConfiguration[] { new HeapResourceCacheConfiguration(100) };
      }

      @Override
      public String createKey(long seed) {
        return new String("" + seed);
      }

      @Override
      public String createValue(long seed) {
        return new String("" + seed);
      }
    };
  }

  @Test
  public void testGet() throws Exception {
    StoreGetTest<String, String> testSuite = new StoreGetTest<String, String>(storeFactory);
    testSuite.runTestSuite().reportAndThrow();
  }

  @Test
  public void testContainsKey() throws Exception {
    StoreContainsKeyTest<String, String> testSuite = new StoreContainsKeyTest<String, String>(storeFactory);
    testSuite.runTestSuite().reportAndThrow();
  }

  @Test
  public void testPutKey() throws Exception {
    StorePutTest<String, String> testSuite = new StorePutTest<String, String>(storeFactory);
    testSuite.runTestSuite().reportAndThrow();
  }

  @Test
  public void testPutIfAbsentKey() throws Exception {
    StorePutIfAbsentTest<String, String> testSuite = new StorePutIfAbsentTest<String, String>(storeFactory);
    testSuite.runTestSuite().reportAndThrow();
  }

  @Test
  public void testRemoveKey() throws Exception {
    StoreRemoveKeyTest<String, String> testSuite = new StoreRemoveKeyTest<String, String>(storeFactory);
    testSuite.runTestSuite().reportAndThrow();
  }

  @Test
  public void testRemoveKeyValue() throws Exception {
    StoreRemoveKeyValueTest<String, String> testSuite = new StoreRemoveKeyValueTest<String, String>(storeFactory);
    testSuite.runTestSuite().reportAndThrow();
  }

  @Test
  public void testReplaceKeyValue() throws Exception {
    StoreReplaceKeyValueTest<String, String> testSuite = new StoreReplaceKeyValueTest<String, String>(storeFactory);
    testSuite.runTestSuite().reportAndThrow();
  }

  @Test
  public void testReplaceKeyValueValue() throws Exception {
    StoreReplaceKeyValueValueTest<String, String> testSuite =
        new StoreReplaceKeyValueValueTest<String, String>(storeFactory);
    testSuite.runTestSuite().reportAndThrow();
  }

  @Test
  public void testClear() throws Exception {
    StoreClearTest<String, String> testSuite = new StoreClearTest<String, String>(storeFactory);
    testSuite.runTestSuite().reportAndThrow();
  }

  @Test
  public void testDestroy() throws Exception {
    StoreDestroyTest<String, String> testSuite = new StoreDestroyTest<String, String>(storeFactory);
    testSuite.runTestSuite().reportAndThrow();
  }

  @Test
  public void testClose() throws Exception {
    StoreCloseTest<String, String> testSuite = new StoreCloseTest<String, String>(storeFactory);
    testSuite.runTestSuite().reportAndThrow();
  }

  @Test
  public void testIterator() throws Exception {
    StoreIteratorTest<String, String> testSuite = new StoreIteratorTest<String, String>(storeFactory);
    testSuite.runTestSuite().reportAndThrow();
  }

  @Test
  public void testValueHolderValue() throws Exception {
    StoreValueHolderValueTest<String, String> testSuite = new StoreValueHolderValueTest<String, String>(storeFactory);
    testSuite.runTestSuite().reportAndThrow();
  }

  @Test
  public void testValueHolderCreationTime() throws Exception {
    StoreValueHolderCreationTimeTest<String, String> testSuite =
        new StoreValueHolderCreationTimeTest<String, String>(storeFactory);
    testSuite.runTestSuite().reportAndThrow();
  }

  @Test
  public void testValueHolderLastAccessTime() throws Exception {
    StoreValueHolderLastAccessTimeTest<String, String> testSuite =
        new StoreValueHolderLastAccessTimeTest<String, String>(storeFactory);
    testSuite.runTestSuite().reportAndThrow();
  }

  @Test
  public void testValueHolderHitRate() throws Exception {
    StoreValueHolderHitRateTest<String, String> testSuite =
        new StoreValueHolderHitRateTest<String, String>(storeFactory);
    testSuite.runTestSuite().reportAndThrow();
  }

  @Test
  public void testProviderCreateStore() throws Exception {
    StoreProviderCreateStoreTest<String, String> testSuite =
        new StoreProviderCreateStoreTest<String, String>(storeFactory);
    testSuite.runTestSuite().reportAndThrow();
  }

  @Test
  public void testProviderReleaseStore() throws Exception {
    StoreProviderReleaseStoreTest<String, String> testSuite =
        new StoreProviderReleaseStoreTest<String, String>(storeFactory);
    testSuite.runTestSuite().reportAndThrow();
  }

  @Test
  public void testConfigurationGetKeyType() throws Exception {
    StoreConfigurationGetKeyTypeTest<String, String> testSuite =
        new StoreConfigurationGetKeyTypeTest<String, String>(storeFactory);
    testSuite.runTestSuite().reportAndThrow();
  }

  @Test
  public void testConfigurationGetValueType() throws Exception {
    StoreConfigurationGetValueTypeTest<String, String> testSuite =
        new StoreConfigurationGetValueTypeTest<String, String>(storeFactory);
    testSuite.runTestSuite().reportAndThrow();
  }

  @Test
  public void testConfigurationGetCapacityConstraint() throws Exception {
    StoreConfigurationGetCapacityConstraintTest<String, String> testSuite =
        new StoreConfigurationGetCapacityConstraintTest<String, String>(storeFactory);
    testSuite.runTestSuite().reportAndThrow();
  }

  @Test
  public void testConfigurationGetEvictionVeto() throws Exception {
    StoreConfigurationGetEvictionVetoTest<String, String> testSuite =
        new StoreConfigurationGetEvictionVetoTest<String, String>(storeFactory);
    testSuite.runTestSuite().reportAndThrow();
  }

  @Test
  public void testConfigurationGetEvictionPrioritizer() throws Exception {
    StoreConfigurationGetEvictionPrioritizerTest<String, String> testSuite =
        new StoreConfigurationGetEvictionPrioritizerTest<String, String>(storeFactory);
    testSuite.runTestSuite().reportAndThrow();
  }

  @Test
  public void testIteratorHasNext() throws Exception {
    StoreIteratorHasNextTest<String, String> testSuite =
        new StoreIteratorHasNextTest<String, String>(storeFactory);
    testSuite.runTestSuite().reportAndThrow();
  }

  @Test
  public void testIteratorNext() throws Exception {
    StoreIteratorNextTest<String, String> testSuite =
        new StoreIteratorNextTest<String, String>(storeFactory);
    testSuite.runTestSuite().reportAndThrow();
  }

}
