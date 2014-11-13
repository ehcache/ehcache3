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

import org.junit.Test;

/**
 * @author Ludovic Orban
 */
public abstract class StoreSPITest<K, V> {

  protected abstract StoreFactory<K, V> getStoreFactory();
  
  @Test
  public void testCompute() throws Exception {
    StoreComputeTest<K, V> testSuite = new StoreComputeTest<K, V>(getStoreFactory());
    testSuite.runTestSuite().reportAndThrow();
  }
  
  @Test
  public void testComputeIfAbsent() throws Exception {
    StoreComputeIfAbsentTest<K, V> testSuite = new StoreComputeIfAbsentTest<K, V>(getStoreFactory());
    testSuite.runTestSuite().reportAndThrow();
  }
  
  @Test
  public void testComputeIfPresent() throws Exception {
    StoreComputeIfPresentTest<K, V> testSuite = new StoreComputeIfPresentTest<K, V>(getStoreFactory());
    testSuite.runTestSuite().reportAndThrow();
  }

  @Test
  public void testGet() throws Exception {
    StoreGetTest<K, V> testSuite = new StoreGetTest<K, V>(getStoreFactory());
    testSuite.runTestSuite().reportAndThrow();
  }

  @Test
  public void testContainsKey() throws Exception {
    StoreContainsKeyTest<K, V> testSuite = new StoreContainsKeyTest<K, V>(getStoreFactory());
    testSuite.runTestSuite().reportAndThrow();
  }

  @Test
  public void testPutKey() throws Exception {
    StorePutTest<K, V> testSuite = new StorePutTest<K, V>(getStoreFactory());
    testSuite.runTestSuite().reportAndThrow();
  }

  @Test
  public void testPutIfAbsentKey() throws Exception {
    StorePutIfAbsentTest<K, V> testSuite = new StorePutIfAbsentTest<K, V>(getStoreFactory());
    testSuite.runTestSuite().reportAndThrow();
  }

  @Test
  public void testRemoveKey() throws Exception {
    StoreRemoveKeyTest<K, V> testSuite = new StoreRemoveKeyTest<K, V>(getStoreFactory());
    testSuite.runTestSuite().reportAndThrow();
  }

  @Test
  public void testRemoveKeyValue() throws Exception {
    StoreRemoveKeyValueTest<K, V> testSuite = new StoreRemoveKeyValueTest<K, V>(getStoreFactory());
    testSuite.runTestSuite().reportAndThrow();
  }

  @Test
  public void testReplaceKeyValue() throws Exception {
    StoreReplaceKeyValueTest<K, V> testSuite = new StoreReplaceKeyValueTest<K, V>(getStoreFactory());
    testSuite.runTestSuite().reportAndThrow();
  }

  @Test
  public void testReplaceKeyValueValue() throws Exception {
    StoreReplaceKeyValueValueTest<K, V> testSuite =
      new StoreReplaceKeyValueValueTest<K, V>(getStoreFactory());
    testSuite.runTestSuite().reportAndThrow();
  }

  @Test
  public void testClear() throws Exception {
    StoreClearTest<K, V> testSuite = new StoreClearTest<K, V>(getStoreFactory());
    testSuite.runTestSuite().reportAndThrow();
  }

  @Test
  public void testDestroy() throws Exception {
    StoreDestroyTest<K, V> testSuite = new StoreDestroyTest<K, V>(getStoreFactory());
    testSuite.runTestSuite().reportAndThrow();
  }

  @Test
  public void testClose() throws Exception {
    StoreCloseTest<K, V> testSuite = new StoreCloseTest<K, V>(getStoreFactory());
    testSuite.runTestSuite().reportAndThrow();
  }

  @Test
  public void testIterator() throws Exception {
    StoreIteratorTest<K, V> testSuite = new StoreIteratorTest<K, V>(getStoreFactory());
    testSuite.runTestSuite().reportAndThrow();
  }

  @Test
  public void testValueHolderValue() throws Exception {
    StoreValueHolderValueTest<K, V> testSuite = new StoreValueHolderValueTest<K, V>(getStoreFactory());
    testSuite.runTestSuite().reportAndThrow();
  }

  @Test
  public void testValueHolderCreationTime() throws Exception {
    StoreValueHolderCreationTimeTest<K, V> testSuite =
      new StoreValueHolderCreationTimeTest<K, V>(getStoreFactory());
    testSuite.runTestSuite().reportAndThrow();
  }

  @Test
  public void testValueHolderLastAccessTime() throws Exception {
    StoreValueHolderLastAccessTimeTest<K, V> testSuite =
      new StoreValueHolderLastAccessTimeTest<K, V>(getStoreFactory());
    testSuite.runTestSuite().reportAndThrow();
  }

  @Test
  public void testValueHolderHitRate() throws Exception {
    StoreValueHolderHitRateTest<K, V> testSuite =
      new StoreValueHolderHitRateTest<K, V>(getStoreFactory());
    testSuite.runTestSuite().reportAndThrow();
  }

  @Test
  public void testProviderCreateStore() throws Exception {
    StoreProviderCreateStoreTest<K, V> testSuite =
      new StoreProviderCreateStoreTest<K, V>(getStoreFactory());
    testSuite.runTestSuite().reportAndThrow();
  }

  @Test
  public void testProviderReleaseStore() throws Exception {
    StoreProviderReleaseStoreTest<K, V> testSuite =
      new StoreProviderReleaseStoreTest<K, V>(getStoreFactory());
    testSuite.runTestSuite().reportAndThrow();
  }

  @Test
  public void testConfigurationGetKeyType() throws Exception {
    StoreConfigurationGetKeyTypeTest<K, V> testSuite =
      new StoreConfigurationGetKeyTypeTest<K, V>(getStoreFactory());
    testSuite.runTestSuite().reportAndThrow();
  }

  @Test
  public void testConfigurationGetValueType() throws Exception {
    StoreConfigurationGetValueTypeTest<K, V> testSuite =
      new StoreConfigurationGetValueTypeTest<K, V>(getStoreFactory());
    testSuite.runTestSuite().reportAndThrow();
  }

  @Test
  public void testConfigurationGetCapacityConstraint() throws Exception {
    StoreConfigurationGetCapacityConstraintTest<K, V> testSuite =
      new StoreConfigurationGetCapacityConstraintTest<K, V>(getStoreFactory());
    testSuite.runTestSuite().reportAndThrow();
  }

  @Test
  public void testConfigurationGetEvictionVeto() throws Exception {
    StoreConfigurationGetEvictionVetoTest<K, V> testSuite =
      new StoreConfigurationGetEvictionVetoTest<K, V>(getStoreFactory());
    testSuite.runTestSuite().reportAndThrow();
  }

  @Test
  public void testConfigurationGetEvictionPrioritizer() throws Exception {
    StoreConfigurationGetEvictionPrioritizerTest<K, V> testSuite =
      new StoreConfigurationGetEvictionPrioritizerTest<K, V>(getStoreFactory());
    testSuite.runTestSuite().reportAndThrow();
  }

  @Test
  public void testIteratorHasNext() throws Exception {
    StoreIteratorHasNextTest<K, V> testSuite =
      new StoreIteratorHasNextTest<K, V>(getStoreFactory());
    testSuite.runTestSuite().reportAndThrow();
  }

  @Test
  public void testIteratorNext() throws Exception {
    StoreIteratorNextTest<K, V> testSuite =
      new StoreIteratorNextTest<K, V>(getStoreFactory());
    testSuite.runTestSuite().reportAndThrow();
  }

  @Test
  public void testBulkCompute() throws Exception {
    StoreBulkComputeTest<K, V> testSuite =
      new StoreBulkComputeTest<K, V>(getStoreFactory());
    testSuite.runTestSuite().reportAndThrow();
  }

  @Test
  public void testBulkComputeIfAbsent() throws Exception {
    StoreBulkComputeIfAbsentTest<K, V> testSuite =
      new StoreBulkComputeIfAbsentTest<K, V>(getStoreFactory());
    testSuite.runTestSuite().reportAndThrow();
  }
}
