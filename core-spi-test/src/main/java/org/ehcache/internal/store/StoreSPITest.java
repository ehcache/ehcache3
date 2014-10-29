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
public abstract class StoreSPITest {

  protected abstract StoreFactory<String, String> getStoreFactory();
  
  @Test
  public void testCompute() throws Exception {
    StoreComputeTest<String, String> testSuite = new StoreComputeTest<String, String>(getStoreFactory());
    testSuite.runTestSuite().reportAndThrow();
  }
  
  @Test
  public void testComputeIfAbsent() throws Exception {
    StoreComputeIfAbsentTest<String, String> testSuite = new StoreComputeIfAbsentTest<String, String>(getStoreFactory());
    testSuite.runTestSuite().reportAndThrow();
  }
  
  @Test
  public void testComputeIfPresent() throws Exception {
    StoreComputeIfPresentTest<String, String> testSuite = new StoreComputeIfPresentTest<String, String>(getStoreFactory());
    testSuite.runTestSuite().reportAndThrow();
  }

  @Test
  public void testGet() throws Exception {
    StoreGetTest<String, String> testSuite = new StoreGetTest<String, String>(getStoreFactory());
    testSuite.runTestSuite().reportAndThrow();
  }

  @Test
  public void testContainsKey() throws Exception {
    StoreContainsKeyTest<String, String> testSuite = new StoreContainsKeyTest<String, String>(getStoreFactory());
    testSuite.runTestSuite().reportAndThrow();
  }

  @Test
  public void testPutKey() throws Exception {
    StorePutTest<String, String> testSuite = new StorePutTest<String, String>(getStoreFactory());
    testSuite.runTestSuite().reportAndThrow();
  }

  @Test
  public void testPutIfAbsentKey() throws Exception {
    StorePutIfAbsentTest<String, String> testSuite = new StorePutIfAbsentTest<String, String>(getStoreFactory());
    testSuite.runTestSuite().reportAndThrow();
  }

  @Test
  public void testRemoveKey() throws Exception {
    StoreRemoveKeyTest<String, String> testSuite = new StoreRemoveKeyTest<String, String>(getStoreFactory());
    testSuite.runTestSuite().reportAndThrow();
  }

  @Test
  public void testRemoveKeyValue() throws Exception {
    StoreRemoveKeyValueTest<String, String> testSuite = new StoreRemoveKeyValueTest<String, String>(getStoreFactory());
    testSuite.runTestSuite().reportAndThrow();
  }

  @Test
  public void testReplaceKeyValue() throws Exception {
    StoreReplaceKeyValueTest<String, String> testSuite = new StoreReplaceKeyValueTest<String, String>(getStoreFactory());
    testSuite.runTestSuite().reportAndThrow();
  }

  @Test
  public void testReplaceKeyValueValue() throws Exception {
    StoreReplaceKeyValueValueTest<String, String> testSuite =
      new StoreReplaceKeyValueValueTest<String, String>(getStoreFactory());
    testSuite.runTestSuite().reportAndThrow();
  }

  @Test
  public void testClear() throws Exception {
    StoreClearTest<String, String> testSuite = new StoreClearTest<String, String>(getStoreFactory());
    testSuite.runTestSuite().reportAndThrow();
  }

  @Test
  public void testDestroy() throws Exception {
    StoreDestroyTest<String, String> testSuite = new StoreDestroyTest<String, String>(getStoreFactory());
    testSuite.runTestSuite().reportAndThrow();
  }

  @Test
  public void testClose() throws Exception {
    StoreCloseTest<String, String> testSuite = new StoreCloseTest<String, String>(getStoreFactory());
    testSuite.runTestSuite().reportAndThrow();
  }

  @Test
  public void testIterator() throws Exception {
    StoreIteratorTest<String, String> testSuite = new StoreIteratorTest<String, String>(getStoreFactory());
    testSuite.runTestSuite().reportAndThrow();
  }

  @Test
  public void testValueHolderValue() throws Exception {
    StoreValueHolderValueTest<String, String> testSuite = new StoreValueHolderValueTest<String, String>(getStoreFactory());
    testSuite.runTestSuite().reportAndThrow();
  }

  @Test
  public void testValueHolderCreationTime() throws Exception {
    StoreValueHolderCreationTimeTest<String, String> testSuite =
      new StoreValueHolderCreationTimeTest<String, String>(getStoreFactory());
    testSuite.runTestSuite().reportAndThrow();
  }

  @Test
  public void testValueHolderLastAccessTime() throws Exception {
    StoreValueHolderLastAccessTimeTest<String, String> testSuite =
      new StoreValueHolderLastAccessTimeTest<String, String>(getStoreFactory());
    testSuite.runTestSuite().reportAndThrow();
  }

  @Test
  public void testValueHolderHitRate() throws Exception {
    StoreValueHolderHitRateTest<String, String> testSuite =
      new StoreValueHolderHitRateTest<String, String>(getStoreFactory());
    testSuite.runTestSuite().reportAndThrow();
  }

  @Test
  public void testProviderCreateStore() throws Exception {
    StoreProviderCreateStoreTest<String, String> testSuite =
      new StoreProviderCreateStoreTest<String, String>(getStoreFactory());
    testSuite.runTestSuite().reportAndThrow();
  }

  @Test
  public void testProviderReleaseStore() throws Exception {
    StoreProviderReleaseStoreTest<String, String> testSuite =
      new StoreProviderReleaseStoreTest<String, String>(getStoreFactory());
    testSuite.runTestSuite().reportAndThrow();
  }

  @Test
  public void testConfigurationGetKeyType() throws Exception {
    StoreConfigurationGetKeyTypeTest<String, String> testSuite =
      new StoreConfigurationGetKeyTypeTest<String, String>(getStoreFactory());
    testSuite.runTestSuite().reportAndThrow();
  }

  @Test
  public void testConfigurationGetValueType() throws Exception {
    StoreConfigurationGetValueTypeTest<String, String> testSuite =
      new StoreConfigurationGetValueTypeTest<String, String>(getStoreFactory());
    testSuite.runTestSuite().reportAndThrow();
  }

  @Test
  public void testConfigurationGetCapacityConstraint() throws Exception {
    StoreConfigurationGetCapacityConstraintTest<String, String> testSuite =
      new StoreConfigurationGetCapacityConstraintTest<String, String>(getStoreFactory());
    testSuite.runTestSuite().reportAndThrow();
  }

  @Test
  public void testConfigurationGetEvictionVeto() throws Exception {
    StoreConfigurationGetEvictionVetoTest<String, String> testSuite =
      new StoreConfigurationGetEvictionVetoTest<String, String>(getStoreFactory());
    testSuite.runTestSuite().reportAndThrow();
  }

  @Test
  public void testConfigurationGetEvictionPrioritizer() throws Exception {
    StoreConfigurationGetEvictionPrioritizerTest<String, String> testSuite =
      new StoreConfigurationGetEvictionPrioritizerTest<String, String>(getStoreFactory());
    testSuite.runTestSuite().reportAndThrow();
  }

  @Test
  public void testIteratorHasNext() throws Exception {
    StoreIteratorHasNextTest<String, String> testSuite =
      new StoreIteratorHasNextTest<String, String>(getStoreFactory());
    testSuite.runTestSuite().reportAndThrow();
  }

  @Test
  public void testIteratorNext() throws Exception {
    StoreIteratorNextTest<String, String> testSuite =
      new StoreIteratorNextTest<String, String>(getStoreFactory());
    testSuite.runTestSuite().reportAndThrow();
  }

}
