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
  public void testGetAndCompute() throws Exception {
    StoreGetAndComputeTest<K, V> testSuite = new StoreGetAndComputeTest<>(getStoreFactory());
    testSuite.runTestSuite().reportAndThrow();
  }

  @Test
  public void testComputeIfAbsent() throws Exception {
    StoreComputeIfAbsentTest<K, V> testSuite = new StoreComputeIfAbsentTest<>(getStoreFactory());
    testSuite.runTestSuite().reportAndThrow();
  }

  @Test
  public void testGet() throws Exception {
    StoreGetTest<K, V> testSuite = new StoreGetTest<>(getStoreFactory());
    testSuite.runTestSuite().reportAndThrow();
  }

  @Test
  public void testContainsKey() throws Exception {
    StoreContainsKeyTest<K, V> testSuite = new StoreContainsKeyTest<>(getStoreFactory());
    testSuite.runTestSuite().reportAndThrow();
  }

  @Test
  public void testPutKey() throws Exception {
    StorePutTest<K, V> testSuite = new StorePutTest<>(getStoreFactory());
    testSuite.runTestSuite().reportAndThrow();
  }

  @Test
  public void testPutIfAbsentKey() throws Exception {
    StorePutIfAbsentTest<K, V> testSuite = new StorePutIfAbsentTest<>(getStoreFactory());
    testSuite.runTestSuite().reportAndThrow();
  }

  @Test
  public void testRemoveKey() throws Exception {
    StoreRemoveKeyTest<K, V> testSuite = new StoreRemoveKeyTest<>(getStoreFactory());
    testSuite.runTestSuite().reportAndThrow();
  }

  @Test
  public void testRemoveKeyValue() throws Exception {
    StoreRemoveKeyValueTest<K, V> testSuite = new StoreRemoveKeyValueTest<>(getStoreFactory());
    testSuite.runTestSuite().reportAndThrow();
  }

  @Test
  public void testReplaceKeyValue() throws Exception {
    StoreReplaceKeyValueTest<K, V> testSuite = new StoreReplaceKeyValueTest<>(getStoreFactory());
    testSuite.runTestSuite().reportAndThrow();
  }

  @Test
  public void testReplaceKeyValueValue() throws Exception {
    StoreReplaceKeyValueValueTest<K, V> testSuite =
      new StoreReplaceKeyValueValueTest<>(getStoreFactory());
    testSuite.runTestSuite().reportAndThrow();
  }

  @Test
  public void testClear() throws Exception {
    StoreClearTest<K, V> testSuite = new StoreClearTest<>(getStoreFactory());
    testSuite.runTestSuite().reportAndThrow();
  }

  @Test
  public void testClose() throws Exception {
    StoreCloseTest<K, V> testSuite = new StoreCloseTest<>(getStoreFactory());
    testSuite.runTestSuite().reportAndThrow();
  }

  @Test
  public void testIterator() throws Exception {
    StoreIteratorTest<K, V> testSuite = new StoreIteratorTest<>(getStoreFactory());
    testSuite.runTestSuite().reportAndThrow();
  }

  @Test
  public void testValueHolderValue() throws Exception {
    StoreValueHolderValueTest<K, V> testSuite = new StoreValueHolderValueTest<>(getStoreFactory());
    testSuite.runTestSuite().reportAndThrow();
  }

  @Test
  public void testValueHolderCreationTime() throws Exception {
    StoreValueHolderCreationTimeTest<K, V> testSuite =
      new StoreValueHolderCreationTimeTest<>(getStoreFactory());
    testSuite.runTestSuite().reportAndThrow();
  }

  @Test
  public void testValueHolderLastAccessTime() throws Exception {
    StoreValueHolderLastAccessTimeTest<K, V> testSuite =
      new StoreValueHolderLastAccessTimeTest<>(getStoreFactory());
    testSuite.runTestSuite().reportAndThrow();
  }

  @Test
  public void testIteratorHasNext() throws Exception {
    StoreIteratorHasNextTest<K, V> testSuite =
      new StoreIteratorHasNextTest<>(getStoreFactory());
    testSuite.runTestSuite().reportAndThrow();
  }

  @Test
  public void testIteratorNext() throws Exception {
    StoreIteratorNextTest<K, V> testSuite =
      new StoreIteratorNextTest<>(getStoreFactory());
    testSuite.runTestSuite().reportAndThrow();
  }

  @Test
  public void testBulkCompute() throws Exception {
    StoreBulkComputeTest<K, V> testSuite =
      new StoreBulkComputeTest<>(getStoreFactory());
    testSuite.runTestSuite().reportAndThrow();
  }

  @Test
  public void testBulkComputeIfAbsent() throws Exception {
    StoreBulkComputeIfAbsentTest<K, V> testSuite =
      new StoreBulkComputeIfAbsentTest<>(getStoreFactory());
    testSuite.runTestSuite().reportAndThrow();
  }

  @Test
  public void testStoreEvictionEventListener() throws Exception {
    StoreEvictionEventListenerTest<K, V> testSuite =
      new StoreEvictionEventListenerTest<>(getStoreFactory());
    testSuite.runTestSuite().reportAndThrow();
  }

  @Test
  public void testStoreExpiryEventListener() throws Exception {
    StoreExpiryEventListenerTest<K, V> testSuite =
      new StoreExpiryEventListenerTest<>(getStoreFactory());
    testSuite.runTestSuite().reportAndThrow();
  }

  @Test
  public void testStoreCreationEventListener() throws Exception {
    StoreCreationEventListenerTest<K, V> testSuite = new StoreCreationEventListenerTest<>(getStoreFactory());
    testSuite.runTestSuite().reportAndThrow();
  }

  @Test
  public void testStoreUpdateEventListener() throws Exception {
    StoreUpdateEventListenerTest<K, V> testSuite = new StoreUpdateEventListenerTest<>(getStoreFactory());
    testSuite.runTestSuite().reportAndThrow();
  }

  @Test
  public void testStoreRemovalEventListener() throws Exception {
    StoreRemovalEventListenerTest<K, V> testSuite = new StoreRemovalEventListenerTest<>(getStoreFactory());
    testSuite.runTestSuite().reportAndThrow();
  }
}
