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
package org.ehcache.core;

import java.util.Collections;
import java.util.EnumSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.ehcache.Status;
import org.ehcache.core.internal.resilience.RobustLoaderWriterResilienceStrategy;
import org.ehcache.core.resilience.DefaultRecoveryStore;
import org.ehcache.core.spi.store.Store;
import org.ehcache.core.statistics.CacheOperationOutcomes;
import org.ehcache.spi.loaderwriter.BulkCacheWritingException;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.ehcache.core.statistics.BulkOps;
import org.ehcache.spi.loaderwriter.CacheWritingException;
import org.ehcache.spi.resilience.StoreAccessException;
import org.hamcrest.Matchers;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.slf4j.LoggerFactory;

import static org.ehcache.core.EhcacheBasicBulkUtil.KEY_SET_A;
import static org.ehcache.core.EhcacheBasicBulkUtil.KEY_SET_B;
import static org.ehcache.core.EhcacheBasicBulkUtil.KEY_SET_C;
import static org.ehcache.core.EhcacheBasicBulkUtil.KEY_SET_D;
import static org.ehcache.core.EhcacheBasicBulkUtil.copyWithout;
import static org.ehcache.core.EhcacheBasicBulkUtil.fanIn;
import static org.ehcache.core.EhcacheBasicBulkUtil.getEntryMap;
import static org.ehcache.core.EhcacheBasicBulkUtil.union;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isIn;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.ehcache.core.EhcacheBasicRemoveAllTest.getAnyStringSet;
import static org.ehcache.core.EhcacheBasicRemoveAllTest.getAnyEntryIterableFunction;
import static org.ehcache.core.EhcacheBasicRemoveAllTest.getAnyStringIterable;

/**
 * @author Abhilash
 *
 */
public class EhcacheWithLoaderWriterBasicRemoveAllTest extends EhcacheBasicCrudBase {

  @Mock
  protected CacheLoaderWriter<String, String> cacheLoaderWriter;

  /**
   * A Mockito {@code ArgumentCaptor} for the {@code Set} argument to the
   * {@link Store#bulkCompute(Set, java.util.function.Function, java.util.function.Supplier)
   *    Store.bulkCompute(Set, Function, NullaryFunction} method.
   */
  @Captor
  protected ArgumentCaptor<Set<String>> bulkComputeSetCaptor;

  /**
   * Tests {@link EhcacheWithLoaderWriter#removeAll(Set)} for
   * <ul>
   *    <li>empty request set</li>
   *    <li>populated {@code Store} (keys not relevant)</li>
   *    <li>populated {@code CacheLoaderWriter} (keys not relevant)</li>
   * </ul>
   */
  @Test
  public void testRemoveAllEmptyRequestWithWriter() throws Exception {
    Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);

    FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalStoreContent);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);

    EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);
    ehcache.removeAll(Collections.<String>emptySet());

    verify(this.store, never()).bulkCompute(eq(Collections.<String>emptySet()), getAnyEntryIterableFunction());
    assertThat(fakeStore.getEntryMap(), equalTo(originalStoreContent));
    assertThat(fakeLoaderWriter.getEntryMap(), equalTo(originalStoreContent));
    verify(this.cacheLoaderWriter, never()).deleteAll(eq(Collections.<String>emptySet()));
    verify(this.resilienceStrategy, never()).removeAllFailure(eq(Collections.<String>emptySet()), any(StoreAccessException.class));


    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.RemoveOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.RemoveAllOutcome.SUCCESS));
    assertThat(ehcache.getBulkMethodEntries().get(BulkOps.REMOVE_ALL).intValue(), is(0));
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#removeAll(Set)} for
   * <ul>
   *    <li>non-empty request set</li>
   *    <li>populated {@code Store} - some keys overlap request</li>
   *    <li>populated {@code CacheLoaderWriter} - no keys overlap</li>
   *    <li>no {@link CacheLoaderWriter#deleteAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testRemoveAllStoreSomeOverlapWriterNoOverlapNoneFail() throws Exception {
    Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);

    Map<String, String> originalWriterContent = getEntryMap(KEY_SET_B, KEY_SET_D);
    FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);

    EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    Set<String> contentUpdates = fanIn(KEY_SET_A, KEY_SET_C);
    ehcache.removeAll(contentUpdates);

    verify(this.store, atLeast(1)).bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), equalTo(contentUpdates));
    assertThat(fakeStore.getEntryMap(), equalTo(copyWithout(originalStoreContent, contentUpdates)));
    assertThat(fakeLoaderWriter.getEntryMap(), equalTo(copyWithout(originalWriterContent, contentUpdates)));
    verifyZeroInteractions(this.resilienceStrategy);

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.RemoveOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.RemoveAllOutcome.SUCCESS));
    assertThat(ehcache.getBulkMethodEntries().get(BulkOps.REMOVE_ALL).intValue(), is(KEY_SET_A.size()));
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#removeAll(Set)} for
   * <ul>
   *    <li>non-empty request set</li>
   *    <li>populated {@code Store} - some keys overlap request</li>
   *    <li>{@link Store#bulkCompute} throws before accessing writer</li>
   *    <li>populated {@code CacheLoaderWriter} - no keys overlap</li>
   *    <li>no {@link CacheLoaderWriter#deleteAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testRemoveAllStoreSomeOverlapStoreAccessExceptionBeforeWriterNoOverlapNoneFail() throws Exception {
    Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);
    doThrow(new StoreAccessException("")).when(this.store)
        .bulkCompute(getAnyStringSet(), getAnyEntryIterableFunction());

    Map<String, String> originalWriterContent = getEntryMap(KEY_SET_B, KEY_SET_D);
    FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);

    EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    Set<String> contentUpdates = fanIn(KEY_SET_A, KEY_SET_C);
    ehcache.removeAll(contentUpdates);

    InOrder ordered = inOrder(this.store, this.cacheLoaderWriter, this.resilienceStrategy);
    ordered.verify(this.store, atLeast(1)).bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), everyItem(isIn(contentUpdates)));
    // ResilienceStrategy invoked; no assertions about Store content
    ordered.verify(this.resilienceStrategy).removeAllFailure(eq(contentUpdates), any(StoreAccessException.class));
    ordered.verify(this.cacheLoaderWriter, atLeast(1)).deleteAll(getAnyStringIterable());
    assertThat(fakeLoaderWriter.getEntryMap(), equalTo(copyWithout(originalWriterContent, contentUpdates)));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.RemoveOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.RemoveAllOutcome.FAILURE));
    assertThat(ehcache.getBulkMethodEntries().get(BulkOps.REMOVE_ALL).intValue(), is(0));
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#removeAll(Set)} for
   * <ul>
   *    <li>non-empty request set</li>
   *    <li>populated {@code Store} - some keys overlap request</li>
   *    <li>{@link Store#bulkCompute} throws after accessing writer</li>
   *    <li>populated {@code CacheLoaderWriter} - no keys overlap</li>
   *    <li>no {@link CacheLoaderWriter#deleteAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testRemoveAllStoreSomeOverlapStoreAccessExceptionAfterWriterNoOverlapNoneFail() throws Exception {
    Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    FakeStore fakeStore = new FakeStore(originalStoreContent, Collections.singleton("keyA3"));
    this.store = spy(fakeStore);

    Map<String, String> originalWriterContent = getEntryMap(KEY_SET_B, KEY_SET_D);
    FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);

    EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    Set<String> contentUpdates = fanIn(KEY_SET_A, KEY_SET_C);
    ehcache.removeAll(contentUpdates);

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.RemoveOutcome.class));

    InOrder ordered = inOrder(this.store, this.cacheLoaderWriter, this.resilienceStrategy);
    ordered.verify(this.store, atLeast(1))
        .bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), everyItem(isIn(contentUpdates)));
    // ResilienceStrategy invoked; no assertions about Store content
    ordered.verify(this.resilienceStrategy).removeAllFailure(eq(contentUpdates), any(StoreAccessException.class));
    ordered.verify(this.cacheLoaderWriter, atLeast(1)).deleteAll(getAnyStringIterable());
    assertThat(fakeLoaderWriter.getEntryMap(), equalTo(copyWithout(originalWriterContent, contentUpdates)));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.RemoveOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.RemoveAllOutcome.FAILURE));
    assertThat(ehcache.getBulkMethodEntries().get(BulkOps.REMOVE_ALL).intValue(), is(0));
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#removeAll(Set)} for
   * <ul>
   *    <li>non-empty request set</li>
   *    <li>populated {@code Store} - some keys overlap request</li>
   *    <li>populated {@code CacheLoaderWriter} - no keys overlap</li>
   *    <li>some {@link CacheLoaderWriter#deleteAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testRemoveAllStoreSomeOverlapWriterNoOverlapSomeFail() throws Exception {
    Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);

    Map<String, String> originalWriterContent = getEntryMap(KEY_SET_B, KEY_SET_D);
    FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent, KEY_SET_C);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);

    EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    Set<String> contentUpdates = fanIn(KEY_SET_A, KEY_SET_C);
    Set<String> expectedFailures = KEY_SET_C;
    Set<String> expectedSuccesses = copyWithout(contentUpdates, expectedFailures);
    try {
      ehcache.removeAll(contentUpdates);
      fail();
    } catch (BulkCacheWritingException e) {
      // Expected
      assertThat(e.getSuccesses(), Matchers.<Set<?>>equalTo(expectedSuccesses));
      assertThat(e.getFailures().keySet(), Matchers.<Set<?>>equalTo(expectedFailures));
    }

    verify(this.store, atLeast(1)).bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), equalTo(contentUpdates));
    assertThat(fakeStore.getEntryMap(), equalTo(copyWithout(originalStoreContent, expectedSuccesses)));
    assertThat(fakeLoaderWriter.getEntryMap(), equalTo(copyWithout(originalWriterContent, expectedSuccesses)));
    verifyZeroInteractions(this.resilienceStrategy);

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.RemoveOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.RemoveAllOutcome.FAILURE));
    assertThat(ehcache.getBulkMethodEntries().get(BulkOps.REMOVE_ALL).intValue(), is(KEY_SET_A.size()));
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#removeAll(Set)} for
   * <ul>
   *    <li>non-empty request set</li>
   *    <li>populated {@code Store} - some keys overlap request</li>
   *    <li>{@link Store#bulkCompute} throws before accessing writer</li>
   *    <li>populated {@code CacheLoaderWriter} - no keys overlap</li>
   *    <li>some {@link CacheLoaderWriter#deleteAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testRemoveAllStoreSomeOverlapStoreAccessExceptionBeforeWriterNoOverlapSomeFail() throws Exception {
    Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);
    doThrow(new StoreAccessException("")).when(this.store)
        .bulkCompute(getAnyStringSet(), getAnyEntryIterableFunction());

    Map<String, String> originalWriterContent = getEntryMap(KEY_SET_B, KEY_SET_D);
    FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent, KEY_SET_C);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);

    EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    Set<String> contentUpdates = fanIn(KEY_SET_A, KEY_SET_C);
    Set<String> expectedFailures = KEY_SET_C;
    Set<String> expectedSuccesses = copyWithout(contentUpdates, expectedFailures);
    try {
      ehcache.removeAll(contentUpdates);
      fail();
    } catch (CacheWritingException e) {
      // Expected
    }

    InOrder ordered = inOrder(this.store, this.cacheLoaderWriter, this.resilienceStrategy);
    ordered.verify(this.store, atLeast(1)).bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), everyItem(isIn(contentUpdates)));
    // ResilienceStrategy invoked; no assertions about Store content

    ordered.verify(this.resilienceStrategy).removeAllFailure(eq(contentUpdates), any(StoreAccessException.class));
    ordered.verify(this.cacheLoaderWriter, atLeast(1)).deleteAll(getAnyStringIterable());
    assertThat(fakeLoaderWriter.getEntryMap(), equalTo(copyWithout(originalWriterContent, expectedSuccesses)));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.RemoveOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.RemoveAllOutcome.FAILURE));
    assertThat(ehcache.getBulkMethodEntries().get(BulkOps.REMOVE_ALL).intValue(), is(0));
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#removeAll(Set)} for
   * <ul>
   *    <li>non-empty request set</li>
   *    <li>populated {@code Store} - some keys overlap request</li>
   *    <li>{@link Store#bulkCompute} throws after accessing writer</li>
   *    <li>populated {@code CacheLoaderWriter} - no keys overlap</li>
   *    <li>some {@link CacheLoaderWriter#deleteAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testRemoveAllStoreSomeOverlapStoreAccessExceptionAfterWriterNoOverlapSomeFail() throws Exception {
    Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    FakeStore fakeStore = new FakeStore(originalStoreContent, Collections.singleton("keyA3"));
    this.store = spy(fakeStore);

    Map<String, String> originalWriterContent = getEntryMap(KEY_SET_B, KEY_SET_D);
    FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent, KEY_SET_C);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);

    EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    Set<String> contentUpdates = fanIn(KEY_SET_A, KEY_SET_C);
    Set<String> expectedFailures = KEY_SET_C;
    Set<String> expectedSuccesses = copyWithout(contentUpdates, expectedFailures);
    try {
      ehcache.removeAll(contentUpdates);
      fail();
    } catch (CacheWritingException e) {
      // Expected
    }

    InOrder ordered = inOrder(this.store, this.cacheLoaderWriter, this.resilienceStrategy);
    ordered.verify(this.store, atLeast(1)).bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), everyItem(isIn(contentUpdates)));
    // ResilienceStrategy invoked; no assertions about Store content

    ordered.verify(this.resilienceStrategy).removeAllFailure(eq(contentUpdates), any(StoreAccessException.class));
    ordered.verify(this.cacheLoaderWriter, atLeast(1)).deleteAll(getAnyStringIterable());
    assertThat(fakeLoaderWriter.getEntryMap(), equalTo(copyWithout(originalWriterContent, expectedSuccesses)));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.RemoveOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.RemoveAllOutcome.FAILURE));
    assertThat(ehcache.getBulkMethodEntries().get(BulkOps.REMOVE_ALL).intValue(), is(0));
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#removeAll(Set)} for
   * <ul>
   *    <li>non-empty request set</li>
   *    <li>populated {@code Store} - some keys overlap request</li>
   *    <li>populated {@code CacheLoaderWriter} - no keys overlap</li>
   *    <li>some {@link CacheLoaderWriter#deleteAll(Iterable)} calls fail</li>
   *    <li>at least one {@link CacheLoaderWriter#deleteAll(Iterable)} call aborts</li>
   * </ul>
   */
  @Test
  public void testRemoveAllStoreSomeOverlapWriterNoOverlapSomeFailWithAbort() throws Exception {
    Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);

    Map<String, String> originalWriterContent = getEntryMap(KEY_SET_B, KEY_SET_D);
    FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent, KEY_SET_C);
    fakeLoaderWriter.setCompleteFailureKey("keyC4");
    this.cacheLoaderWriter = spy(fakeLoaderWriter);

    EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    Set<String> contentUpdates = fanIn(KEY_SET_A, KEY_SET_C);
    Set<String> expectedFailures = KEY_SET_C;
    Set<String> expectedSuccesses = copyWithout(contentUpdates, expectedFailures);
    try {
      ehcache.removeAll(contentUpdates);
      fail();
    } catch (BulkCacheWritingException e) {
      // Expected
      assertThat(e.getSuccesses(), Matchers.<Set<?>>equalTo(expectedSuccesses));
      assertThat(e.getFailures().keySet(), Matchers.<Set<?>>equalTo(expectedFailures));
    }

    verify(this.store, atLeast(1)).bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), everyItem(isIn(contentUpdates)));
    assertThat(fakeStore.getEntryMap(), equalTo(copyWithout(originalStoreContent, expectedSuccesses)));
    verify(this.cacheLoaderWriter, atLeast(1)).deleteAll(getAnyStringIterable());
    assertThat(fakeLoaderWriter.getEntryMap(), equalTo(copyWithout(originalWriterContent, expectedSuccesses)));
    verifyZeroInteractions(this.resilienceStrategy);

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.RemoveOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.RemoveAllOutcome.FAILURE));
    assertThat(ehcache.getBulkMethodEntries().get(BulkOps.REMOVE_ALL).intValue(), is(KEY_SET_A.size()));
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#removeAll(Set)} for
   * <ul>
   *    <li>non-empty request set</li>
   *    <li>populated {@code Store} - some keys overlap request</li>
   *    <li>{@link Store#bulkCompute} throws before accessing writer</li>
   *    <li>populated {@code CacheLoaderWriter} - no keys overlap</li>
   *    <li>some {@link CacheLoaderWriter#deleteAll(Iterable)} calls fail</li>
   *    <li>at least one {@link CacheLoaderWriter#deleteAll(Iterable)} call aborts</li>
   * </ul>
   */
  @Test
  public void testRemoveAllStoreSomeOverlapStoreAccessExceptionBeforeWriterNoOverlapSomeFailWithAbort() throws Exception {
    Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);
    doThrow(new StoreAccessException("")).when(this.store)
        .bulkCompute(getAnyStringSet(), getAnyEntryIterableFunction());

    Map<String, String> originalWriterContent = getEntryMap(KEY_SET_B, KEY_SET_D);
    FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent, KEY_SET_C);
    fakeLoaderWriter.setCompleteFailureKey("keyC4");
    this.cacheLoaderWriter = spy(fakeLoaderWriter);

    EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    Set<String> contentUpdates = fanIn(KEY_SET_A, KEY_SET_C);
    Set<String> expectedFailures = KEY_SET_C;
    Set<String> expectedSuccesses = copyWithout(contentUpdates, expectedFailures);
    try {
      ehcache.removeAll(contentUpdates);
      fail();
    } catch (CacheWritingException e) {
      // Expected
    }

    InOrder ordered = inOrder(this.store, this.cacheLoaderWriter, this.resilienceStrategy);
    ordered.verify(this.store, atLeast(1)).bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), everyItem(isIn(contentUpdates)));
    // ResilienceStrategy invoked; no assertions about Store content
    ordered.verify(this.resilienceStrategy).removeAllFailure(eq(contentUpdates), any(StoreAccessException.class));
    ordered.verify(this.cacheLoaderWriter, atLeast(1)).deleteAll(getAnyStringIterable());

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.RemoveOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.RemoveAllOutcome.FAILURE));
    assertThat(ehcache.getBulkMethodEntries().get(BulkOps.REMOVE_ALL).intValue(), is(0));
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#removeAll(Set)} for
   * <ul>
   *    <li>non-empty request set</li>
   *    <li>populated {@code Store} - some keys overlap request</li>
   *    <li>{@link Store#bulkCompute} throws after accessing writer</li>
   *    <li>populated {@code CacheLoaderWriter} - no keys overlap</li>
   *    <li>some {@link CacheLoaderWriter#deleteAll(Iterable)} calls fail</li>
   *    <li>at least one {@link CacheLoaderWriter#deleteAll(Iterable)} call aborts</li>
   * </ul>
   */
  @Test
  public void testRemoveAllStoreSomeOverlapStoreAccessExceptionAfterWriterNoOverlapSomeFailWithAbort() throws Exception {
    Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    FakeStore fakeStore = new FakeStore(originalStoreContent, Collections.singleton("keyA3"));
    this.store = spy(fakeStore);

    Map<String, String> originalWriterContent = getEntryMap(KEY_SET_B, KEY_SET_D);
    FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent, KEY_SET_C);
    fakeLoaderWriter.setCompleteFailureKey("keyC4");
    this.cacheLoaderWriter = spy(fakeLoaderWriter);

    EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    Set<String> contentUpdates = fanIn(KEY_SET_A, KEY_SET_C);
    Set<String> expectedFailures = KEY_SET_C;
    Set<String> expectedSuccesses = copyWithout(contentUpdates, expectedFailures);
    try {
      ehcache.removeAll(contentUpdates);
      fail();
    } catch (CacheWritingException e) {
      // Expected
    }

    InOrder ordered = inOrder(this.store, this.cacheLoaderWriter, this.resilienceStrategy);
    ordered.verify(this.store, atLeast(1)).bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), everyItem(isIn(contentUpdates)));
    // ResilienceStrategy invoked; no assertions about Store content
    ordered.verify(this.resilienceStrategy).removeAllFailure(eq(contentUpdates), any(StoreAccessException.class));
    ordered.verify(this.cacheLoaderWriter, atLeast(1)).deleteAll(getAnyStringIterable());

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.RemoveOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.RemoveAllOutcome.FAILURE));
    assertThat(ehcache.getBulkMethodEntries().get(BulkOps.REMOVE_ALL).intValue(), is(0));
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#removeAll(Set)} for
   * <ul>
   *    <li>non-empty request set</li>
   *    <li>populated {@code Store} - some keys overlap request</li>
   *    <li>populated {@code CacheLoaderWriter} - no keys overlap</li>
   *    <li>all {@link CacheLoaderWriter#deleteAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testRemoveAllStoreSomeOverlapWriterNoOverlapAllFail() throws Exception {
    Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);

    Map<String, String> originalWriterContent = getEntryMap(KEY_SET_B, KEY_SET_D);
    FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);
    doThrow(new Exception("deleteAll failed")).when(this.cacheLoaderWriter).deleteAll(getAnyStringIterable());

    EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    Set<String> contentUpdates = fanIn(KEY_SET_A, KEY_SET_C);
    try {
      ehcache.removeAll(contentUpdates);
      fail();
    } catch (BulkCacheWritingException e) {
      // Expected
      assertThat(e.getSuccesses(), empty());
      assertThat(e.getFailures().keySet(), Matchers.<Set<?>>equalTo(contentUpdates));
    }

    verify(this.store, atLeast(1)).bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), everyItem(isIn(contentUpdates)));
    assertThat(fakeStore.getEntryMap(), equalTo(copyWithout(originalStoreContent, contentUpdates)));
    verify(this.cacheLoaderWriter, atLeast(1)).deleteAll(getAnyStringIterable());
    verifyZeroInteractions(this.resilienceStrategy);

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.RemoveOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.RemoveAllOutcome.FAILURE));
    assertThat(ehcache.getBulkMethodEntries().get(BulkOps.REMOVE_ALL).intValue(), is(0));
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#removeAll(Set)} for
   * <ul>
   *    <li>non-empty request set</li>
   *    <li>populated {@code Store} - some keys overlap request</li>
   *    <li>{@link Store#bulkCompute} throws before accessing writer</li>
   *    <li>populated {@code CacheLoaderWriter} - no keys overlap</li>
   *    <li>all {@link CacheLoaderWriter#deleteAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testRemoveAllStoreSomeOverlapStoreAccessExceptionBeforeWriterNoOverlapAllFail() throws Exception {
    Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);
    doThrow(new StoreAccessException("")).when(this.store)
        .bulkCompute(getAnyStringSet(), getAnyEntryIterableFunction());

    Map<String, String> originalWriterContent = getEntryMap(KEY_SET_B, KEY_SET_D);
    FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);
    doThrow(new Exception("deleteAll failed")).when(this.cacheLoaderWriter).deleteAll(getAnyStringIterable());

    EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    Set<String> contentUpdates = fanIn(KEY_SET_A, KEY_SET_C);
    try {
      ehcache.removeAll(contentUpdates);
      fail();
    } catch (CacheWritingException e) {
      // Expected
    }

    InOrder ordered = inOrder(this.store, this.cacheLoaderWriter, this.resilienceStrategy);
    ordered.verify(this.store, atLeast(1)).bulkCompute(this.bulkComputeSetCaptor.capture(),
        getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), everyItem(isIn(contentUpdates)));
    // ResilienceStrategy invoked; no assertions about Store content
    ordered.verify(this.resilienceStrategy).removeAllFailure(eq(contentUpdates), any(StoreAccessException.class));
    ordered.verify(this.cacheLoaderWriter, atLeast(1)).deleteAll(getAnyStringIterable());

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.RemoveOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.RemoveAllOutcome.FAILURE));
    assertThat(ehcache.getBulkMethodEntries().get(BulkOps.REMOVE_ALL).intValue(), is(0));
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#removeAll(Set)} for
   * <ul>
   *    <li>non-empty request set</li>
   *    <li>populated {@code Store} - some keys overlap request</li>
   *    <li>{@link Store#bulkCompute} throws after accessing writer</li>
   *    <li>populated {@code CacheLoaderWriter} - no keys overlap</li>
   *    <li>all {@link CacheLoaderWriter#deleteAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testRemoveAllStoreSomeOverlapStoreAccessExceptionAfterWriterNoOverlapAllFail() throws Exception {
    Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    FakeStore fakeStore = new FakeStore(originalStoreContent, Collections.singleton("keyA3"));
    this.store = spy(fakeStore);

    Map<String, String> originalWriterContent = getEntryMap(KEY_SET_B, KEY_SET_D);
    FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);
    doThrow(new Exception("deleteAll failed")).when(this.cacheLoaderWriter).deleteAll(getAnyStringIterable());

    EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    Set<String> contentUpdates = fanIn(KEY_SET_A, KEY_SET_C);
    try {
      ehcache.removeAll(contentUpdates);
      fail();
    } catch (CacheWritingException e) {
      // Expected
    }

    InOrder ordered = inOrder(this.store, this.cacheLoaderWriter, this.resilienceStrategy);
    ordered.verify(this.store, atLeast(1)).bulkCompute(this.bulkComputeSetCaptor.capture(),
        getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), everyItem(isIn(contentUpdates)));
    // ResilienceStrategy invoked; no assertions about Store content
    ordered.verify(this.resilienceStrategy).removeAllFailure(eq(contentUpdates), any(StoreAccessException.class));
    ordered.verify(this.cacheLoaderWriter, atLeast(1)).deleteAll(getAnyStringIterable());

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.RemoveOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.RemoveAllOutcome.FAILURE));
    assertThat(ehcache.getBulkMethodEntries().get(BulkOps.REMOVE_ALL).intValue(), is(0));
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#removeAll(Set)} for
   * <ul>
   *    <li>non-empty request set</li>
   *    <li>populated {@code Store} - some keys overlap request</li>
   *    <li>populated {@code CacheLoaderWriter} - some keys overlap</li>
   *    <li>no {@link CacheLoaderWriter#deleteAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testRemoveAllStoreSomeOverlapWriterSomeOverlapNoneFail() throws Exception {
    Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);

    Map<String, String> originalWriterContent = getEntryMap(KEY_SET_B, KEY_SET_C);
    FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);

    EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    Set<String> contentUpdates = fanIn(KEY_SET_A, KEY_SET_C, KEY_SET_D);
    ehcache.removeAll(contentUpdates);

    verify(this.store, atLeast(1)).bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), equalTo(contentUpdates));
    assertThat(fakeStore.getEntryMap(), equalTo(copyWithout(originalStoreContent, contentUpdates)));
    verify(this.cacheLoaderWriter, atLeast(1)).deleteAll(getAnyStringIterable());
    assertThat(fakeLoaderWriter.getEntryMap(), equalTo(copyWithout(originalWriterContent, contentUpdates)));
    verifyZeroInteractions(this.resilienceStrategy);

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.RemoveOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.RemoveAllOutcome.SUCCESS));
    assertThat(ehcache.getBulkMethodEntries().get(BulkOps.REMOVE_ALL).intValue(), is(KEY_SET_A.size()));
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#removeAll(Set)} for
   * <ul>
   *    <li>non-empty request set</li>
   *    <li>populated {@code Store} - some keys overlap request</li>
   *    <li>{@link Store#bulkCompute} throws before accessing writer</li>
   *    <li>populated {@code CacheLoaderWriter} - some keys overlap</li>
   *    <li>no {@link CacheLoaderWriter#deleteAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testRemoveAllStoreSomeOverlapStoreAccessExceptionBeforeWriterSomeOverlapNoneFail() throws Exception {
    Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);
    doThrow(new StoreAccessException("")).when(this.store)
        .bulkCompute(getAnyStringSet(), getAnyEntryIterableFunction());

    Map<String, String> originalWriterContent = getEntryMap(KEY_SET_B, KEY_SET_C);
    FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);

    EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    Set<String> contentUpdates = fanIn(KEY_SET_A, KEY_SET_C, KEY_SET_D);
    ehcache.removeAll(contentUpdates);

    InOrder ordered = inOrder(this.store, this.cacheLoaderWriter, this.resilienceStrategy);
    ordered.verify(this.store, atLeast(1))
        .bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), everyItem(isIn(contentUpdates)));
    // ResilienceStrategy invoked; no assertions about Store content
    ordered.verify(this.resilienceStrategy).removeAllFailure(eq(contentUpdates), any(StoreAccessException.class));
    ordered.verify(this.cacheLoaderWriter, atLeast(1)).deleteAll(getAnyStringIterable());
    assertThat(fakeLoaderWriter.getEntryMap(), equalTo(copyWithout(originalWriterContent, contentUpdates)));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.RemoveOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.RemoveAllOutcome.FAILURE));
    assertThat(ehcache.getBulkMethodEntries().get(BulkOps.REMOVE_ALL).intValue(), is(0));
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#removeAll(Set)} for
   * <ul>
   *    <li>non-empty request set</li>
   *    <li>populated {@code Store} - some keys overlap request</li>
   *    <li>{@link Store#bulkCompute} throws after accessing writer</li>
   *    <li>populated {@code CacheLoaderWriter} - some keys overlap</li>
   *    <li>no {@link CacheLoaderWriter#deleteAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testRemoveAllStoreSomeOverlapStoreAccessExceptionAfterWriterSomeOverlapNoneFail() throws Exception {
    Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    FakeStore fakeStore = new FakeStore(originalStoreContent, Collections.singleton("keyA3"));
    this.store = spy(fakeStore);

    Map<String, String> originalWriterContent = getEntryMap(KEY_SET_B, KEY_SET_C);
    FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);

    EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    Set<String> contentUpdates = fanIn(KEY_SET_A, KEY_SET_C, KEY_SET_D);
    ehcache.removeAll(contentUpdates);

    InOrder ordered = inOrder(this.store, this.cacheLoaderWriter, this.resilienceStrategy);
    ordered.verify(this.store, atLeast(1))
        .bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), everyItem(isIn(contentUpdates)));
    // ResilienceStrategy invoked; no assertions about Store content
    ordered.verify(this.resilienceStrategy).removeAllFailure(eq(contentUpdates), any(StoreAccessException.class));
    ordered.verify(this.cacheLoaderWriter, atLeast(1)).deleteAll(getAnyStringIterable());
    assertThat(fakeLoaderWriter.getEntryMap(), equalTo(copyWithout(originalWriterContent, contentUpdates)));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.RemoveOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.RemoveAllOutcome.FAILURE));
    assertThat(ehcache.getBulkMethodEntries().get(BulkOps.REMOVE_ALL).intValue(), is(0));
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#removeAll(Set)} for
   * <ul>
   *    <li>non-empty request set</li>
   *    <li>populated {@code Store} - some keys overlap request</li>
   *    <li>populated {@code CacheLoaderWriter} - some keys overlap</li>
   *    <li>some {@link CacheLoaderWriter#deleteAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testRemoveAllStoreSomeOverlapWriterSomeOverlapSomeFail() throws Exception {
    Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);

    Map<String, String> originalWriterContent = getEntryMap(KEY_SET_B, KEY_SET_C);
    FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent, KEY_SET_D);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);

    EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    Set<String> contentUpdates = fanIn(KEY_SET_A, KEY_SET_C, KEY_SET_D);
    Set<String> expectedFailures = KEY_SET_D;
    Set<String> expectedSuccesses = copyWithout(contentUpdates, expectedFailures);
    try {
      ehcache.removeAll(contentUpdates);
      fail();
    } catch (BulkCacheWritingException e) {
      // Expected
      assertThat(e.getSuccesses(), Matchers.<Set<?>>equalTo(expectedSuccesses));
      assertThat(e.getFailures().keySet(), Matchers.<Set<?>>equalTo(expectedFailures));
    }

    verify(this.store, atLeast(1)).bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), everyItem(isIn(contentUpdates)));
    assertThat(fakeStore.getEntryMap(), equalTo(copyWithout(originalStoreContent, expectedSuccesses)));
    verify(this.cacheLoaderWriter, atLeast(1)).deleteAll(getAnyStringIterable());
    assertThat(fakeLoaderWriter.getEntryMap(), equalTo(copyWithout(originalWriterContent, expectedSuccesses)));
    verifyZeroInteractions(this.resilienceStrategy);

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.RemoveOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.RemoveAllOutcome.FAILURE));
    assertThat(ehcache.getBulkMethodEntries().get(BulkOps.REMOVE_ALL).intValue(), is(KEY_SET_A.size()));
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#removeAll(Set)} for
   * <ul>
   *    <li>non-empty request set</li>
   *    <li>populated {@code Store} - some keys overlap request</li>
   *    <li>{@link Store#bulkCompute} throws before accessing writer</li>
   *    <li>populated {@code CacheLoaderWriter} - some keys overlap</li>
   *    <li>some {@link CacheLoaderWriter#deleteAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testRemoveAllStoreSomeOverlapStoreAccessExceptionBeforeWriterSomeOverlapSomeFail() throws Exception {
    Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);
    doThrow(new StoreAccessException("")).when(this.store)
        .bulkCompute(getAnyStringSet(), getAnyEntryIterableFunction());

    Map<String, String> originalWriterContent = getEntryMap(KEY_SET_B, KEY_SET_C);
    FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent, KEY_SET_D);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);

    EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    Set<String> contentUpdates = fanIn(KEY_SET_A, KEY_SET_C, KEY_SET_D);
    Set<String> expectedFailures = KEY_SET_D;
    Set<String> expectedSuccesses = copyWithout(contentUpdates, expectedFailures);
    try {
      ehcache.removeAll(contentUpdates);
      fail();
    } catch (CacheWritingException e) {
      // Expected
    }

    InOrder ordered = inOrder(this.store, this.cacheLoaderWriter, this.resilienceStrategy);
    ordered.verify(this.store, atLeast(1))
        .bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), everyItem(isIn(contentUpdates)));
    // ResilienceStrategy invoked; no assertions about Store content
    ordered.verify(this.resilienceStrategy).removeAllFailure(eq(contentUpdates), any(StoreAccessException.class));
    ordered.verify(this.cacheLoaderWriter, atLeast(1)).deleteAll(getAnyStringIterable());
    assertThat(fakeLoaderWriter.getEntryMap(), equalTo(copyWithout(originalWriterContent, expectedSuccesses)));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.RemoveOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.RemoveAllOutcome.FAILURE));
    assertThat(ehcache.getBulkMethodEntries().get(BulkOps.REMOVE_ALL).intValue(), is(0));
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#removeAll(Set)} for
   * <ul>
   *    <li>non-empty request set</li>
   *    <li>populated {@code Store} - some keys overlap request</li>
   *    <li>{@link Store#bulkCompute} throws after accessing writer</li>
   *    <li>populated {@code CacheLoaderWriter} - some keys overlap</li>
   *    <li>some {@link CacheLoaderWriter#deleteAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testRemoveAllStoreSomeOverlapStoreAccessExceptionAfterWriterSomeOverlapSomeFail() throws Exception {
    Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    FakeStore fakeStore = new FakeStore(originalStoreContent, Collections.singleton("keyA3"));
    this.store = spy(fakeStore);

    Map<String, String> originalWriterContent = getEntryMap(KEY_SET_B, KEY_SET_C);
    FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent, KEY_SET_D);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);

    EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    Set<String> contentUpdates = fanIn(KEY_SET_A, KEY_SET_C, KEY_SET_D);
    Set<String> expectedFailures = KEY_SET_D;
    Set<String> expectedSuccesses = copyWithout(contentUpdates, expectedFailures);
    try {
      ehcache.removeAll(contentUpdates);
      fail();
    } catch (CacheWritingException e) {
      // Expected
    }

    InOrder ordered = inOrder(this.store, this.cacheLoaderWriter, this.resilienceStrategy);
    ordered.verify(this.store, atLeast(1))
        .bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), everyItem(isIn(contentUpdates)));
    // ResilienceStrategy invoked; no assertions about Store content
    ordered.verify(this.resilienceStrategy).removeAllFailure(eq(contentUpdates), any(StoreAccessException.class));
    ordered.verify(this.cacheLoaderWriter, atLeast(1)).deleteAll(getAnyStringIterable());
    assertThat(fakeLoaderWriter.getEntryMap(), equalTo(copyWithout(originalWriterContent, expectedSuccesses)));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.RemoveOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.RemoveAllOutcome.FAILURE));
    assertThat(ehcache.getBulkMethodEntries().get(BulkOps.REMOVE_ALL).intValue(), is(0));
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#removeAll(Set)} for
   * <ul>
   *    <li>non-empty request set</li>
   *    <li>populated {@code Store} - some keys overlap request</li>
   *    <li>populated {@code CacheLoaderWriter} - some keys overlap</li>
   *    <li>some {@link CacheLoaderWriter#deleteAll(Iterable)} calls fail</li>
   *    <li>at least one {@link CacheLoaderWriter#deleteAll(Iterable)} call aborts</li>
   * </ul>
   */
  @Test
  public void testRemoveAllStoreSomeOverlapWriterSomeOverlapSomeFailWithAbort() throws Exception {
    Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);

    Map<String, String> originalWriterContent = getEntryMap(KEY_SET_B, KEY_SET_C);
    FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent, KEY_SET_D);
    fakeLoaderWriter.setCompleteFailureKey("keyC4");
    this.cacheLoaderWriter = spy(fakeLoaderWriter);

    EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    Set<String> contentUpdates = fanIn(KEY_SET_A, KEY_SET_C, KEY_SET_D);
    Set<String> expectedFailures = union(KEY_SET_D, Collections.singleton("keyC4"));
    Set<String> expectedSuccesses = copyWithout(contentUpdates, expectedFailures);
    try {
      ehcache.removeAll(contentUpdates);
      fail();
    } catch (BulkCacheWritingException e) {
      // Expected
      assertThat(e.getSuccesses(), Matchers.<Set<?>>equalTo(expectedSuccesses));
      assertThat(e.getFailures().keySet(), Matchers.<Set<?>>equalTo(expectedFailures));
    }

    verify(this.store, atLeast(1)).bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), everyItem(isIn(contentUpdates)));
    assertThat(fakeStore.getEntryMap(), equalTo(copyWithout(originalStoreContent, expectedSuccesses)));
    verify(this.cacheLoaderWriter, atLeast(1)).deleteAll(getAnyStringIterable());
    assertThat(fakeLoaderWriter.getEntryMap(), equalTo(copyWithout(originalWriterContent, expectedSuccesses)));
    verifyZeroInteractions(this.resilienceStrategy);

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.RemoveOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.RemoveAllOutcome.FAILURE));
    assertThat(ehcache.getBulkMethodEntries().get(BulkOps.REMOVE_ALL).intValue(), is(KEY_SET_A.size()));
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#removeAll(Set)} for
   * <ul>
   *    <li>non-empty request set</li>
   *    <li>populated {@code Store} - some keys overlap request</li>
   *    <li>{@link Store#bulkCompute} throws before accessing writer</li>
   *    <li>populated {@code CacheLoaderWriter} - some keys overlap</li>
   *    <li>some {@link CacheLoaderWriter#deleteAll(Iterable)} calls fail</li>
   *    <li>at least one {@link CacheLoaderWriter#deleteAll(Iterable)} call aborts</li>
   * </ul>
   */
  @Test
  public void testRemoveAllStoreSomeOverlapStoreAccessExceptionBeforeWriterSomeOverlapSomeFailWithAbort() throws Exception {
    Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);
    doThrow(new StoreAccessException("")).when(this.store)
        .bulkCompute(getAnyStringSet(), getAnyEntryIterableFunction());

    Map<String, String> originalWriterContent = getEntryMap(KEY_SET_B, KEY_SET_C);
    FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent, KEY_SET_D);
    fakeLoaderWriter.setCompleteFailureKey("keyC4");
    this.cacheLoaderWriter = spy(fakeLoaderWriter);

    EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    Set<String> contentUpdates = fanIn(KEY_SET_A, KEY_SET_C, KEY_SET_D);
    Set<String> expectedFailures = union(KEY_SET_D, Collections.singleton("keyC4"));
    Set<String> expectedSuccesses = copyWithout(contentUpdates, expectedFailures);
    try {
      ehcache.removeAll(contentUpdates);
      fail();
    } catch (CacheWritingException e) {
      // Expected
    }

    InOrder ordered = inOrder(this.store, this.cacheLoaderWriter, this.resilienceStrategy);
    ordered.verify(this.store, atLeast(1)).bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), everyItem(isIn(contentUpdates)));
    // ResilienceStrategy invoked; no assertions about Store content
    ordered.verify(this.resilienceStrategy).removeAllFailure(eq(contentUpdates), any(StoreAccessException.class));
    ordered.verify(this.cacheLoaderWriter, atLeast(1)).deleteAll(getAnyStringIterable());

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.RemoveOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.RemoveAllOutcome.FAILURE));
    assertThat(ehcache.getBulkMethodEntries().get(BulkOps.REMOVE_ALL).intValue(), is(0));
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#removeAll(Set)} for
   * <ul>
   *    <li>non-empty request set</li>
   *    <li>populated {@code Store} - some keys overlap request</li>
   *    <li>{@link Store#bulkCompute} throws after accessing writer</li>
   *    <li>populated {@code CacheLoaderWriter} - some keys overlap</li>
   *    <li>some {@link CacheLoaderWriter#deleteAll(Iterable)} calls fail</li>
   *    <li>at least one {@link CacheLoaderWriter#deleteAll(Iterable)} call aborts</li>
   * </ul>
   */
  @Test
  public void testRemoveAllStoreSomeOverlapStoreAccessExceptionAfterWriterSomeOverlapSomeFailWithAbort() throws Exception {
    Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    FakeStore fakeStore = new FakeStore(originalStoreContent, Collections.singleton("keyA3"));
    this.store = spy(fakeStore);

    Map<String, String> originalWriterContent = getEntryMap(KEY_SET_B, KEY_SET_C);
    FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent, KEY_SET_D);
    fakeLoaderWriter.setCompleteFailureKey("keyC4");
    this.cacheLoaderWriter = spy(fakeLoaderWriter);

    EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    Set<String> contentUpdates = fanIn(KEY_SET_A, KEY_SET_C, KEY_SET_D);
    Set<String> expectedFailures = union(KEY_SET_D, Collections.singleton("keyC4"));
    Set<String> expectedSuccesses = copyWithout(contentUpdates, expectedFailures);
    try {
      ehcache.removeAll(contentUpdates);
      fail();
    } catch (CacheWritingException e) {
      // Expected
    }

    InOrder ordered = inOrder(this.store, this.cacheLoaderWriter, this.resilienceStrategy);
    ordered.verify(this.store, atLeast(1)).bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), everyItem(isIn(contentUpdates)));
    // ResilienceStrategy invoked; no assertions about Store content
    ordered.verify(this.resilienceStrategy).removeAllFailure(eq(contentUpdates), any(StoreAccessException.class));
    ordered.verify(this.cacheLoaderWriter, atLeast(1)).deleteAll(getAnyStringIterable());

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.RemoveOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.RemoveAllOutcome.FAILURE));
    assertThat(ehcache.getBulkMethodEntries().get(BulkOps.REMOVE_ALL).intValue(), is(0));
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#removeAll(Set)} for
   * <ul>
   *    <li>non-empty request set</li>
   *    <li>populated {@code Store} - some keys overlap request</li>
   *    <li>populated {@code CacheLoaderWriter} - some keys overlap</li>
   *    <li>all {@link CacheLoaderWriter#deleteAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testRemoveAllStoreSomeOverlapWriterSomeOverlapAllFail() throws Exception {
    Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);

    Map<String, String> originalWriterContent = getEntryMap(KEY_SET_B, KEY_SET_C);
    FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);
    doThrow(new Exception("deleteAll failed")).when(this.cacheLoaderWriter).deleteAll(getAnyStringIterable());

    EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    Set<String> contentUpdates = fanIn(KEY_SET_A, KEY_SET_C, KEY_SET_D);
    try {
      ehcache.removeAll(contentUpdates);
      fail();
    } catch (BulkCacheWritingException e) {
      // Expected
      assertThat(e.getSuccesses(), empty());
      assertThat(e.getFailures().keySet(), Matchers.<Set<?>>equalTo(contentUpdates));
    }

    verify(this.store, atLeast(1)).bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), everyItem(isIn(contentUpdates)));
    assertThat(fakeStore.getEntryMap(), equalTo(copyWithout(originalStoreContent, contentUpdates)));
    verify(this.cacheLoaderWriter, atLeast(1)).deleteAll(getAnyStringIterable());
    verifyZeroInteractions(this.resilienceStrategy);

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.RemoveOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.RemoveAllOutcome.FAILURE));
    assertThat(ehcache.getBulkMethodEntries().get(BulkOps.REMOVE_ALL).intValue(), is(0));
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#removeAll(Set)} for
   * <ul>
   *    <li>non-empty request set</li>
   *    <li>populated {@code Store} - some keys overlap request</li>
   *    <li>{@link Store#bulkCompute} throws before accessing writer</li>
   *    <li>populated {@code CacheLoaderWriter} - some keys overlap</li>
   *    <li>all {@link CacheLoaderWriter#deleteAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testRemoveAllStoreSomeOverlapStoreAccessExceptionBeforeWriterSomeOverlapAllFail() throws Exception {
    Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);
    doThrow(new StoreAccessException("")).when(this.store)
        .bulkCompute(getAnyStringSet(), getAnyEntryIterableFunction());

    Map<String, String> originalWriterContent = getEntryMap(KEY_SET_B, KEY_SET_C);
    FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);
    doThrow(new Exception("deleteAll failed")).when(this.cacheLoaderWriter).deleteAll(getAnyStringIterable());

    EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    Set<String> contentUpdates = fanIn(KEY_SET_A, KEY_SET_C, KEY_SET_D);
    try {
      ehcache.removeAll(contentUpdates);
      fail();
    } catch (CacheWritingException e) {
      // Expected
    }

    InOrder ordered = inOrder(this.store, this.cacheLoaderWriter, this.resilienceStrategy);
    ordered.verify(this.store, atLeast(1))
        .bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), everyItem(isIn(contentUpdates)));
    // ResilienceStrategy invoked; no assertions about Store content
    ordered.verify(this.resilienceStrategy).removeAllFailure(eq(contentUpdates), any(StoreAccessException.class));
    ordered.verify(this.cacheLoaderWriter, atLeast(1)).deleteAll(getAnyStringIterable());

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.RemoveOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.RemoveAllOutcome.FAILURE));
    assertThat(ehcache.getBulkMethodEntries().get(BulkOps.REMOVE_ALL).intValue(), is(0));
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#removeAll(Set)} for
   * <ul>
   *    <li>non-empty request set</li>
   *    <li>populated {@code Store} - some keys overlap request</li>
   *    <li>{@link Store#bulkCompute} throws after accessing writer</li>
   *    <li>populated {@code CacheLoaderWriter} - some keys overlap</li>
   *    <li>all {@link CacheLoaderWriter#deleteAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testRemoveAllStoreSomeOverlapStoreAccessExceptionAfterWriterSomeOverlapAllFail() throws Exception {
    Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    FakeStore fakeStore = new FakeStore(originalStoreContent, Collections.singleton("keyA3"));
    this.store = spy(fakeStore);

    Map<String, String> originalWriterContent = getEntryMap(KEY_SET_B, KEY_SET_C);
    FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);
    doThrow(new Exception("deleteAll failed")).when(this.cacheLoaderWriter).deleteAll(getAnyStringIterable());

    EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    Set<String> contentUpdates = fanIn(KEY_SET_A, KEY_SET_C, KEY_SET_D);
    try {
      ehcache.removeAll(contentUpdates);
      fail();
    } catch (CacheWritingException e) {
      // Expected
    }

    InOrder ordered = inOrder(this.store, this.cacheLoaderWriter, this.resilienceStrategy);
    ordered.verify(this.store, atLeast(1))
        .bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), everyItem(isIn(contentUpdates)));
    // ResilienceStrategy invoked; no assertions about Store content
    ordered.verify(this.resilienceStrategy).removeAllFailure(eq(contentUpdates), any(StoreAccessException.class));
    ordered.verify(this.cacheLoaderWriter, atLeast(1)).deleteAll(getAnyStringIterable());

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.RemoveOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.RemoveAllOutcome.FAILURE));
    assertThat(ehcache.getBulkMethodEntries().get(BulkOps.REMOVE_ALL).intValue(), is(0));
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#removeAll(Set)} for
   * <ul>
   *    <li>non-empty request set</li>
   *    <li>populated {@code Store} - some keys overlap request</li>
   *    <li>populated {@code CacheLoaderWriter} - all keys overlap</li>
   *    <li>no {@link CacheLoaderWriter#deleteAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testRemoveAllStoreSomeOverlapWriterFullOverlapNoneFail() throws Exception {
    Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);

    Map<String, String> originalWriterContent = getEntryMap(KEY_SET_B, KEY_SET_C);
    FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);

    EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    Set<String> contentUpdates = fanIn(KEY_SET_B, KEY_SET_C);
    ehcache.removeAll(contentUpdates);

    verify(this.store, atLeast(1)).bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), equalTo(contentUpdates));
    assertThat(fakeStore.getEntryMap(), equalTo(copyWithout(originalStoreContent, contentUpdates)));
    verify(this.cacheLoaderWriter, atLeast(1)).deleteAll(getAnyStringIterable());
    assertThat(fakeLoaderWriter.getEntryMap(), equalTo(copyWithout(originalWriterContent, contentUpdates)));
    verifyZeroInteractions(this.resilienceStrategy);

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.RemoveOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.RemoveAllOutcome.SUCCESS));
    assertThat(ehcache.getBulkMethodEntries().get(BulkOps.REMOVE_ALL).intValue(), is(KEY_SET_B.size()));
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#removeAll(Set)} for
   * <ul>
   *    <li>non-empty request set</li>
   *    <li>populated {@code Store} - some keys overlap request</li>
   *    <li>{@link Store#bulkCompute} throws before accessing writer</li>
   *    <li>populated {@code CacheLoaderWriter} - all keys overlap</li>
   *    <li>no {@link CacheLoaderWriter#deleteAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testRemoveAllStoreSomeOverlapStoreAccessExceptionBeforeWriterFullOverlapNoneFail() throws Exception {
    Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);
    doThrow(new StoreAccessException("")).when(this.store)
        .bulkCompute(getAnyStringSet(), getAnyEntryIterableFunction());

    Map<String, String> originalWriterContent = getEntryMap(KEY_SET_B, KEY_SET_C);
    FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);

    EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    Set<String> contentUpdates = fanIn(KEY_SET_B, KEY_SET_C);
    ehcache.removeAll(contentUpdates);

    InOrder ordered = inOrder(this.store, this.cacheLoaderWriter, this.resilienceStrategy);
    ordered.verify(this.store, atLeast(1))
        .bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), everyItem(isIn(contentUpdates)));
    // ResilienceStrategy invoked; no assertions about Store content
    ordered.verify(this.resilienceStrategy).removeAllFailure(eq(contentUpdates), any(StoreAccessException.class));
    ordered.verify(this.cacheLoaderWriter, atLeast(1)).deleteAll(getAnyStringIterable());
    assertThat(fakeLoaderWriter.getEntryMap(), equalTo(copyWithout(originalWriterContent, contentUpdates)));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.RemoveOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.RemoveAllOutcome.FAILURE));
    assertThat(ehcache.getBulkMethodEntries().get(BulkOps.REMOVE_ALL).intValue(), is(0));
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#removeAll(Set)} for
   * <ul>
   *    <li>non-empty request set</li>
   *    <li>populated {@code Store} - some keys overlap request</li>
   *    <li>{@link Store#bulkCompute} throws after accessing writer</li>
   *    <li>populated {@code CacheLoaderWriter} - all keys overlap</li>
   *    <li>no {@link CacheLoaderWriter#deleteAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testRemoveAllStoreSomeOverlapStoreAccessExceptionAfterWriterFullOverlapNoneFail() throws Exception {
    Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    FakeStore fakeStore = new FakeStore(originalStoreContent, Collections.singleton("keyB3"));
    this.store = spy(fakeStore);

    Map<String, String> originalWriterContent = getEntryMap(KEY_SET_B, KEY_SET_C);
    FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);

    EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    Set<String> contentUpdates = fanIn(KEY_SET_B, KEY_SET_C);
    ehcache.removeAll(contentUpdates);

    InOrder ordered = inOrder(this.store, this.cacheLoaderWriter, this.resilienceStrategy);
    ordered.verify(this.store, atLeast(1))
        .bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), everyItem(isIn(contentUpdates)));
    // ResilienceStrategy invoked; no assertions about Store content
    ordered.verify(this.resilienceStrategy).removeAllFailure(eq(contentUpdates), any(StoreAccessException.class));
    ordered.verify(this.cacheLoaderWriter, atLeast(1)).deleteAll(getAnyStringIterable());
    assertThat(fakeLoaderWriter.getEntryMap(), equalTo(copyWithout(originalWriterContent, contentUpdates)));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.RemoveOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.RemoveAllOutcome.FAILURE));
    assertThat(ehcache.getBulkMethodEntries().get(BulkOps.REMOVE_ALL).intValue(), is(0));
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#removeAll(Set)} for
   * <ul>
   *    <li>non-empty request set</li>
   *    <li>populated {@code Store} - some keys overlap request</li>
   *    <li>populated {@code CacheLoaderWriter} - all keys overlap</li>
   *    <li>some {@link CacheLoaderWriter#deleteAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testRemoveAllStoreSomeOverlapWriterFullOverlapSomeFail() throws Exception {
    Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);

    Map<String, String> originalWriterContent = getEntryMap(KEY_SET_B, KEY_SET_C, KEY_SET_D);
    FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent, KEY_SET_D);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);

    EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    Set<String> contentUpdates = fanIn(KEY_SET_B, KEY_SET_C, KEY_SET_D);
    Set<String> expectedFailures = KEY_SET_D;
    Set<String> expectedSuccesses = copyWithout(contentUpdates, expectedFailures);
    try {
      ehcache.removeAll(contentUpdates);
      fail();
    } catch (BulkCacheWritingException e) {
      // Expected
      assertThat(e.getSuccesses(), Matchers.<Set<?>>equalTo(expectedSuccesses));
      assertThat(e.getFailures().keySet(), Matchers.<Set<?>>equalTo(expectedFailures));
    }

    verify(this.store, atLeast(1)).bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), everyItem(isIn(contentUpdates)));
    assertThat(fakeStore.getEntryMap(), equalTo(copyWithout(originalStoreContent, expectedSuccesses)));
    verify(this.cacheLoaderWriter, atLeast(1)).deleteAll(getAnyStringIterable());
    assertThat(fakeLoaderWriter.getEntryMap(), equalTo(copyWithout(originalWriterContent, expectedSuccesses)));
    verifyZeroInteractions(this.resilienceStrategy);

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.RemoveOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.RemoveAllOutcome.FAILURE));
    assertThat(ehcache.getBulkMethodEntries().get(BulkOps.REMOVE_ALL).intValue(), is(KEY_SET_B.size()));
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#removeAll(Set)} for
   * <ul>
   *    <li>non-empty request set</li>
   *    <li>populated {@code Store} - some keys overlap request</li>
   *    <li>{@link Store#bulkCompute} throws before accessing writer</li>
   *    <li>populated {@code CacheLoaderWriter} - all keys overlap</li>
   *    <li>some {@link CacheLoaderWriter#deleteAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testRemoveAllStoreSomeOverlapStoreAccessExceptionBeforeWriterFullOverlapSomeFail() throws Exception {
    Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);
    doThrow(new StoreAccessException("")).when(this.store)
        .bulkCompute(getAnyStringSet(), getAnyEntryIterableFunction());

    Map<String, String> originalWriterContent = getEntryMap(KEY_SET_B, KEY_SET_C, KEY_SET_D);
    FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent, KEY_SET_D);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);

    EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    Set<String> contentUpdates = fanIn(KEY_SET_B, KEY_SET_C, KEY_SET_D);
    Set<String> expectedFailures = KEY_SET_D;
    Set<String> expectedSuccesses = copyWithout(contentUpdates, expectedFailures);
    try {
      ehcache.removeAll(contentUpdates);
      fail();
    } catch (CacheWritingException e) {
      // Expected
    }

    InOrder ordered = inOrder(this.store, this.cacheLoaderWriter, this.resilienceStrategy);
    ordered.verify(this.store, atLeast(1))
        .bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), everyItem(isIn(contentUpdates)));
    // ResilienceStrategy invoked; no assertions about Store content
    ordered.verify(this.resilienceStrategy).removeAllFailure(eq(contentUpdates), any(StoreAccessException.class));
    ordered.verify(this.cacheLoaderWriter, atLeast(1)).deleteAll(getAnyStringIterable());
    assertThat(fakeLoaderWriter.getEntryMap(), equalTo(copyWithout(originalWriterContent, expectedSuccesses)));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.RemoveOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.RemoveAllOutcome.FAILURE));
    assertThat(ehcache.getBulkMethodEntries().get(BulkOps.REMOVE_ALL).intValue(), is(0));
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#removeAll(Set)} for
   * <ul>
   *    <li>non-empty request set</li>
   *    <li>populated {@code Store} - some keys overlap request</li>
   *    <li>{@link Store#bulkCompute} throws after accessing writer</li>
   *    <li>populated {@code CacheLoaderWriter} - all keys overlap</li>
   *    <li>some {@link CacheLoaderWriter#deleteAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testRemoveAllStoreSomeOverlapStoreAccessExceptionAfterWriterFullOverlapSomeFail() throws Exception {
    Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    FakeStore fakeStore = new FakeStore(originalStoreContent, Collections.singleton("keyB3"));
    this.store = spy(fakeStore);

    Map<String, String> originalWriterContent = getEntryMap(KEY_SET_B, KEY_SET_C, KEY_SET_D);
    FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent, KEY_SET_D);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);

    EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    Set<String> contentUpdates = fanIn(KEY_SET_B, KEY_SET_C, KEY_SET_D);
    Set<String> expectedFailures = KEY_SET_D;
    Set<String> expectedSuccesses = copyWithout(contentUpdates, expectedFailures);
    try {
      ehcache.removeAll(contentUpdates);
      fail();
    } catch (CacheWritingException e) {
      // Expected
    }

    InOrder ordered = inOrder(this.store, this.cacheLoaderWriter, this.resilienceStrategy);
    ordered.verify(this.store, atLeast(1))
        .bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), everyItem(isIn(contentUpdates)));
    // ResilienceStrategy invoked; no assertions about Store content
    ordered.verify(this.resilienceStrategy).removeAllFailure(eq(contentUpdates), any(StoreAccessException.class));
    ordered.verify(this.cacheLoaderWriter, atLeast(1)).deleteAll(getAnyStringIterable());
    assertThat(fakeLoaderWriter.getEntryMap(), equalTo(copyWithout(originalWriterContent, expectedSuccesses)));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.RemoveOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.RemoveAllOutcome.FAILURE));
    assertThat(ehcache.getBulkMethodEntries().get(BulkOps.REMOVE_ALL).intValue(), is(0));
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#removeAll(Set)} for
   * <ul>
   *    <li>non-empty request set</li>
   *    <li>populated {@code Store} - some keys overlap request</li>
   *    <li>populated {@code CacheLoaderWriter} - all keys overlap</li>
   *    <li>some {@link CacheLoaderWriter#deleteAll(Iterable)} calls fail</li>
   *    <li>at least one {@link CacheLoaderWriter#deleteAll(Iterable)} call aborts</li>
   * </ul>
   */
  @Test
  public void testRemoveAllStoreSomeOverlapWriterFullOverlapSomeFailWithAbort() throws Exception {
    Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);

    Map<String, String> originalWriterContent = getEntryMap(KEY_SET_B, KEY_SET_C, KEY_SET_D);
    FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent, KEY_SET_D);
    fakeLoaderWriter.setCompleteFailureKey("keyC4");
    this.cacheLoaderWriter = spy(fakeLoaderWriter);

    EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    Set<String> contentUpdates = fanIn(KEY_SET_B, KEY_SET_C, KEY_SET_D);
    Set<String> expectedFailures = union(KEY_SET_D, Collections.singleton("keyC4"));
    Set<String> expectedSuccesses = copyWithout(contentUpdates, expectedFailures);
    try {
      ehcache.removeAll(contentUpdates);
      fail();
    } catch (BulkCacheWritingException e) {
      // Expected
      assertThat(e.getSuccesses(), Matchers.<Set<?>>equalTo(expectedSuccesses));
      assertThat(e.getFailures().keySet(), Matchers.<Set<?>>equalTo(expectedFailures));
    }

    verify(this.store, atLeast(1))
        .bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), everyItem(isIn(contentUpdates)));
    assertThat(fakeStore.getEntryMap(), equalTo(copyWithout(originalStoreContent, expectedSuccesses)));
    verify(this.cacheLoaderWriter, atLeast(1)).deleteAll(getAnyStringIterable());
    assertThat(fakeLoaderWriter.getEntryMap(), equalTo(copyWithout(originalWriterContent, expectedSuccesses)));
    verifyZeroInteractions(this.resilienceStrategy);

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.RemoveOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.RemoveAllOutcome.FAILURE));
    assertThat(ehcache.getBulkMethodEntries().get(BulkOps.REMOVE_ALL).intValue(), is(KEY_SET_B.size()));
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#removeAll(Set)} for
   * <ul>
   *    <li>non-empty request set</li>
   *    <li>populated {@code Store} - some keys overlap request</li>
   *    <li>{@link Store#bulkCompute} throws before accessing writer</li>
   *    <li>populated {@code CacheLoaderWriter} - all keys overlap</li>
   *    <li>some {@link CacheLoaderWriter#deleteAll(Iterable)} calls fail</li>
   *    <li>at least one {@link CacheLoaderWriter#deleteAll(Iterable)} call aborts</li>
   * </ul>
   */
  @Test
  public void testRemoveAllStoreSomeOverlapStoreAccessExceptionBeforeWriterFullOverlapSomeFailWithAbort() throws Exception {
    Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);
    doThrow(new StoreAccessException("")).when(this.store)
        .bulkCompute(getAnyStringSet(), getAnyEntryIterableFunction());

    Map<String, String> originalWriterContent = getEntryMap(KEY_SET_B, KEY_SET_C, KEY_SET_D);
    FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent, KEY_SET_D);
    fakeLoaderWriter.setCompleteFailureKey("keyC4");
    this.cacheLoaderWriter = spy(fakeLoaderWriter);

    EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    Set<String> contentUpdates = fanIn(KEY_SET_B, KEY_SET_C, KEY_SET_D);
    Set<String> expectedFailures = union(KEY_SET_D, Collections.singleton("keyC4"));
    Set<String> expectedSuccesses = copyWithout(contentUpdates, expectedFailures);
    try {
      ehcache.removeAll(contentUpdates);
      fail();
    } catch (CacheWritingException e) {
      // Expected
    }

    InOrder ordered = inOrder(this.store, this.cacheLoaderWriter, this.resilienceStrategy);
    ordered.verify(this.store, atLeast(1))
        .bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), everyItem(isIn(contentUpdates)));
    // ResilienceStrategy invoked; no assertions about Store content
    ordered.verify(this.resilienceStrategy).removeAllFailure(eq(contentUpdates), any(StoreAccessException.class));
    ordered.verify(this.cacheLoaderWriter, atLeast(1)).deleteAll(getAnyStringIterable());

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.RemoveOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.RemoveAllOutcome.FAILURE));
    assertThat(ehcache.getBulkMethodEntries().get(BulkOps.REMOVE_ALL).intValue(), is(0));
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#removeAll(Set)} for
   * <ul>
   *    <li>non-empty request set</li>
   *    <li>populated {@code Store} - some keys overlap request</li>
   *    <li>{@link Store#bulkCompute} throws after accessing writer</li>
   *    <li>populated {@code CacheLoaderWriter} - all keys overlap</li>
   *    <li>some {@link CacheLoaderWriter#deleteAll(Iterable)} calls fail</li>
   *    <li>at least one {@link CacheLoaderWriter#deleteAll(Iterable)} call aborts</li>
   * </ul>
   */
  @Test
  public void testRemoveAllStoreSomeOverlapStoreAccessExceptionAfterWriterFullOverlapSomeFailWithAbort() throws Exception {
    Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    FakeStore fakeStore = new FakeStore(originalStoreContent, Collections.singleton("keyB3"));
    this.store = spy(fakeStore);

    Map<String, String> originalWriterContent = getEntryMap(KEY_SET_B, KEY_SET_C, KEY_SET_D);
    FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent, KEY_SET_D);
    fakeLoaderWriter.setCompleteFailureKey("keyC4");
    this.cacheLoaderWriter = spy(fakeLoaderWriter);

    EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    Set<String> contentUpdates = fanIn(KEY_SET_B, KEY_SET_C, KEY_SET_D);
    Set<String> expectedFailures = union(KEY_SET_D, Collections.singleton("keyC4"));
    Set<String> expectedSuccesses = copyWithout(contentUpdates, expectedFailures);
    try {
      ehcache.removeAll(contentUpdates);
      fail();
    } catch (CacheWritingException e) {
      // Expected
    }

    InOrder ordered = inOrder(this.store, this.cacheLoaderWriter, this.resilienceStrategy);
    ordered.verify(this.store, atLeast(1))
        .bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), everyItem(isIn(contentUpdates)));
    // ResilienceStrategy invoked; no assertions about Store content
    ordered.verify(this.resilienceStrategy).removeAllFailure(eq(contentUpdates), any(StoreAccessException.class));
    ordered.verify(this.cacheLoaderWriter, atLeast(1)).deleteAll(getAnyStringIterable());

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.RemoveOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.RemoveAllOutcome.FAILURE));
    assertThat(ehcache.getBulkMethodEntries().get(BulkOps.REMOVE_ALL).intValue(), is(0));
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#removeAll(Set)} for
   * <ul>
   *    <li>non-empty request set</li>
   *    <li>populated {@code Store} - some keys overlap request</li>
   *    <li>populated {@code CacheLoaderWriter} - all keys overlap</li>
   *    <li>all {@link CacheLoaderWriter#deleteAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testRemoveAllStoreSomeOverlapWriterFullOverlapAllFail() throws Exception {
    Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);

    Map<String, String> originalWriterContent = getEntryMap(KEY_SET_B, KEY_SET_C, KEY_SET_D);
    FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);
    doThrow(new Exception("deleteAll failed")).when(this.cacheLoaderWriter).deleteAll(getAnyStringIterable());

    EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    Set<String> contentUpdates = fanIn(KEY_SET_B, KEY_SET_C, KEY_SET_D);
    try {
      ehcache.removeAll(contentUpdates);
      fail();
    } catch (BulkCacheWritingException e) {
      // Expected
      assertThat(e.getSuccesses(), empty());
      assertThat(e.getFailures().keySet(), Matchers.<Set<?>>equalTo(contentUpdates));
    }

    verify(this.store, atLeast(1))
        .bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), everyItem(isIn(contentUpdates)));
    assertThat(fakeStore.getEntryMap(), equalTo(copyWithout(originalStoreContent, contentUpdates)));
    verify(this.cacheLoaderWriter, atLeast(1)).deleteAll(getAnyStringIterable());
    verifyZeroInteractions(this.resilienceStrategy);

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.RemoveOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.RemoveAllOutcome.FAILURE));
    assertThat(ehcache.getBulkMethodEntries().get(BulkOps.REMOVE_ALL).intValue(), is(0));
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#removeAll(Set)} for
   * <ul>
   *    <li>non-empty request set</li>
   *    <li>populated {@code Store} - some keys overlap request</li>
   *    <li>{@link Store#bulkCompute} throws before accessing writer</li>
   *    <li>populated {@code CacheLoaderWriter} - all keys overlap</li>
   *    <li>all {@link CacheLoaderWriter#deleteAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testRemoveAllStoreSomeOverlapStoreAccessExceptionBeforeWriterFullOverlapAllFail() throws Exception {
    Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);
    doThrow(new StoreAccessException("")).when(this.store)
        .bulkCompute(getAnyStringSet(), getAnyEntryIterableFunction());

    Map<String, String> originalWriterContent = getEntryMap(KEY_SET_B, KEY_SET_C, KEY_SET_D);
    FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);
    doThrow(new Exception("deleteAll failed")).when(this.cacheLoaderWriter).deleteAll(getAnyStringIterable());

    EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    Set<String> contentUpdates = fanIn(KEY_SET_B, KEY_SET_C, KEY_SET_D);
    try {
      ehcache.removeAll(contentUpdates);
      fail();
    } catch (CacheWritingException e) {
      // Expected
    }

    InOrder ordered = inOrder(this.store, this.cacheLoaderWriter, this.resilienceStrategy);
    ordered.verify(this.store, atLeast(1))
        .bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), everyItem(isIn(contentUpdates)));
    // ResilienceStrategy invoked; no assertions about Store content
    ordered.verify(this.resilienceStrategy).removeAllFailure(eq(contentUpdates), any(StoreAccessException.class));
    ordered.verify(this.cacheLoaderWriter, atLeast(1)).deleteAll(getAnyStringIterable());
    assertThat(fakeLoaderWriter.getEntryMap(), equalTo(originalWriterContent));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.RemoveOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.RemoveAllOutcome.FAILURE));
    assertThat(ehcache.getBulkMethodEntries().get(BulkOps.REMOVE_ALL).intValue(), is(0));
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#removeAll(Set)} for
   * <ul>
   *    <li>non-empty request set</li>
   *    <li>populated {@code Store} - some keys overlap request</li>
   *    <li>{@link Store#bulkCompute} throws after accessing writer</li>
   *    <li>populated {@code CacheLoaderWriter} - all keys overlap</li>
   *    <li>all {@link CacheLoaderWriter#deleteAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testRemoveAllStoreSomeOverlapStoreAccessExceptionAfterWriterFullOverlapAllFail() throws Exception {
    Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    FakeStore fakeStore = new FakeStore(originalStoreContent, Collections.singleton("keyB3"));
    this.store = spy(fakeStore);

    Map<String, String> originalWriterContent = getEntryMap(KEY_SET_B, KEY_SET_C, KEY_SET_D);
    FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);
    doThrow(new Exception("deleteAll failed")).when(this.cacheLoaderWriter).deleteAll(getAnyStringIterable());

    EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    Set<String> contentUpdates = fanIn(KEY_SET_B, KEY_SET_C, KEY_SET_D);
    try {
      ehcache.removeAll(contentUpdates);
      fail();
    } catch (CacheWritingException e) {
      // Expected
    }

    InOrder ordered = inOrder(this.store, this.cacheLoaderWriter, this.resilienceStrategy);
    ordered.verify(this.store, atLeast(1))
        .bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), everyItem(isIn(contentUpdates)));
    // ResilienceStrategy invoked; no assertions about Store content
    ordered.verify(this.resilienceStrategy).removeAllFailure(eq(contentUpdates), any(StoreAccessException.class));
    ordered.verify(this.cacheLoaderWriter, atLeast(1)).deleteAll(getAnyStringIterable());
    assertThat(fakeLoaderWriter.getEntryMap(), equalTo(originalWriterContent));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.RemoveOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.RemoveAllOutcome.FAILURE));
    assertThat(ehcache.getBulkMethodEntries().get(BulkOps.REMOVE_ALL).intValue(), is(0));
  }

  private EhcacheWithLoaderWriter<String, String> getEhcache(CacheLoaderWriter<String, String> cacheLoaderWriter) {
    this.resilienceStrategy = spy(new RobustLoaderWriterResilienceStrategy<>(new DefaultRecoveryStore<>(this.store), cacheLoaderWriter));
    EhcacheWithLoaderWriter<String, String> ehcache = new EhcacheWithLoaderWriter<>(CACHE_CONFIGURATION, this.store, resilienceStrategy, cacheLoaderWriter, cacheEventDispatcher, LoggerFactory
      .getLogger(EhcacheWithLoaderWriter.class + "-" + "EhcacheWithLoaderWriterBasicRemoveAllTest"));
    ehcache.init();
    assertThat("cache not initialized", ehcache.getStatus(), Matchers.is(Status.AVAILABLE));
    return ehcache;
  }

  /**
   * Collects all arguments captured by {@link #bulkComputeSetCaptor}.
   *
   * @return the argument values collected by {@link #bulkComputeSetCaptor}; the
   *    {@code Iterator} over the resulting {@code Set} returns the values
   *    in the order observed by the captor.
   */
  private Set<String> getBulkComputeArgs() {
    Set<String> bulkComputeArgs = new LinkedHashSet<>();
    for (Set<String> set : this.bulkComputeSetCaptor.getAllValues()) {
      bulkComputeArgs.addAll(set);
    }
    return bulkComputeArgs;
  }

}
