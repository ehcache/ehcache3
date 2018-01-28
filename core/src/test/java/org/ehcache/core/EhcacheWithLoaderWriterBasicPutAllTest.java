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

import java.time.Duration;
import java.util.Collections;
import java.util.EnumSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.ehcache.Status;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.core.config.BaseCacheConfiguration;
import org.ehcache.core.config.ResourcePoolsHelper;
import org.ehcache.core.internal.resilience.RobustLoaderWriterResilienceStrategy;
import org.ehcache.core.resilience.DefaultRecoveryStore;
import org.ehcache.core.spi.store.Store;
import org.ehcache.core.statistics.BulkOps;
import org.ehcache.core.statistics.CacheOperationOutcomes;
import org.ehcache.expiry.ExpiryPolicy;
import org.ehcache.spi.loaderwriter.BulkCacheWritingException;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
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
import static org.ehcache.core.EhcacheBasicBulkUtil.getAltEntryMap;
import static org.ehcache.core.EhcacheBasicBulkUtil.getEntryMap;
import static org.ehcache.core.EhcacheBasicBulkUtil.union;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isIn;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;
import static org.mockito.hamcrest.MockitoHamcrest.argThat;
import static org.ehcache.core.EhcacheBasicPutAllTest.getAnyEntryIterable;
import static org.ehcache.core.EhcacheBasicPutAllTest.getAnyEntryIterableFunction;
import static org.ehcache.core.EhcacheBasicPutAllTest.getAnyStringSet;

/**
 * Provides testing of basic PUT_ALL operations on an {@code EhcacheWithLoaderWriter}.
 * <p>
 * In an effort compromise, this class intentionally omits test cases in which
 * the {@code Store} is pre-populated with no entries, pre-populated only with
 * entries having keys not in the {@code putAll} request map, and pre-populated
 * with entries for all keys in the {@code putAll} request map.  This reduces
 * the potential test cases by about 70% without, hopefully, compromising code
 * coverage.
 * <p>
 * Since the processing in {@link EhcacheWithLoaderWriter#putAll} relies on non-deterministically ordered Maps in several stages
 * of processing, the result of {@code putAll} when handling failures is *not* deterministic -- changes in
 * iteration order of the {@code putAll} request map can change the results of the {@code putAll} operation under
 * error scenarios.  The test assertions attempt to confirm results in aggregate over successes and failures and
 * do not specify specific success and failures for each test.
 *
 * @author Clifford W. Johnson
 */
public class EhcacheWithLoaderWriterBasicPutAllTest extends EhcacheBasicCrudBase {

  @Mock
  private CacheLoaderWriter<String, String> cacheLoaderWriter;

  /**
   * A Mockito {@code ArgumentCaptor} for the {@code Set} argument to the
   * {@link Store#bulkCompute(Set, java.util.function.Function, java.util.function.Supplier)
   *    Store.bulkCompute(Set, Function, NullaryFunction} method.
   */
  @Captor
  private ArgumentCaptor<Set<String>> bulkComputeSetCaptor;

  /**
   * Tests {@link EhcacheWithLoaderWriter#putAll(Map)} for
   * <ul>
   *    <li>empty request map</li>
   *    <li>populated {@code Store} (keys not relevant)</li>
   *    <li>populated {@code CacheLoaderWriter} (keys not relevant)</li>
   * </ul>
   */
  @Test
  public void testPutAllEmptyRequestWithWriter() throws Exception {
    Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);

    FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalStoreContent);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);

    InternalCache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);
    ehcache.putAll(Collections.<String, String>emptyMap());

    verify(this.store, never()).bulkCompute(eq(Collections.<String>emptySet()), getAnyEntryIterableFunction());
    assertThat(fakeStore.getEntryMap(), equalTo(originalStoreContent));
    assertThat(fakeLoaderWriter.getEntryMap(), equalTo(originalStoreContent));
    verify(this.resilienceStrategy, never()).putAllFailure(eq(Collections.<String, String>emptyMap()), any(StoreAccessException.class));
    verify(this.cacheLoaderWriter, never()).writeAll(eq(Collections.<Map.Entry<String, String>>emptyList()));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.PutOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.PutAllOutcome.SUCCESS));
    assertThat(ehcache.getBulkMethodEntries().get(BulkOps.PUT_ALL).intValue(), is(0));
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#putAll(Map)} for
   * <ul>
   *    <li>non-empty request map</li>
   *    <li>populated {@code Store} - some keys overlap request</li>
   *    <li>populated {@code CacheLoaderWriter} - some keys overlap</li>
   *    <li>no {@link CacheLoaderWriter#writeAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testPutAllStoreSomeOverlapWriterNoOverlapNoneFail() throws Exception {
    Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);

    Map<String, String> originalWriterContent = getEntryMap(KEY_SET_A, KEY_SET_B, KEY_SET_D);
    FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);

    InternalCache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    Map<String, String> contentUpdates = getAltEntryMap("new_", fanIn(KEY_SET_A, KEY_SET_C));
    ehcache.putAll(contentUpdates);

    verify(this.store, atLeast(1)).bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), equalTo(contentUpdates.keySet()));
    assertThat(fakeStore.getEntryMap(), equalTo(union(originalStoreContent, contentUpdates)));
    assertThat(fakeLoaderWriter.getEntryMap(), equalTo(union(originalWriterContent, contentUpdates)));
    verifyZeroInteractions(this.resilienceStrategy);

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.PutOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.PutAllOutcome.SUCCESS));
    assertThat(ehcache.getBulkMethodEntries().get(BulkOps.PUT_ALL).intValue(), is(contentUpdates.size()));
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#putAll(Map)} for
   * <ul>
   *    <li>non-empty request map</li>
   *    <li>populated {@code Store} - some keys overlap request</li>
   *    <li>{@link Store#bulkCompute} throws before accessing writer</li>
   *    <li>populated {@code CacheLoaderWriter} - some keys overlap</li>
   *    <li>no {@link CacheLoaderWriter#writeAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testPutAllStoreSomeOverlapStoreAccessExceptionBeforeWriterNoOverlapNoneFail() throws Exception {
    Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);
    doThrow(new StoreAccessException("")).when(this.store)
        .bulkCompute(getAnyStringSet(), getAnyEntryIterableFunction());

    Map<String, String> originalWriterContent = getEntryMap(KEY_SET_A, KEY_SET_B, KEY_SET_D);
    FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);

    InternalCache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    Map<String, String> contentUpdates = getAltEntryMap("new_", fanIn(KEY_SET_A, KEY_SET_C));
    ehcache.putAll(contentUpdates);

    InOrder ordered = inOrder(this.store, this.cacheLoaderWriter, this.resilienceStrategy);
    ordered.verify(this.store, atLeast(1)).bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), everyItem(isIn(contentUpdates.keySet())));
    // ResilienceStrategy invoked; no assertions about Store content
    ordered.verify(this.resilienceStrategy).putAllFailure(eq(contentUpdates), any(StoreAccessException.class));
    ordered.verify(this.cacheLoaderWriter, atLeast(1)).writeAll(getAnyEntryIterable());
    assertThat(fakeLoaderWriter.getEntryMap(), equalTo(union(originalWriterContent, contentUpdates)));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.PutOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.PutAllOutcome.FAILURE));
    assertThat(ehcache.getBulkMethodEntries().get(BulkOps.PUT_ALL).intValue(), is(0));
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#putAll(Map)} for
   * <ul>
   *    <li>non-empty request map</li>
   *    <li>populated {@code Store} - some keys overlap request</li>
   *    <li>{@link Store#bulkCompute} throws after accessing writer</li>
   *    <li>populated {@code CacheLoaderWriter} - some keys overlap</li>
   *    <li>no {@link CacheLoaderWriter#writeAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testPutAllStoreSomeOverlapStoreAccessExceptionAfterWriterNoOverlapNoneFail() throws Exception {
    Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    FakeStore fakeStore = new FakeStore(originalStoreContent, Collections.singleton("keyA3"));
    this.store = spy(fakeStore);

    Map<String, String> originalWriterContent = getEntryMap(KEY_SET_A, KEY_SET_B, KEY_SET_D);
    FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);

    InternalCache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    Map<String, String> contentUpdates = getAltEntryMap("new_", fanIn(KEY_SET_A, KEY_SET_C));
    ehcache.putAll(contentUpdates);

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.PutOutcome.class));

    InOrder ordered = inOrder(this.store, this.cacheLoaderWriter, this.resilienceStrategy);
    ordered.verify(this.store, atLeast(1))
        .bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), everyItem(isIn(contentUpdates.keySet())));
    // ResilienceStrategy invoked; no assertions about Store content
    ordered.verify(this.resilienceStrategy).putAllFailure(eq(contentUpdates), any(StoreAccessException.class));
    ordered.verify(this.cacheLoaderWriter, atLeast(1)).writeAll(getAnyEntryIterable());
    assertThat(fakeLoaderWriter.getEntryMap(), equalTo(union(originalWriterContent, contentUpdates)));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.PutOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.PutAllOutcome.FAILURE));
    assertThat(ehcache.getBulkMethodEntries().get(BulkOps.PUT_ALL).intValue(), is(0));
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#putAll(Map)} for
   * <ul>
   *    <li>non-empty request map</li>
   *    <li>populated {@code Store} - some keys overlap request</li>
   *    <li>populated {@code CacheLoaderWriter} - some keys overlap</li>
   *    <li>some {@link CacheLoaderWriter#writeAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testPutAllStoreSomeOverlapWriterNoOverlapSomeFail() throws Exception {
    Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);

    Map<String, String> originalWriterContent = getEntryMap(KEY_SET_A, KEY_SET_B, KEY_SET_D);
    FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent, KEY_SET_C);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);

    InternalCache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    Map<String, String> contentUpdates = getAltEntryMap("new_", fanIn(KEY_SET_A, KEY_SET_C));
    Set<String> expectedFailures = KEY_SET_C;
    Map<String, String> expectedSuccesses = copyWithout(contentUpdates, expectedFailures);
    try {
      ehcache.putAll(contentUpdates);
      fail();
    } catch (BulkCacheWritingException e) {
      // Expected
      assertThat(e.getSuccesses(), Matchers.equalTo(expectedSuccesses.keySet()));
      assertThat(e.getFailures().keySet(), Matchers.equalTo(expectedFailures));
    }

    verify(this.store, atLeast(1)).bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), equalTo(contentUpdates.keySet()));
    assertThat(fakeStore.getEntryMap(), equalTo(union(originalStoreContent, expectedSuccesses)));
    assertThat(fakeLoaderWriter.getEntryMap(), equalTo(union(originalWriterContent, expectedSuccesses)));
    verifyZeroInteractions(this.resilienceStrategy);

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.PutOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.PutAllOutcome.FAILURE));
    assertThat(ehcache.getBulkMethodEntries().get(BulkOps.PUT_ALL).intValue(), is(KEY_SET_A.size()));
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#putAll(Map)} for
   * <ul>
   *    <li>non-empty request map</li>
   *    <li>populated {@code Store} - some keys overlap request</li>
   *    <li>{@link Store#bulkCompute} throws before accessing writer</li>
   *    <li>populated {@code CacheLoaderWriter} - some keys overlap</li>
   *    <li>some {@link CacheLoaderWriter#writeAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testPutAllStoreSomeOverlapStoreAccessExceptionBeforeWriterNoOverlapSomeFail() throws Exception {
    Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);
    doThrow(new StoreAccessException("")).when(this.store)
        .bulkCompute(getAnyStringSet(), getAnyEntryIterableFunction());

    Map<String, String> originalWriterContent = getEntryMap(KEY_SET_A, KEY_SET_B, KEY_SET_D);
    FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent, KEY_SET_C);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);

    InternalCache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    Map<String, String> contentUpdates = getAltEntryMap("new_", fanIn(KEY_SET_A, KEY_SET_C));
    Set<String> expectedFailures = KEY_SET_C;
    Map<String, String> expectedSuccesses = copyWithout(contentUpdates, expectedFailures);
    try {
      ehcache.putAll(contentUpdates);
      fail();
    } catch (CacheWritingException e) {
      // Expected
    }

    InOrder ordered = inOrder(this.store, this.cacheLoaderWriter, this.resilienceStrategy);
    ordered.verify(this.store, atLeast(1)).bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), everyItem(isIn(contentUpdates.keySet())));
    // ResilienceStrategy invoked; no assertions about Store content

    ordered.verify(this.resilienceStrategy).putAllFailure(eq(contentUpdates), any(StoreAccessException.class));
    ordered.verify(this.cacheLoaderWriter, atLeast(1)).writeAll(getAnyEntryIterable());
    assertThat(fakeLoaderWriter.getEntryMap(), equalTo(union(originalWriterContent, expectedSuccesses)));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.PutOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.PutAllOutcome.FAILURE));
    assertThat(ehcache.getBulkMethodEntries().get(BulkOps.PUT_ALL).intValue(), is(0));
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#putAll(Map)} for
   * <ul>
   *    <li>non-empty request map</li>
   *    <li>populated {@code Store} - some keys overlap request</li>
   *    <li>{@link Store#bulkCompute} throws after accessing writer</li>
   *    <li>populated {@code CacheLoaderWriter} - some keys overlap</li>
   *    <li>some {@link CacheLoaderWriter#writeAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testPutAllStoreSomeOverlapStoreAccessExceptionAfterWriterNoOverlapSomeFail() throws Exception {
    Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    FakeStore fakeStore = new FakeStore(originalStoreContent, Collections.singleton("keyA3"));
    this.store = spy(fakeStore);

    Map<String, String> originalWriterContent = getEntryMap(KEY_SET_A, KEY_SET_B, KEY_SET_D);
    FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent, KEY_SET_C);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);

    InternalCache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    Map<String, String> contentUpdates = getAltEntryMap("new_", fanIn(KEY_SET_A, KEY_SET_C));
    Set<String> expectedFailures = KEY_SET_C;
    Map<String, String> expectedSuccesses = copyWithout(contentUpdates, expectedFailures);
    try {
      ehcache.putAll(contentUpdates);
      fail();
    } catch (CacheWritingException e) {
      // Expected
    }

    InOrder ordered = inOrder(this.store, this.cacheLoaderWriter, this.resilienceStrategy);
    ordered.verify(this.store, atLeast(1)).bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), everyItem(isIn(contentUpdates.keySet())));
    // ResilienceStrategy invoked; no assertions about Store content

    ordered.verify(this.resilienceStrategy).putAllFailure(eq(contentUpdates), any(StoreAccessException.class));
    ordered.verify(this.cacheLoaderWriter, atLeast(1)).writeAll(getAnyEntryIterable());
    assertThat(fakeLoaderWriter.getEntryMap(), equalTo(union(originalWriterContent, expectedSuccesses)));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.PutOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.PutAllOutcome.FAILURE));
    assertThat(ehcache.getBulkMethodEntries().get(BulkOps.PUT_ALL).intValue(), is(0));
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#putAll(Map)} for
   * <ul>
   *    <li>non-empty request map</li>
   *    <li>populated {@code Store} - some keys overlap request</li>
   *    <li>populated {@code CacheLoaderWriter} - some keys overlap</li>
   *    <li>some {@link CacheLoaderWriter#writeAll(Iterable)} calls fail</li>
   *    <li>at least one {@link CacheLoaderWriter#writeAll(Iterable)} call aborts</li>
   * </ul>
   */
  @Test
  public void testPutAllStoreSomeOverlapWriterNoOverlapSomeFailWithAbort() throws Exception {
    Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);

    Map<String, String> originalWriterContent = getEntryMap(KEY_SET_A, KEY_SET_B, KEY_SET_D);
    FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent, KEY_SET_C);
    fakeLoaderWriter.setCompleteFailureKey("keyC4");
    this.cacheLoaderWriter = spy(fakeLoaderWriter);

    InternalCache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    Map<String, String> contentUpdates = getAltEntryMap("new_", fanIn(KEY_SET_A, KEY_SET_C));
    Set<String> expectedFailures = KEY_SET_C;
    Map<String, String> expectedSuccesses = copyWithout(contentUpdates, expectedFailures);
    try {
      ehcache.putAll(contentUpdates);
      fail();
    } catch (BulkCacheWritingException e) {
      // Expected
      assertThat(e.getSuccesses(), Matchers.equalTo(expectedSuccesses.keySet()));
      assertThat(e.getFailures().keySet(), Matchers.equalTo(expectedFailures));
    }

    verify(this.store, atLeast(1)).bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), everyItem(isIn(contentUpdates.keySet())));
    assertThat(fakeStore.getEntryMap(), equalTo(union(originalStoreContent, expectedSuccesses)));
    verify(this.cacheLoaderWriter, atLeast(1)).writeAll(getAnyEntryIterable());
    assertThat(fakeLoaderWriter.getEntryMap(), equalTo(union(originalWriterContent, expectedSuccesses)));
    verifyZeroInteractions(this.resilienceStrategy);

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.PutOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.PutAllOutcome.FAILURE));
    assertThat(ehcache.getBulkMethodEntries().get(BulkOps.PUT_ALL).intValue(), is(expectedSuccesses.size()));
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#putAll(Map)} for
   * <ul>
   *    <li>non-empty request map</li>
   *    <li>populated {@code Store} - some keys overlap request</li>
   *    <li>{@link Store#bulkCompute} throws before accessing writer</li>
   *    <li>populated {@code CacheLoaderWriter} - some keys overlap</li>
   *    <li>some {@link CacheLoaderWriter#writeAll(Iterable)} calls fail</li>
   *    <li>at least one {@link CacheLoaderWriter#writeAll(Iterable)} call aborts</li>
   * </ul>
   */
  @Test
  public void testPutAllStoreSomeOverlapStoreAccessExceptionBeforeWriterOverlapSomeFailWithAbort() throws Exception {
    Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);
    doThrow(new StoreAccessException("")).when(this.store)
        .bulkCompute(getAnyStringSet(), getAnyEntryIterableFunction());

    Map<String, String> originalWriterContent = getEntryMap(KEY_SET_A, KEY_SET_B, KEY_SET_D);
    FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent, KEY_SET_C);
    fakeLoaderWriter.setCompleteFailureKey("keyC4");
    this.cacheLoaderWriter = spy(fakeLoaderWriter);

    InternalCache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    Map<String, String> contentUpdates = getAltEntryMap("new_", fanIn(KEY_SET_A, KEY_SET_C));

    try {
      ehcache.putAll(contentUpdates);
      fail();
    } catch (CacheWritingException e) {
      // Expected
    }

    InOrder ordered = inOrder(this.store, this.cacheLoaderWriter, this.resilienceStrategy);
    ordered.verify(this.store, atLeast(1)).bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), everyItem(isIn(contentUpdates.keySet())));
    // ResilienceStrategy invoked; no assertions about Store content
    ordered.verify(this.resilienceStrategy).putAllFailure(eq(contentUpdates), any(StoreAccessException.class));
    ordered.verify(this.cacheLoaderWriter, atLeast(1)).writeAll(getAnyEntryIterable());

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.PutOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.PutAllOutcome.FAILURE));
    assertThat(ehcache.getBulkMethodEntries().get(BulkOps.PUT_ALL).intValue(), is(0));
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#putAll(Map)} for
   * <ul>
   *    <li>non-empty request map</li>
   *    <li>populated {@code Store} - some keys overlap request</li>
   *    <li>{@link Store#bulkCompute} throws after accessing writer</li>
   *    <li>populated {@code CacheLoaderWriter} - some keys overlap</li>
   *    <li>some {@link CacheLoaderWriter#writeAll(Iterable)} calls fail</li>
   *    <li>at least one {@link CacheLoaderWriter#writeAll(Iterable)} call aborts</li>
   * </ul>
   */
  @Test
  public void testPutAllStoreSomeOverlapStoreAccessExceptionAfterWriterOverlapSomeFailWithAbort() throws Exception {
    Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    FakeStore fakeStore = new FakeStore(originalStoreContent, Collections.singleton("keyA3"));
    this.store = spy(fakeStore);

    Map<String, String> originalWriterContent = getEntryMap(KEY_SET_A, KEY_SET_B, KEY_SET_D);
    FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent, KEY_SET_C);
    fakeLoaderWriter.setCompleteFailureKey("keyC4");
    this.cacheLoaderWriter = spy(fakeLoaderWriter);

    InternalCache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    Map<String, String> contentUpdates = getAltEntryMap("new_", fanIn(KEY_SET_A, KEY_SET_C));
    Set<String> expectedFailures = KEY_SET_C;
    try {
      ehcache.putAll(contentUpdates);
      fail();
    } catch (CacheWritingException e) {
      // Expected
    }

    InOrder ordered = inOrder(this.store, this.cacheLoaderWriter, this.resilienceStrategy);
    ordered.verify(this.store, atLeast(1)).bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), everyItem(isIn(contentUpdates.keySet())));
    // ResilienceStrategy invoked; no assertions about Store content
    ordered.verify(this.resilienceStrategy).putAllFailure(eq(contentUpdates), any(StoreAccessException.class));
    ordered.verify(this.cacheLoaderWriter, atLeast(1)).writeAll(getAnyEntryIterable());

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.PutOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.PutAllOutcome.FAILURE));
    assertThat(ehcache.getBulkMethodEntries().get(BulkOps.PUT_ALL).intValue(), is(0));
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#putAll(Map)} for
   * <ul>
   *    <li>non-empty request map</li>
   *    <li>populated {@code Store} - some keys overlap request</li>
   *    <li>populated {@code CacheLoaderWriter} - some keys overlap</li>
   *    <li>all {@link CacheLoaderWriter#writeAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testPutAllStoreSomeOverlapWriterNoOverlapAllFail() throws Exception {
    Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);

    Map<String, String> originalWriterContent = getEntryMap(KEY_SET_A, KEY_SET_B, KEY_SET_D);
    FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);
    doThrow(new Exception("writeAll failed")).when(this.cacheLoaderWriter).writeAll(getAnyEntryIterable());

    InternalCache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    Map<String, String> contentUpdates = getAltEntryMap("new_", fanIn(KEY_SET_A, KEY_SET_C));
    try {
      ehcache.putAll(contentUpdates);
      fail();
    } catch (BulkCacheWritingException e) {
      // Expected
      assertThat(e.getSuccesses(), empty());
      assertThat(e.getFailures().keySet(), Matchers.equalTo(contentUpdates.keySet()));
    }

    verify(this.store, atLeast(1)).bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), everyItem(isIn(contentUpdates.keySet())));
    assertThat(fakeStore.getEntryMap(), equalTo(originalStoreContent));
    verify(this.cacheLoaderWriter, atLeast(1)).writeAll(getAnyEntryIterable());
    verifyZeroInteractions(this.resilienceStrategy);

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.PutOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.PutAllOutcome.FAILURE));
    assertThat(ehcache.getBulkMethodEntries().get(BulkOps.PUT_ALL).intValue(), is(0));
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#putAll(Map)} for
   * <ul>
   *    <li>non-empty request map</li>
   *    <li>populated {@code Store} - some keys overlap request</li>
   *    <li>{@link Store#bulkCompute} throws before accessing writer</li>
   *    <li>populated {@code CacheLoaderWriter} - some keys overlap</li>
   *    <li>all {@link CacheLoaderWriter#writeAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testPutAllStoreSomeOverlapStoreAccessExceptionBeforeWriterNoOverlapAllFail() throws Exception {
    Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);
    doThrow(new StoreAccessException("")).when(this.store)
        .bulkCompute(getAnyStringSet(), getAnyEntryIterableFunction());

    Map<String, String> originalWriterContent = getEntryMap(KEY_SET_A, KEY_SET_B, KEY_SET_D);
    FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);
    doThrow(new Exception("writeAll failed")).when(this.cacheLoaderWriter).writeAll(getAnyEntryIterable());

    InternalCache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    Map<String, String> contentUpdates = getAltEntryMap("new_", fanIn(KEY_SET_A, KEY_SET_C));
    try {
      ehcache.putAll(contentUpdates);
      fail();
    } catch (CacheWritingException e) {
      // Expected
    }

    InOrder ordered = inOrder(this.store, this.cacheLoaderWriter, this.resilienceStrategy);
    ordered.verify(this.store, atLeast(1)).bulkCompute(this.bulkComputeSetCaptor.capture(),
        getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), everyItem(isIn(contentUpdates.keySet())));
    // ResilienceStrategy invoked; no assertions about Store content
    ordered.verify(this.resilienceStrategy).putAllFailure(eq(contentUpdates), any(StoreAccessException.class));
    ordered.verify(this.cacheLoaderWriter, atLeast(1)).writeAll(getAnyEntryIterable());

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.PutOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.PutAllOutcome.FAILURE));
    assertThat(ehcache.getBulkMethodEntries().get(BulkOps.PUT_ALL).intValue(), is(0));
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#putAll(Map)} for
   * <ul>
   *    <li>non-empty request map</li>
   *    <li>populated {@code Store} - some keys overlap request</li>
   *    <li>{@link Store#bulkCompute} throws after accessing writer</li>
   *    <li>populated {@code CacheLoaderWriter} - some keys overlap</li>
   *    <li>all {@link CacheLoaderWriter#writeAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testPutAllStoreSomeOverlapStoreAccessExceptionAfterWriterNoOverlapAllFail() throws Exception {
    Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    FakeStore fakeStore = new FakeStore(originalStoreContent, Collections.singleton("keyA3"));
    this.store = spy(fakeStore);

    Map<String, String> originalWriterContent = getEntryMap(KEY_SET_A, KEY_SET_B, KEY_SET_D);
    FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);
    doThrow(new Exception("writeAll failed")).when(this.cacheLoaderWriter).writeAll(getAnyEntryIterable());

    InternalCache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    Map<String, String> contentUpdates = getAltEntryMap("new_", fanIn(KEY_SET_A, KEY_SET_C));
    try {
      ehcache.putAll(contentUpdates);
      fail();
    } catch (CacheWritingException e) {
      // Expected
    }

    InOrder ordered = inOrder(this.store, this.cacheLoaderWriter, this.resilienceStrategy);
    ordered.verify(this.store, atLeast(1)).bulkCompute(this.bulkComputeSetCaptor.capture(),
        getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), everyItem(isIn(contentUpdates.keySet())));
    // ResilienceStrategy invoked; no assertions about Store content
    ordered.verify(this.resilienceStrategy).putAllFailure(eq(contentUpdates), any(StoreAccessException.class));
    ordered.verify(this.cacheLoaderWriter, atLeast(1)).writeAll(getAnyEntryIterable());

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.PutOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.PutAllOutcome.FAILURE));
    assertThat(ehcache.getBulkMethodEntries().get(BulkOps.PUT_ALL).intValue(), is(0));
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#putAll(Map)} for
   * <ul>
   *    <li>non-empty request map</li>
   *    <li>populated {@code Store} - some keys overlap request</li>
   *    <li>populated {@code CacheLoaderWriter} - some keys overlap</li>
   *    <li>no {@link CacheLoaderWriter#writeAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testPutAllStoreSomeOverlapWriterSomeOverlapNoneFail() throws Exception {
    Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);

    Map<String, String> originalWriterContent = getEntryMap(KEY_SET_A, KEY_SET_B, KEY_SET_C);
    FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);

    InternalCache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    Map<String, String> contentUpdates = getAltEntryMap("new_", fanIn(KEY_SET_A, KEY_SET_C, KEY_SET_D));
    ehcache.putAll(contentUpdates);

    verify(this.store, atLeast(1)).bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), equalTo(contentUpdates.keySet()));
    assertThat(fakeStore.getEntryMap(), equalTo(union(originalStoreContent, contentUpdates)));
    verify(this.cacheLoaderWriter, atLeast(1)).writeAll(getAnyEntryIterable());
    assertThat(fakeLoaderWriter.getEntryMap(), equalTo(union(originalWriterContent, contentUpdates)));
    verifyZeroInteractions(this.resilienceStrategy);

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.PutOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.PutAllOutcome.SUCCESS));
    assertThat(ehcache.getBulkMethodEntries().get(BulkOps.PUT_ALL).intValue(), is(contentUpdates.size()));
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#putAll(Map)} for
   * <ul>
   *    <li>non-empty request map</li>
   *    <li>populated {@code Store} - some keys overlap request</li>
   *    <li>{@link Store#bulkCompute} throws before accessing writer</li>
   *    <li>populated {@code CacheLoaderWriter} - some keys overlap</li>
   *    <li>no {@link CacheLoaderWriter#writeAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testPutAllStoreSomeOverlapStoreAccessExceptionBeforeWriterSomeOverlapNoneFail() throws Exception {
    Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);
    doThrow(new StoreAccessException("")).when(this.store)
        .bulkCompute(getAnyStringSet(), getAnyEntryIterableFunction());

    Map<String, String> originalWriterContent = getEntryMap(KEY_SET_A, KEY_SET_B, KEY_SET_C);
    FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);

    InternalCache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    Map<String, String> contentUpdates = getAltEntryMap("new_", fanIn(KEY_SET_A, KEY_SET_C, KEY_SET_D));
    ehcache.putAll(contentUpdates);

    InOrder ordered = inOrder(this.store, this.cacheLoaderWriter, this.resilienceStrategy);
    ordered.verify(this.store, atLeast(1))
        .bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), everyItem(isIn(contentUpdates.keySet())));
    // ResilienceStrategy invoked; no assertions about Store content
    ordered.verify(this.resilienceStrategy).putAllFailure(eq(contentUpdates), any(StoreAccessException.class));
    ordered.verify(this.cacheLoaderWriter, atLeast(1)).writeAll(getAnyEntryIterable());
    assertThat(fakeLoaderWriter.getEntryMap(), equalTo(union(originalWriterContent, contentUpdates)));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.PutOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.PutAllOutcome.FAILURE));
    assertThat(ehcache.getBulkMethodEntries().get(BulkOps.PUT_ALL).intValue(), is(0));
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#putAll(Map)} for
   * <ul>
   *    <li>non-empty request map</li>
   *    <li>populated {@code Store} - some keys overlap request</li>
   *    <li>{@link Store#bulkCompute} throws after accessing writer</li>
   *    <li>populated {@code CacheLoaderWriter} - some keys overlap</li>
   *    <li>no {@link CacheLoaderWriter#writeAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testPutAllStoreSomeOverlapStoreAccessExceptionAfterWriterSomeOverlapNoneFail() throws Exception {
    Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    FakeStore fakeStore = new FakeStore(originalStoreContent, Collections.singleton("keyA3"));
    this.store = spy(fakeStore);

    Map<String, String> originalWriterContent = getEntryMap(KEY_SET_A, KEY_SET_B, KEY_SET_C);
    FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);

    InternalCache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    Map<String, String> contentUpdates = getAltEntryMap("new_", fanIn(KEY_SET_A, KEY_SET_C, KEY_SET_D));
    ehcache.putAll(contentUpdates);

    InOrder ordered = inOrder(this.store, this.cacheLoaderWriter, this.resilienceStrategy);
    ordered.verify(this.store, atLeast(1))
        .bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), everyItem(isIn(contentUpdates.keySet())));
    // ResilienceStrategy invoked; no assertions about Store content
    ordered.verify(this.resilienceStrategy).putAllFailure(eq(contentUpdates), any(StoreAccessException.class));
    ordered.verify(this.cacheLoaderWriter, atLeast(1)).writeAll(getAnyEntryIterable());
    assertThat(fakeLoaderWriter.getEntryMap(), equalTo(union(originalWriterContent, contentUpdates)));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.PutOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.PutAllOutcome.FAILURE));
    assertThat(ehcache.getBulkMethodEntries().get(BulkOps.PUT_ALL).intValue(), is(0));
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#putAll(Map)} for
   * <ul>
   *    <li>non-empty request map</li>
   *    <li>populated {@code Store} - some keys overlap request</li>
   *    <li>populated {@code CacheLoaderWriter} - some keys overlap</li>
   *    <li>some {@link CacheLoaderWriter#writeAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testPutAllStoreSomeOverlapWriterSomeOverlapSomeFail() throws Exception {
    Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);

    Map<String, String> originalWriterContent = getEntryMap(KEY_SET_A, KEY_SET_B, KEY_SET_C);
    FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent, KEY_SET_D);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);

    InternalCache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    Map<String, String> contentUpdates = getAltEntryMap("new_", fanIn(KEY_SET_A, KEY_SET_C, KEY_SET_D));
    Set<String> expectedFailures = KEY_SET_D;
    Map<String, String> expectedSuccesses = copyWithout(contentUpdates, expectedFailures);
    try {
      ehcache.putAll(contentUpdates);
      fail();
    } catch (BulkCacheWritingException e) {
      // Expected
      assertThat(e.getSuccesses(), Matchers.equalTo(expectedSuccesses.keySet()));
      assertThat(e.getFailures().keySet(), Matchers.equalTo(expectedFailures));
    }

    verify(this.store, atLeast(1)).bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), everyItem(isIn(contentUpdates.keySet())));
    assertThat(fakeStore.getEntryMap(), equalTo(union(originalStoreContent, expectedSuccesses)));
    verify(this.cacheLoaderWriter, atLeast(1)).writeAll(getAnyEntryIterable());
    assertThat(fakeLoaderWriter.getEntryMap(), equalTo(union(originalWriterContent, expectedSuccesses)));
    verifyZeroInteractions(this.resilienceStrategy);

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.PutOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.PutAllOutcome.FAILURE));
    assertThat(ehcache.getBulkMethodEntries().get(BulkOps.PUT_ALL).intValue(), is(expectedSuccesses.size()));
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#putAll(Map)} for
   * <ul>
   *    <li>non-empty request map</li>
   *    <li>populated {@code Store} - some keys overlap request</li>
   *    <li>{@link Store#bulkCompute} throws before accessing writer</li>
   *    <li>populated {@code CacheLoaderWriter} - some keys overlap</li>
   *    <li>some {@link CacheLoaderWriter#writeAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testPutAllStoreSomeOverlapStoreAccessExceptionBeforeWriterSomeOverlapSomeFail() throws Exception {
    Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);
    doThrow(new StoreAccessException("")).when(this.store)
        .bulkCompute(getAnyStringSet(), getAnyEntryIterableFunction());

    Map<String, String> originalWriterContent = getEntryMap(KEY_SET_A, KEY_SET_B, KEY_SET_C);
    FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent, KEY_SET_D);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);

    InternalCache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    Map<String, String> contentUpdates = getAltEntryMap("new_", fanIn(KEY_SET_A, KEY_SET_C, KEY_SET_D));
    Set<String> expectedFailures = KEY_SET_D;
    Map<String, String> expectedSuccesses = copyWithout(contentUpdates, expectedFailures);
    try {
      ehcache.putAll(contentUpdates);
      fail();
    } catch (BulkCacheWritingException e) {
      // Expected
    }

    InOrder ordered = inOrder(this.store, this.cacheLoaderWriter, this.resilienceStrategy);
    ordered.verify(this.store, atLeast(1))
        .bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), everyItem(isIn(contentUpdates.keySet())));
    // ResilienceStrategy invoked; no assertions about Store content
    ordered.verify(this.resilienceStrategy).putAllFailure(eq(contentUpdates), any(StoreAccessException.class));
    ordered.verify(this.cacheLoaderWriter, atLeast(1)).writeAll(getAnyEntryIterable());
    assertThat(fakeLoaderWriter.getEntryMap(), equalTo(union(originalWriterContent, expectedSuccesses)));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.PutOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.PutAllOutcome.FAILURE));
    assertThat(ehcache.getBulkMethodEntries().get(BulkOps.PUT_ALL).intValue(), is(0));
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#putAll(Map)} for
   * <ul>
   *    <li>non-empty request map</li>
   *    <li>populated {@code Store} - some keys overlap request</li>
   *    <li>{@link Store#bulkCompute} throws after accessing writer</li>
   *    <li>populated {@code CacheLoaderWriter} - some keys overlap</li>
   *    <li>some {@link CacheLoaderWriter#writeAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testPutAllStoreSomeOverlapStoreAccessExceptionAfterWriterSomeOverlapSomeFail() throws Exception {
    Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    FakeStore fakeStore = new FakeStore(originalStoreContent, Collections.singleton("keyA3"));
    this.store = spy(fakeStore);

    Map<String, String> originalWriterContent = getEntryMap(KEY_SET_A, KEY_SET_B, KEY_SET_C);
    FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent, KEY_SET_D);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);

    InternalCache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    Map<String, String> contentUpdates = getAltEntryMap("new_", fanIn(KEY_SET_A, KEY_SET_C, KEY_SET_D));
    Set<String> expectedFailures = KEY_SET_D;
    Map<String, String> expectedSuccesses = copyWithout(contentUpdates, expectedFailures);
    try {
      ehcache.putAll(contentUpdates);
      fail();
    } catch (BulkCacheWritingException e) {
      // Expected
    }

    InOrder ordered = inOrder(this.store, this.cacheLoaderWriter, this.resilienceStrategy);
    ordered.verify(this.store, atLeast(1))
        .bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), everyItem(isIn(contentUpdates.keySet())));
    // ResilienceStrategy invoked; no assertions about Store content
    ordered.verify(this.resilienceStrategy).putAllFailure(eq(contentUpdates), any(StoreAccessException.class));
    ordered.verify(this.cacheLoaderWriter, atLeast(1)).writeAll(getAnyEntryIterable());
    assertThat(fakeLoaderWriter.getEntryMap(), equalTo(union(originalWriterContent, expectedSuccesses)));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.PutOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.PutAllOutcome.FAILURE));
    assertThat(ehcache.getBulkMethodEntries().get(BulkOps.PUT_ALL).intValue(), is(0));
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#putAll(Map)} for
   * <ul>
   *    <li>non-empty request map</li>
   *    <li>populated {@code Store} - some keys overlap request</li>
   *    <li>populated {@code CacheLoaderWriter} - some keys overlap</li>
   *    <li>some {@link CacheLoaderWriter#writeAll(Iterable)} calls fail</li>
   *    <li>at least one {@link CacheLoaderWriter#writeAll(Iterable)} call aborts</li>
   * </ul>
   */
  @Test
  public void testPutAllStoreSomeOverlapWriterSomeOverlapSomeFailWithAbort() throws Exception {
    Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);

    Map<String, String> originalWriterContent = getEntryMap(KEY_SET_A, KEY_SET_B, KEY_SET_C);
    FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent, KEY_SET_D);
    fakeLoaderWriter.setCompleteFailureKey("keyC4");
    this.cacheLoaderWriter = spy(fakeLoaderWriter);

    InternalCache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    Map<String, String> contentUpdates = getAltEntryMap("new_", fanIn(KEY_SET_A, KEY_SET_C, KEY_SET_D));
    Set<String> expectedFailures = union(KEY_SET_D, Collections.singleton("keyC4"));
    Map<String, String> expectedSuccesses = copyWithout(contentUpdates, expectedFailures);
    try {
      ehcache.putAll(contentUpdates);
      fail();
    } catch (BulkCacheWritingException e) {
      // Expected
      assertThat(e.getSuccesses(), Matchers.equalTo(expectedSuccesses.keySet()));
      assertThat(e.getFailures().keySet(), Matchers.equalTo(expectedFailures));
    }

    verify(this.store, atLeast(1)).bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), everyItem(isIn(contentUpdates.keySet())));
    assertThat(fakeStore.getEntryMap(), equalTo(union(originalStoreContent, expectedSuccesses)));
    verify(this.cacheLoaderWriter, atLeast(1)).writeAll(getAnyEntryIterable());
    assertThat(fakeLoaderWriter.getEntryMap(), equalTo(union(originalWriterContent, expectedSuccesses)));
    verifyZeroInteractions(this.resilienceStrategy);

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.PutOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.PutAllOutcome.FAILURE));
    assertThat(ehcache.getBulkMethodEntries().get(BulkOps.PUT_ALL).intValue(), is(expectedSuccesses.size()));
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#putAll(Map)} for
   * <ul>
   *    <li>non-empty request map</li>
   *    <li>populated {@code Store} - some keys overlap request</li>
   *    <li>{@link Store#bulkCompute} throws before accessing writer</li>
   *    <li>populated {@code CacheLoaderWriter} - some keys overlap</li>
   *    <li>some {@link CacheLoaderWriter#writeAll(Iterable)} calls fail</li>
   *    <li>at least one {@link CacheLoaderWriter#writeAll(Iterable)} call aborts</li>
   * </ul>
   */
  @Test
  public void testPutAllStoreSomeOverlapStoreAccessExceptionBeforeWriterSomeOverlapSomeFailWithAbort() throws Exception {
    Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);
    doThrow(new StoreAccessException("")).when(this.store)
        .bulkCompute(getAnyStringSet(), getAnyEntryIterableFunction());

    Map<String, String> originalWriterContent = getEntryMap(KEY_SET_A, KEY_SET_B, KEY_SET_C);
    FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent, KEY_SET_D);
    fakeLoaderWriter.setCompleteFailureKey("keyC4");
    this.cacheLoaderWriter = spy(fakeLoaderWriter);

    InternalCache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    Map<String, String> contentUpdates = getAltEntryMap("new_", fanIn(KEY_SET_A, KEY_SET_C, KEY_SET_D));
    try {
      ehcache.putAll(contentUpdates);
      fail();
    } catch (CacheWritingException e) {
      // Expected
    }

    InOrder ordered = inOrder(this.store, this.cacheLoaderWriter, this.resilienceStrategy);
    ordered.verify(this.store, atLeast(1)).bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), everyItem(isIn(contentUpdates.keySet())));
    // ResilienceStrategy invoked; no assertions about Store content
    ordered.verify(this.resilienceStrategy).putAllFailure(eq(contentUpdates), any(StoreAccessException.class));
    ordered.verify(this.cacheLoaderWriter, atLeast(1)).writeAll(getAnyEntryIterable());

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.PutOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.PutAllOutcome.FAILURE));
    assertThat(ehcache.getBulkMethodEntries().get(BulkOps.PUT_ALL).intValue(), is(0));
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#putAll(Map)} for
   * <ul>
   *    <li>non-empty request map</li>
   *    <li>populated {@code Store} - some keys overlap request</li>
   *    <li>{@link Store#bulkCompute} throws after accessing writer</li>
   *    <li>populated {@code CacheLoaderWriter} - some keys overlap</li>
   *    <li>some {@link CacheLoaderWriter#writeAll(Iterable)} calls fail</li>
   *    <li>at least one {@link CacheLoaderWriter#writeAll(Iterable)} call aborts</li>
   * </ul>
   */
  @Test
  public void testPutAllStoreSomeOverlapStoreAccessExceptionAfterWriterSomeOverlapSomeFailWithAbort() throws Exception {
    Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    FakeStore fakeStore = new FakeStore(originalStoreContent, Collections.singleton("keyA3"));
    this.store = spy(fakeStore);

    Map<String, String> originalWriterContent = getEntryMap(KEY_SET_A, KEY_SET_B, KEY_SET_C);
    FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent, KEY_SET_D);
    fakeLoaderWriter.setCompleteFailureKey("keyC4");
    this.cacheLoaderWriter = spy(fakeLoaderWriter);

    InternalCache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    Map<String, String> contentUpdates = getAltEntryMap("new_", fanIn(KEY_SET_A, KEY_SET_C, KEY_SET_D));
    Set<String> expectedFailures = union(KEY_SET_D, Collections.singleton("keyC4"));
    Map<String, String> expectedSuccesses = copyWithout(contentUpdates, expectedFailures);
    try {
      ehcache.putAll(contentUpdates);
      fail();
    } catch (CacheWritingException e) {
      // Expected
    }

    InOrder ordered = inOrder(this.store, this.cacheLoaderWriter, this.resilienceStrategy);
    ordered.verify(this.store, atLeast(1)).bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), everyItem(isIn(contentUpdates.keySet())));
    // ResilienceStrategy invoked; no assertions about Store content
    ordered.verify(this.resilienceStrategy).putAllFailure(eq(contentUpdates), any(StoreAccessException.class));
    ordered.verify(this.cacheLoaderWriter, atLeast(1)).writeAll(getAnyEntryIterable());

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.PutOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.PutAllOutcome.FAILURE));
    assertThat(ehcache.getBulkMethodEntries().get(BulkOps.PUT_ALL).intValue(), is(0));
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#putAll(Map)} for
   * <ul>
   *    <li>non-empty request map</li>
   *    <li>populated {@code Store} - some keys overlap request</li>
   *    <li>populated {@code CacheLoaderWriter} - some keys overlap</li>
   *    <li>all {@link CacheLoaderWriter#writeAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testPutAllStoreSomeOverlapWriterSomeOverlapAllFail() throws Exception {
    Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);

    Map<String, String> originalWriterContent = getEntryMap(KEY_SET_A, KEY_SET_B, KEY_SET_C);
    FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);
    doThrow(new Exception("writeAll failed")).when(this.cacheLoaderWriter).writeAll(getAnyEntryIterable());

    InternalCache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    Map<String, String> contentUpdates = getAltEntryMap("new_", fanIn(KEY_SET_A, KEY_SET_C, KEY_SET_D));
    try {
      ehcache.putAll(contentUpdates);
      fail();
    } catch (BulkCacheWritingException e) {
      // Expected
      assertThat(e.getSuccesses(), empty());
      assertThat(e.getFailures().keySet(), Matchers.equalTo(contentUpdates.keySet()));
    }

    verify(this.store, atLeast(1)).bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), everyItem(isIn(contentUpdates.keySet())));
    assertThat(fakeStore.getEntryMap(), equalTo(originalStoreContent));
    verify(this.cacheLoaderWriter, atLeast(1)).writeAll(getAnyEntryIterable());
    verifyZeroInteractions(this.resilienceStrategy);

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.PutOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.PutAllOutcome.FAILURE));
    assertThat(ehcache.getBulkMethodEntries().get(BulkOps.PUT_ALL).intValue(), is(0));
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#putAll(Map)} for
   * <ul>
   *    <li>non-empty request map</li>
   *    <li>populated {@code Store} - some keys overlap request</li>
   *    <li>{@link Store#bulkCompute} throws before accessing writer</li>
   *    <li>populated {@code CacheLoaderWriter} - some keys overlap</li>
   *    <li>all {@link CacheLoaderWriter#writeAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testPutAllStoreSomeOverlapStoreAccessExceptionBeforeWriterSomeOverlapAllFail() throws Exception {
    Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);
    doThrow(new StoreAccessException("")).when(this.store)
        .bulkCompute(getAnyStringSet(), getAnyEntryIterableFunction());

    Map<String, String> originalWriterContent = getEntryMap(KEY_SET_A, KEY_SET_B, KEY_SET_C);
    FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);
    doThrow(new Exception("writeAll failed")).when(this.cacheLoaderWriter).writeAll(getAnyEntryIterable());

    InternalCache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    Map<String, String> contentUpdates = getAltEntryMap("new_", fanIn(KEY_SET_A, KEY_SET_C, KEY_SET_D));
    try {
      ehcache.putAll(contentUpdates);
      fail();
    } catch (CacheWritingException e) {
      // Expected
    }

    InOrder ordered = inOrder(this.store, this.cacheLoaderWriter, this.resilienceStrategy);
    ordered.verify(this.store, atLeast(1))
        .bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), everyItem(isIn(contentUpdates.keySet())));
    // ResilienceStrategy invoked; no assertions about Store content
    ordered.verify(this.resilienceStrategy).putAllFailure(eq(contentUpdates), any(StoreAccessException.class));
    ordered.verify(this.cacheLoaderWriter, atLeast(1)).writeAll(getAnyEntryIterable());

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.PutOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.PutAllOutcome.FAILURE));
    assertThat(ehcache.getBulkMethodEntries().get(BulkOps.PUT_ALL).intValue(), is(0));
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#putAll(Map)} for
   * <ul>
   *    <li>non-empty request map</li>
   *    <li>populated {@code Store} - some keys overlap request</li>
   *    <li>{@link Store#bulkCompute} throws after accessing writer</li>
   *    <li>populated {@code CacheLoaderWriter} - some keys overlap</li>
   *    <li>all {@link CacheLoaderWriter#writeAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testPutAllStoreSomeOverlapStoreAccessExceptionAfterWriterSomeOverlapAllFail() throws Exception {
    Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    FakeStore fakeStore = new FakeStore(originalStoreContent, Collections.singleton("keyA3"));
    this.store = spy(fakeStore);

    Map<String, String> originalWriterContent = getEntryMap(KEY_SET_A, KEY_SET_B, KEY_SET_C);
    FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);
    doThrow(new Exception("writeAll failed")).when(this.cacheLoaderWriter).writeAll(getAnyEntryIterable());

    InternalCache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    Map<String, String> contentUpdates = getAltEntryMap("new_", fanIn(KEY_SET_A, KEY_SET_C, KEY_SET_D));
    try {
      ehcache.putAll(contentUpdates);
      fail();
    } catch (CacheWritingException e) {
      // Expected
    }

    InOrder ordered = inOrder(this.store, this.cacheLoaderWriter, this.resilienceStrategy);
    ordered.verify(this.store, atLeast(1))
        .bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), everyItem(isIn(contentUpdates.keySet())));
    // ResilienceStrategy invoked; no assertions about Store content
    ordered.verify(this.resilienceStrategy).putAllFailure(eq(contentUpdates), any(StoreAccessException.class));
    ordered.verify(this.cacheLoaderWriter, atLeast(1)).writeAll(getAnyEntryIterable());

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.PutOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.PutAllOutcome.FAILURE));
    assertThat(ehcache.getBulkMethodEntries().get(BulkOps.PUT_ALL).intValue(), is(0));
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#putAll(Map)} for
   * <ul>
   *    <li>non-empty request map</li>
   *    <li>populated {@code Store} - some keys overlap request</li>
   *    <li>populated {@code CacheLoaderWriter} - all keys overlap</li>
   *    <li>no {@link CacheLoaderWriter#writeAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testPutAllStoreSomeOverlapWriterFullOverlapNoneFail() throws Exception {
    Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);

    Map<String, String> originalWriterContent = getEntryMap(KEY_SET_A, KEY_SET_B, KEY_SET_C);
    FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);

    InternalCache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    Map<String, String> contentUpdates = getAltEntryMap("new_", fanIn(KEY_SET_B, KEY_SET_C));
    ehcache.putAll(contentUpdates);

    verify(this.store, atLeast(1)).bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), equalTo(contentUpdates.keySet()));
    assertThat(fakeStore.getEntryMap(), equalTo(union(originalStoreContent, contentUpdates)));
    verify(this.cacheLoaderWriter, atLeast(1)).writeAll(getAnyEntryIterable());
    assertThat(fakeLoaderWriter.getEntryMap(), equalTo(union(originalWriterContent, contentUpdates)));
    verifyZeroInteractions(this.resilienceStrategy);

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.PutOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.PutAllOutcome.SUCCESS));
    assertThat(ehcache.getBulkMethodEntries().get(BulkOps.PUT_ALL).intValue(), is(contentUpdates.size()));
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#putAll(Map)} for
   * <ul>
   *    <li>non-empty request map</li>
   *    <li>populated {@code Store} - some keys overlap request</li>
   *    <li>{@link Store#bulkCompute} throws before accessing writer</li>
   *    <li>populated {@code CacheLoaderWriter} - all keys overlap</li>
   *    <li>no {@link CacheLoaderWriter#writeAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testPutAllStoreSomeOverlapStoreAccessExceptionBeforeWriterFullOverlapNoneFail() throws Exception {
    Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);
    doThrow(new StoreAccessException("")).when(this.store)
        .bulkCompute(getAnyStringSet(), getAnyEntryIterableFunction());

    Map<String, String> originalWriterContent = getEntryMap(KEY_SET_A, KEY_SET_B, KEY_SET_C);
    FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);

    InternalCache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    Map<String, String> contentUpdates = getAltEntryMap("new_", fanIn(KEY_SET_B, KEY_SET_C));
    ehcache.putAll(contentUpdates);

    InOrder ordered = inOrder(this.store, this.cacheLoaderWriter, this.resilienceStrategy);
    ordered.verify(this.store, atLeast(1))
        .bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), everyItem(isIn(contentUpdates.keySet())));
    // ResilienceStrategy invoked; no assertions about Store content
    ordered.verify(this.resilienceStrategy).putAllFailure(eq(contentUpdates), any(StoreAccessException.class));
    ordered.verify(this.cacheLoaderWriter, atLeast(1)).writeAll(getAnyEntryIterable());
    assertThat(fakeLoaderWriter.getEntryMap(), equalTo(union(originalWriterContent, contentUpdates)));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.PutOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.PutAllOutcome.FAILURE));
    assertThat(ehcache.getBulkMethodEntries().get(BulkOps.PUT_ALL).intValue(), is(0));
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#putAll(Map)} for
   * <ul>
   *    <li>non-empty request map</li>
   *    <li>populated {@code Store} - some keys overlap request</li>
   *    <li>{@link Store#bulkCompute} throws after accessing writer</li>
   *    <li>populated {@code CacheLoaderWriter} - all keys overlap</li>
   *    <li>no {@link CacheLoaderWriter#writeAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testPutAllStoreSomeOverlapStoreAccessExceptionAfterWriterFullOverlapNoneFail() throws Exception {
    Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    FakeStore fakeStore = new FakeStore(originalStoreContent, Collections.singleton("keyB3"));
    this.store = spy(fakeStore);

    Map<String, String> originalWriterContent = getEntryMap(KEY_SET_A, KEY_SET_B, KEY_SET_C);
    FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);

    InternalCache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    Map<String, String> contentUpdates = getAltEntryMap("new_", fanIn(KEY_SET_B, KEY_SET_C));
    ehcache.putAll(contentUpdates);

    InOrder ordered = inOrder(this.store, this.cacheLoaderWriter, this.resilienceStrategy);
    ordered.verify(this.store, atLeast(1))
        .bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), everyItem(isIn(contentUpdates.keySet())));
    // ResilienceStrategy invoked; no assertions about Store content
    ordered.verify(this.resilienceStrategy).putAllFailure(eq(contentUpdates), any(StoreAccessException.class));
    ordered.verify(this.cacheLoaderWriter, atLeast(1)).writeAll(getAnyEntryIterable());
    assertThat(fakeLoaderWriter.getEntryMap(), equalTo(union(originalWriterContent, contentUpdates)));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.PutOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.PutAllOutcome.FAILURE));
    assertThat(ehcache.getBulkMethodEntries().get(BulkOps.PUT_ALL).intValue(), is(0));
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#putAll(Map)} for
   * <ul>
   *    <li>non-empty request map</li>
   *    <li>populated {@code Store} - some keys overlap request</li>
   *    <li>populated {@code CacheLoaderWriter} - all keys overlap</li>
   *    <li>some {@link CacheLoaderWriter#writeAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testPutAllStoreSomeOverlapWriterFullOverlapSomeFail() throws Exception {
    Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);

    Map<String, String> originalWriterContent = getEntryMap(KEY_SET_A, KEY_SET_B, KEY_SET_C, KEY_SET_D);
    FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent, KEY_SET_D);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);

    InternalCache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    Map<String, String> contentUpdates = getAltEntryMap("new_", fanIn(KEY_SET_B, KEY_SET_C, KEY_SET_D));
    Set<String> expectedFailures = KEY_SET_D;
    Map<String, String> expectedSuccesses = copyWithout(contentUpdates, expectedFailures);
    try {
      ehcache.putAll(contentUpdates);
      fail();
    } catch (BulkCacheWritingException e) {
      // Expected
      assertThat(e.getSuccesses(), Matchers.equalTo(expectedSuccesses.keySet()));
      assertThat(e.getFailures().keySet(), Matchers.equalTo(expectedFailures));
    }

    verify(this.store, atLeast(1)).bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), everyItem(isIn(contentUpdates.keySet())));
    assertThat(fakeStore.getEntryMap(), equalTo(union(originalStoreContent, expectedSuccesses)));
    verify(this.cacheLoaderWriter, atLeast(1)).writeAll(getAnyEntryIterable());
    assertThat(fakeLoaderWriter.getEntryMap(), equalTo(union(originalWriterContent, expectedSuccesses)));
    verifyZeroInteractions(this.resilienceStrategy);

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.PutOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.PutAllOutcome.FAILURE));
    assertThat(ehcache.getBulkMethodEntries().get(BulkOps.PUT_ALL).intValue(), is(KEY_SET_B.size() + KEY_SET_C.size()));
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#putAll(Map)} for
   * <ul>
   *    <li>non-empty request map</li>
   *    <li>populated {@code Store} - some keys overlap request</li>
   *    <li>{@link Store#bulkCompute} throws before accessing writer</li>
   *    <li>populated {@code CacheLoaderWriter} - all keys overlap</li>
   *    <li>some {@link CacheLoaderWriter#writeAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testPutAllStoreSomeOverlapStoreAccessExceptionBeforeWriterFullOverlapSomeFail() throws Exception {
    Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);
    doThrow(new StoreAccessException("")).when(this.store)
        .bulkCompute(getAnyStringSet(), getAnyEntryIterableFunction());

    Map<String, String> originalWriterContent = getEntryMap(KEY_SET_A, KEY_SET_B, KEY_SET_C, KEY_SET_D);
    FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent, KEY_SET_D);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);

    InternalCache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    Map<String, String> contentUpdates = getAltEntryMap("new_", fanIn(KEY_SET_B, KEY_SET_C, KEY_SET_D));
    Set<String> expectedFailures = KEY_SET_D;
    Map<String, String> expectedSuccesses = copyWithout(contentUpdates, expectedFailures);
    try {
      ehcache.putAll(contentUpdates);
      fail();
    } catch (CacheWritingException e) {
      // Expected
    }

    InOrder ordered = inOrder(this.store, this.cacheLoaderWriter, this.resilienceStrategy);
    ordered.verify(this.store, atLeast(1))
        .bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), everyItem(isIn(contentUpdates.keySet())));
    // ResilienceStrategy invoked; no assertions about Store content
    ordered.verify(this.resilienceStrategy).putAllFailure(eq(contentUpdates), any(StoreAccessException.class));
    ordered.verify(this.cacheLoaderWriter, atLeast(1)).writeAll(getAnyEntryIterable());
    assertThat(fakeLoaderWriter.getEntryMap(), equalTo(union(originalWriterContent, expectedSuccesses)));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.PutOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.PutAllOutcome.FAILURE));
    assertThat(ehcache.getBulkMethodEntries().get(BulkOps.PUT_ALL).intValue(), is(0));
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#putAll(Map)} for
   * <ul>
   *    <li>non-empty request map</li>
   *    <li>populated {@code Store} - some keys overlap request</li>
   *    <li>{@link Store#bulkCompute} throws after accessing writer</li>
   *    <li>populated {@code CacheLoaderWriter} - all keys overlap</li>
   *    <li>some {@link CacheLoaderWriter#writeAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testPutAllStoreSomeOverlapStoreAccessExceptionAfterWriterFullOverlapSomeFail() throws Exception {
    Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    FakeStore fakeStore = new FakeStore(originalStoreContent, Collections.singleton("keyB3"));
    this.store = spy(fakeStore);

    Map<String, String> originalWriterContent = getEntryMap(KEY_SET_A, KEY_SET_B, KEY_SET_C, KEY_SET_D);
    FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent, KEY_SET_D);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);

    InternalCache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    Map<String, String> contentUpdates = getAltEntryMap("new_", fanIn(KEY_SET_B, KEY_SET_C, KEY_SET_D));
    Set<String> expectedFailures = KEY_SET_D;
    Map<String, String> expectedSuccesses = copyWithout(contentUpdates, expectedFailures);
    try {
      ehcache.putAll(contentUpdates);
      fail();
    } catch (CacheWritingException e) {
      // Expected
    }

    InOrder ordered = inOrder(this.store, this.cacheLoaderWriter, this.resilienceStrategy);
    ordered.verify(this.store, atLeast(1))
        .bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), everyItem(isIn(contentUpdates.keySet())));
    // ResilienceStrategy invoked; no assertions about Store content
    ordered.verify(this.resilienceStrategy).putAllFailure(eq(contentUpdates), any(StoreAccessException.class));
    ordered.verify(this.cacheLoaderWriter, atLeast(1)).writeAll(getAnyEntryIterable());
    assertThat(fakeLoaderWriter.getEntryMap(), equalTo(union(originalWriterContent, expectedSuccesses)));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.PutOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.PutAllOutcome.FAILURE));
    assertThat(ehcache.getBulkMethodEntries().get(BulkOps.PUT_ALL).intValue(), is(0));
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#putAll(Map)} for
   * <ul>
   *    <li>non-empty request map</li>
   *    <li>populated {@code Store} - some keys overlap request</li>
   *    <li>populated {@code CacheLoaderWriter} - all keys overlap</li>
   *    <li>some {@link CacheLoaderWriter#writeAll(Iterable)} calls fail</li>
   *    <li>at least one {@link CacheLoaderWriter#writeAll(Iterable)} call aborts</li>
   * </ul>
   */
  @Test
  public void testPutAllStoreSomeOverlapWriterFullOverlapSomeFailWithAbort() throws Exception {
    Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);

    Map<String, String> originalWriterContent = getEntryMap(KEY_SET_A, KEY_SET_B, KEY_SET_C, KEY_SET_D);
    FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent, KEY_SET_D);
    fakeLoaderWriter.setCompleteFailureKey("keyC4");
    this.cacheLoaderWriter = spy(fakeLoaderWriter);

    InternalCache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    Map<String, String> contentUpdates = getAltEntryMap("new_", fanIn(KEY_SET_B, KEY_SET_C, KEY_SET_D));
    Set<String> expectedFailures = union(KEY_SET_D, Collections.singleton("keyC4"));
    Map<String, String> expectedSuccesses = copyWithout(contentUpdates, expectedFailures);
    try {
      ehcache.putAll(contentUpdates);
      fail();
    } catch (BulkCacheWritingException e) {
      // Expected
      assertThat(e.getSuccesses(), Matchers.equalTo(expectedSuccesses.keySet()));
      assertThat(e.getFailures().keySet(), Matchers.equalTo(expectedFailures));
    }

    verify(this.store, atLeast(1))
        .bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), everyItem(isIn(contentUpdates.keySet())));
    assertThat(fakeStore.getEntryMap(), equalTo(union(originalStoreContent, expectedSuccesses)));
    verify(this.cacheLoaderWriter, atLeast(1)).writeAll(getAnyEntryIterable());
    assertThat(fakeLoaderWriter.getEntryMap(), equalTo(union(originalWriterContent, expectedSuccesses)));
    verifyZeroInteractions(this.resilienceStrategy);

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.PutOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.PutAllOutcome.FAILURE));
    assertThat(ehcache.getBulkMethodEntries().get(BulkOps.PUT_ALL).intValue(), is(expectedSuccesses.size()));
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#putAll(Map)} for
   * <ul>
   *    <li>non-empty request map</li>
   *    <li>populated {@code Store} - some keys overlap request</li>
   *    <li>{@link Store#bulkCompute} throws before accessing writer</li>
   *    <li>populated {@code CacheLoaderWriter} - all keys overlap</li>
   *    <li>some {@link CacheLoaderWriter#writeAll(Iterable)} calls fail</li>
   *    <li>at least one {@link CacheLoaderWriter#writeAll(Iterable)} call aborts</li>
   * </ul>
   */
  @Test
  public void testPutAllStoreSomeOverlapStoreAccessExceptionBeforeWriterFullOverlapSomeFailWithAbort() throws Exception {
    Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);
    doThrow(new StoreAccessException("")).when(this.store)
        .bulkCompute(getAnyStringSet(), getAnyEntryIterableFunction());

    Map<String, String> originalWriterContent = getEntryMap(KEY_SET_A, KEY_SET_B, KEY_SET_C, KEY_SET_D);
    FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent, KEY_SET_D);
    fakeLoaderWriter.setCompleteFailureKey("keyC4");
    this.cacheLoaderWriter = spy(fakeLoaderWriter);

    InternalCache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    Map<String, String> contentUpdates = getAltEntryMap("new_", fanIn(KEY_SET_B, KEY_SET_C, KEY_SET_D));
    Set<String> expectedFailures = union(KEY_SET_D, Collections.singleton("keyC4"));
    try {
      ehcache.putAll(contentUpdates);
      fail();
    } catch (CacheWritingException e) {
      // Expected
    }

    InOrder ordered = inOrder(this.store, this.cacheLoaderWriter, this.resilienceStrategy);
    ordered.verify(this.store, atLeast(1))
        .bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), everyItem(isIn(contentUpdates.keySet())));
    // ResilienceStrategy invoked; no assertions about Store content
    ordered.verify(this.resilienceStrategy).putAllFailure(eq(contentUpdates), any(StoreAccessException.class));
    ordered.verify(this.cacheLoaderWriter, atLeast(1)).writeAll(getAnyEntryIterable());

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.PutOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.PutAllOutcome.FAILURE));
    assertThat(ehcache.getBulkMethodEntries().get(BulkOps.PUT_ALL).intValue(), is(0));
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#putAll(Map)} for
   * <ul>
   *    <li>non-empty request map</li>
   *    <li>populated {@code Store} - some keys overlap request</li>
   *    <li>{@link Store#bulkCompute} throws after accessing writer</li>
   *    <li>populated {@code CacheLoaderWriter} - all keys overlap</li>
   *    <li>some {@link CacheLoaderWriter#writeAll(Iterable)} calls fail</li>
   *    <li>at least one {@link CacheLoaderWriter#writeAll(Iterable)} call aborts</li>
   * </ul>
   */
  @Test
  public void testPutAllStoreSomeOverlapStoreAccessExceptionAfterWriterFullOverlapSomeFailWithAbort() throws Exception {
    Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    FakeStore fakeStore = new FakeStore(originalStoreContent, Collections.singleton("keyB3"));
    this.store = spy(fakeStore);

    Map<String, String> originalWriterContent = getEntryMap(KEY_SET_A, KEY_SET_B, KEY_SET_C, KEY_SET_D);
    FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent, KEY_SET_D);
    fakeLoaderWriter.setCompleteFailureKey("keyC4");
    this.cacheLoaderWriter = spy(fakeLoaderWriter);

    InternalCache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    Map<String, String> contentUpdates = getAltEntryMap("new_", fanIn(KEY_SET_B, KEY_SET_C, KEY_SET_D));
    Set<String> expectedFailures = union(KEY_SET_D, Collections.singleton("keyC4"));
    Map<String, String> expectedSuccesses = copyWithout(contentUpdates, expectedFailures);
    try {
      ehcache.putAll(contentUpdates);
      fail();
    } catch (CacheWritingException e) {
      // Expected
    }

    InOrder ordered = inOrder(this.store, this.cacheLoaderWriter, this.resilienceStrategy);
    ordered.verify(this.store, atLeast(1))
        .bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), everyItem(isIn(contentUpdates.keySet())));
    // ResilienceStrategy invoked; no assertions about Store content
    ordered.verify(this.resilienceStrategy).putAllFailure(eq(contentUpdates), any(StoreAccessException.class));
    ordered.verify(this.cacheLoaderWriter, atLeast(1)).writeAll(getAnyEntryIterable());

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.PutOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.PutAllOutcome.FAILURE));
    assertThat(ehcache.getBulkMethodEntries().get(BulkOps.PUT_ALL).intValue(), is(0));
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#putAll(Map)} for
   * <ul>
   *    <li>non-empty request map</li>
   *    <li>populated {@code Store} - some keys overlap request</li>
   *    <li>populated {@code CacheLoaderWriter} - all keys overlap</li>
   *    <li>all {@link CacheLoaderWriter#writeAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testPutAllStoreSomeOverlapWriterFullOverlapAllFail() throws Exception {
    Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);

    Map<String, String> originalWriterContent = getEntryMap(KEY_SET_A, KEY_SET_B, KEY_SET_C, KEY_SET_D);
    FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);
    doThrow(new Exception("writeAll failed")).when(this.cacheLoaderWriter).writeAll(getAnyEntryIterable());

    InternalCache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    Map<String, String> contentUpdates = getAltEntryMap("new_", fanIn(KEY_SET_B, KEY_SET_C, KEY_SET_D));
    try {
      ehcache.putAll(contentUpdates);
      fail();
    } catch (BulkCacheWritingException e) {
      // Expected
      assertThat(e.getSuccesses(), empty());
      assertThat(e.getFailures().keySet(), Matchers.equalTo(contentUpdates.keySet()));
    }

    verify(this.store, atLeast(1))
        .bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), everyItem(isIn(contentUpdates.keySet())));
    assertThat(fakeStore.getEntryMap(), equalTo(getEntryMap(KEY_SET_A, KEY_SET_B)));
    verify(this.cacheLoaderWriter, atLeast(1)).writeAll(getAnyEntryIterable());
    verifyZeroInteractions(this.resilienceStrategy);

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.PutOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.PutAllOutcome.FAILURE));
    assertThat(ehcache.getBulkMethodEntries().get(BulkOps.PUT_ALL).intValue(), is(0));
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#putAll(Map)} for
   * <ul>
   *    <li>non-empty request map</li>
   *    <li>populated {@code Store} - some keys overlap request</li>
   *    <li>{@link Store#bulkCompute} throws before accessing writer</li>
   *    <li>populated {@code CacheLoaderWriter} - all keys overlap</li>
   *    <li>all {@link CacheLoaderWriter#writeAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testPutAllStoreSomeOverlapStoreAccessExceptionBeforeWriterFullOverlapAllFail() throws Exception {
    Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);
    doThrow(new StoreAccessException("")).when(this.store)
        .bulkCompute(getAnyStringSet(), getAnyEntryIterableFunction());

    Map<String, String> originalWriterContent = getEntryMap(KEY_SET_A, KEY_SET_B, KEY_SET_C, KEY_SET_D);
    FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);
    doThrow(new Exception("writeAll failed")).when(this.cacheLoaderWriter).writeAll(getAnyEntryIterable());

    InternalCache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    Map<String, String> contentUpdates = getAltEntryMap("new_", fanIn(KEY_SET_B, KEY_SET_C, KEY_SET_D));
    try {
      ehcache.putAll(contentUpdates);
      fail();
    } catch (CacheWritingException e) {
      // Expected
    }

    InOrder ordered = inOrder(this.store, this.cacheLoaderWriter, this.resilienceStrategy);
    ordered.verify(this.store, atLeast(1))
        .bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), everyItem(isIn(contentUpdates.keySet())));
    // ResilienceStrategy invoked; no assertions about Store content
    ordered.verify(this.resilienceStrategy).putAllFailure(eq(contentUpdates), any(StoreAccessException.class));
    ordered.verify(this.cacheLoaderWriter, atLeast(1)).writeAll(getAnyEntryIterable());
    assertThat(fakeLoaderWriter.getEntryMap(), equalTo(originalWriterContent));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.PutOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.PutAllOutcome.FAILURE));
    assertThat(ehcache.getBulkMethodEntries().get(BulkOps.PUT_ALL).intValue(), is(0));
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#putAll(Map)} for
   * <ul>
   *    <li>non-empty request map</li>
   *    <li>populated {@code Store} - some keys overlap request</li>
   *    <li>{@link Store#bulkCompute} throws after accessing writer</li>
   *    <li>populated {@code CacheLoaderWriter} - all keys overlap</li>
   *    <li>all {@link CacheLoaderWriter#writeAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testPutAllStoreSomeOverlapStoreAccessExceptionAfterWriterFullOverlapAllFail() throws Exception {
    Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    FakeStore fakeStore = new FakeStore(originalStoreContent, Collections.singleton("keyB3"));
    this.store = spy(fakeStore);

    Map<String, String> originalWriterContent = getEntryMap(KEY_SET_A, KEY_SET_B, KEY_SET_C, KEY_SET_D);
    FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);
    doThrow(new Exception("writeAll failed")).when(this.cacheLoaderWriter).writeAll(getAnyEntryIterable());

    InternalCache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    Map<String, String> contentUpdates = getAltEntryMap("new_", fanIn(KEY_SET_B, KEY_SET_C, KEY_SET_D));
    try {
      ehcache.putAll(contentUpdates);
      fail();
    } catch (CacheWritingException e) {
      // Expected
    }

    InOrder ordered = inOrder(this.store, this.cacheLoaderWriter, this.resilienceStrategy);
    ordered.verify(this.store, atLeast(1))
        .bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), everyItem(isIn(contentUpdates.keySet())));
    // ResilienceStrategy invoked; no assertions about Store content
    ordered.verify(this.resilienceStrategy).putAllFailure(eq(contentUpdates), any(StoreAccessException.class));
    ordered.verify(this.cacheLoaderWriter, atLeast(1)).writeAll(getAnyEntryIterable());
    assertThat(fakeLoaderWriter.getEntryMap(), equalTo(originalWriterContent));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.PutOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.PutAllOutcome.FAILURE));
    assertThat(ehcache.getBulkMethodEntries().get(BulkOps.PUT_ALL).intValue(), is(0));
  }

  @Test
  public void testPutAllPartialIntersectionsImmediatelyExpiredCreatedEntries() throws Exception {
    Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);

    Map<String, String> originalWriterContent = getEntryMap(KEY_SET_A, KEY_SET_B, KEY_SET_C);
    FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);

    @SuppressWarnings("unchecked")
    ExpiryPolicy<String, String> expiry = mock(ExpiryPolicy.class);
    when(expiry.getExpiryForCreation(any(String.class), any(String.class))).thenReturn(Duration.ZERO);

    EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(cacheLoaderWriter, expiry);

    Map<String, String> contentUpdates = getAltEntryMap("new_", fanIn(KEY_SET_A, KEY_SET_C, KEY_SET_D));
    ehcache.putAll(contentUpdates);

    verify(this.store, atLeast(1)).bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), equalTo(contentUpdates.keySet()));
    assertThat(fakeStore.getEntryMap(), equalTo(union(getAltEntryMap("new_", KEY_SET_A), getEntryMap(KEY_SET_B))));
    verify(this.cacheLoaderWriter, atLeast(1)).writeAll(getAnyEntryIterable());
    assertThat(fakeLoaderWriter.getEntryMap(), equalTo(union(originalWriterContent, contentUpdates)));
    verifyZeroInteractions(this.resilienceStrategy);

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.PutOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.PutAllOutcome.SUCCESS));
    assertThat(ehcache.getBulkMethodEntries().get(BulkOps.PUT_ALL).intValue(), is(KEY_SET_A.size()));
  }

  @Test
  public void testPutAllPartialIntersectionsImmediatelyExpiredUpdatedEntries() throws Exception {
    Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);

    Map<String, String> originalWriterContent = getEntryMap(KEY_SET_A, KEY_SET_B, KEY_SET_C);
    FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);

    @SuppressWarnings("unchecked")
    ExpiryPolicy<String, String> expiry = mock(ExpiryPolicy.class);
    when(expiry.getExpiryForUpdate(any(String.class), argThat(org.ehcache.core.util.Matchers.<String>holding(instanceOf(String.class))), any(String.class))).thenReturn(Duration.ZERO);

    EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(cacheLoaderWriter, expiry);

    Map<String, String> contentUpdates = getAltEntryMap("new_", fanIn(KEY_SET_A, KEY_SET_C, KEY_SET_D));
    ehcache.putAll(contentUpdates);

    verify(this.store, atLeast(1)).bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), equalTo(contentUpdates.keySet()));
    assertThat(fakeStore.getEntryMap(), equalTo(union(getEntryMap(KEY_SET_B), getAltEntryMap("new_", union(KEY_SET_C, KEY_SET_D)))));
    verify(this.cacheLoaderWriter, atLeast(1)).writeAll(getAnyEntryIterable());
    assertThat(fakeLoaderWriter.getEntryMap(), equalTo(union(originalWriterContent, contentUpdates)));
    verifyZeroInteractions(this.resilienceStrategy);

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.PutOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.PutAllOutcome.SUCCESS));
    assertThat(ehcache.getBulkMethodEntries().get(BulkOps.PUT_ALL).intValue(), is(KEY_SET_C.size() + KEY_SET_D.size()));
  }

  private void assertThatAllStoreEntriesWithoutFailuresMatchWriterState(FakeStore fakeStore, FakeCacheLoaderWriter fakeLoaderWriter, Map<String, Exception> bcweFailures) {
    assertThat(copyWithout(fakeStore.getEntryMap(), bcweFailures.keySet()).entrySet(), everyItem(isIn(fakeLoaderWriter.getEntryMap()
        .entrySet())));
  }

  private EhcacheWithLoaderWriter<String, String> getEhcache(CacheLoaderWriter<String, String> cacheLoaderWriter) {
    return getEhcache(cacheLoaderWriter, CACHE_CONFIGURATION);
  }

  private EhcacheWithLoaderWriter<String, String> getEhcache(CacheLoaderWriter<String, String> cacheLoaderWriter, ExpiryPolicy<String, String> expiry) {
    CacheConfiguration<String, String> config = new BaseCacheConfiguration<>(String.class, String.class, null, null,
      expiry, ResourcePoolsHelper.createHeapOnlyPools());
    return getEhcache(cacheLoaderWriter, config);
  }

  private EhcacheWithLoaderWriter<String, String> getEhcache(CacheLoaderWriter<String, String> cacheLoaderWriter, CacheConfiguration<String, String> config) {
    this.resilienceStrategy = spy(new RobustLoaderWriterResilienceStrategy<>(new DefaultRecoveryStore<>(this.store), cacheLoaderWriter));
    EhcacheWithLoaderWriter<String, String> ehcache = new EhcacheWithLoaderWriter<>(config, this.store, resilienceStrategy, cacheLoaderWriter, cacheEventDispatcher, LoggerFactory
      .getLogger(EhcacheWithLoaderWriter.class + "-" + "EhcacheBasicPutAllTest"));
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
