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
import org.ehcache.config.CacheConfiguration;
import org.ehcache.core.config.BaseCacheConfiguration;
import org.ehcache.core.config.ResourcePoolsHelper;
import org.ehcache.core.spi.store.Store;
import org.ehcache.core.spi.function.NullaryFunction;
import org.ehcache.core.statistics.BulkOps;
import org.ehcache.core.statistics.CacheOperationOutcomes;
import org.ehcache.spi.loaderwriter.BulkCacheWritingException;
import org.ehcache.core.spi.store.StoreAccessException;
import org.ehcache.expiry.Duration;
import org.ehcache.expiry.Expiry;
import org.ehcache.core.spi.function.Function;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
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
   * {@link Store#bulkCompute(Set, Function, NullaryFunction)
   *    Store.bulkCompute(Set, Function, NullaryFunction} method.
   */
  @Captor
  private ArgumentCaptor<Set<String>> bulkComputeSetCaptor;

  /**
   * A Mockito {@code ArgumentCaptor} for the
   * {@link BulkCacheWritingException BulkCacheWritingException}
   * provided to the
   * {@link org.ehcache.core.internal.resilience.ResilienceStrategy#putAllFailure(Map, StoreAccessException, BulkCacheWritingException)}
   *    ResilienceStrategy.putAllFailure(Iterable, StoreAccessException, BulkCacheWritingException)} method.
   */
  @Captor
  private ArgumentCaptor<BulkCacheWritingException> bulkExceptionCaptor;

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
    final Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    final FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalStoreContent);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);

    final InternalCache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);
    ehcache.putAll(Collections.<String, String>emptyMap());

    verify(this.store, never()).bulkCompute(eq(Collections.<String>emptySet()), getAnyEntryIterableFunction());
    assertThat(fakeStore.getEntryMap(), equalTo(originalStoreContent));
    assertThat(fakeLoaderWriter.getEntryMap(), equalTo(originalStoreContent));
    verify(this.spiedResilienceStrategy, never()).putAllFailure(eq(Collections.<String, String>emptyMap()), any(StoreAccessException.class));
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
    final Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    final FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);

    final Map<String, String> originalWriterContent = getEntryMap(KEY_SET_A, KEY_SET_B, KEY_SET_D);
    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);

    final InternalCache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    final Map<String, String> contentUpdates = getAltEntryMap("new_", fanIn(KEY_SET_A, KEY_SET_C));
    ehcache.putAll(contentUpdates);

    verify(this.store, atLeast(1)).bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), equalTo(contentUpdates.keySet()));
    assertThat(fakeStore.getEntryMap(), equalTo(union(originalStoreContent, contentUpdates)));
    assertThat(fakeLoaderWriter.getEntryMap(), equalTo(union(originalWriterContent, contentUpdates)));
    verifyZeroInteractions(this.spiedResilienceStrategy);

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
    final Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    final FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);
    doThrow(new StoreAccessException("")).when(this.store)
        .bulkCompute(getAnyStringSet(), getAnyEntryIterableFunction());

    final Map<String, String> originalWriterContent = getEntryMap(KEY_SET_A, KEY_SET_B, KEY_SET_D);
    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);

    final InternalCache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    final Map<String, String> contentUpdates = getAltEntryMap("new_", fanIn(KEY_SET_A, KEY_SET_C));
    ehcache.putAll(contentUpdates);

    final InOrder ordered = inOrder(this.store, this.cacheLoaderWriter, this.spiedResilienceStrategy);
    ordered.verify(this.store, atLeast(1)).bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), everyItem(isIn(contentUpdates.keySet())));
    // ResilienceStrategy invoked; no assertions about Store content
    ordered.verify(this.cacheLoaderWriter, atLeast(1)).writeAll(getAnyEntryIterable());
    assertThat(fakeLoaderWriter.getEntryMap(), equalTo(union(originalWriterContent, contentUpdates)));
    ordered.verify(this.spiedResilienceStrategy)
        .putAllFailure(eq(contentUpdates), any(StoreAccessException.class));

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
    final Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    final FakeStore fakeStore = new FakeStore(originalStoreContent, Collections.singleton("keyA3"));
    this.store = spy(fakeStore);

    final Map<String, String> originalWriterContent = getEntryMap(KEY_SET_A, KEY_SET_B, KEY_SET_D);
    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);

    final InternalCache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    final Map<String, String> contentUpdates = getAltEntryMap("new_", fanIn(KEY_SET_A, KEY_SET_C));
    ehcache.putAll(contentUpdates);

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.PutOutcome.class));

    final InOrder ordered = inOrder(this.store, this.cacheLoaderWriter, this.spiedResilienceStrategy);
    ordered.verify(this.store, atLeast(1))
        .bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), everyItem(isIn(contentUpdates.keySet())));
    // ResilienceStrategy invoked; no assertions about Store content
    ordered.verify(this.cacheLoaderWriter, atLeast(1)).writeAll(getAnyEntryIterable());
    assertThat(fakeLoaderWriter.getEntryMap(), equalTo(union(originalWriterContent, contentUpdates)));
    ordered.verify(this.spiedResilienceStrategy)
        .putAllFailure(eq(contentUpdates), any(StoreAccessException.class));

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
    final Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    final FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);

    final Map<String, String> originalWriterContent = getEntryMap(KEY_SET_A, KEY_SET_B, KEY_SET_D);
    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent, KEY_SET_C);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);

    final InternalCache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    final Map<String, String> contentUpdates = getAltEntryMap("new_", fanIn(KEY_SET_A, KEY_SET_C));
    final Set<String> expectedFailures = KEY_SET_C;
    final Map<String, String> expectedSuccesses = copyWithout(contentUpdates, expectedFailures);
    try {
      ehcache.putAll(contentUpdates);
      fail();
    } catch (BulkCacheWritingException e) {
      // Expected
      assertThat(e.getSuccesses(), Matchers.<Set<?>>equalTo(expectedSuccesses.keySet()));
      assertThat(e.getFailures().keySet(), Matchers.<Set<?>>equalTo(expectedFailures));
    }

    verify(this.store, atLeast(1)).bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), equalTo(contentUpdates.keySet()));
    assertThat(fakeStore.getEntryMap(), equalTo(union(originalStoreContent, expectedSuccesses)));
    assertThat(fakeLoaderWriter.getEntryMap(), equalTo(union(originalWriterContent, expectedSuccesses)));
    verifyZeroInteractions(this.spiedResilienceStrategy);

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
    final Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    final FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);
    doThrow(new StoreAccessException("")).when(this.store)
        .bulkCompute(getAnyStringSet(), getAnyEntryIterableFunction());

    final Map<String, String> originalWriterContent = getEntryMap(KEY_SET_A, KEY_SET_B, KEY_SET_D);
    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent, KEY_SET_C);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);

    final InternalCache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    final Map<String, String> contentUpdates = getAltEntryMap("new_", fanIn(KEY_SET_A, KEY_SET_C));
    final Set<String> expectedFailures = KEY_SET_C;
    final Map<String, String> expectedSuccesses = copyWithout(contentUpdates, expectedFailures);
    try {
      ehcache.putAll(contentUpdates);
      fail();
    } catch (BulkCacheWritingException e) {
      // Expected
    }

    final InOrder ordered = inOrder(this.store, this.cacheLoaderWriter, this.spiedResilienceStrategy);
    ordered.verify(this.store, atLeast(1)).bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), everyItem(isIn(contentUpdates.keySet())));
    // ResilienceStrategy invoked; no assertions about Store content

    ordered.verify(this.cacheLoaderWriter, atLeast(1)).writeAll(getAnyEntryIterable());
    assertThat(fakeLoaderWriter.getEntryMap(), equalTo(union(originalWriterContent, expectedSuccesses)));

    ordered.verify(this.spiedResilienceStrategy)
        .putAllFailure(eq(contentUpdates), any(StoreAccessException.class), this.bulkExceptionCaptor.capture());
    assertThat(this.bulkExceptionCaptor.getValue().getSuccesses(), Matchers.<Set<?>>equalTo(expectedSuccesses.keySet()));
    assertThat(this.bulkExceptionCaptor.getValue().getFailures().keySet(), Matchers.<Set<?>>equalTo(expectedFailures));

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
    final Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    final FakeStore fakeStore = new FakeStore(originalStoreContent, Collections.singleton("keyA3"));
    this.store = spy(fakeStore);

    final Map<String, String> originalWriterContent = getEntryMap(KEY_SET_A, KEY_SET_B, KEY_SET_D);
    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent, KEY_SET_C);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);

    final InternalCache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    final Map<String, String> contentUpdates = getAltEntryMap("new_", fanIn(KEY_SET_A, KEY_SET_C));
    final Set<String> expectedFailures = KEY_SET_C;
    final Map<String, String> expectedSuccesses = copyWithout(contentUpdates, expectedFailures);
    try {
      ehcache.putAll(contentUpdates);
      fail();
    } catch (BulkCacheWritingException e) {
      // Expected
    }

    final InOrder ordered = inOrder(this.store, this.cacheLoaderWriter, this.spiedResilienceStrategy);
    ordered.verify(this.store, atLeast(1)).bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), everyItem(isIn(contentUpdates.keySet())));
    // ResilienceStrategy invoked; no assertions about Store content

    ordered.verify(this.cacheLoaderWriter, atLeast(1)).writeAll(getAnyEntryIterable());
    assertThat(fakeLoaderWriter.getEntryMap(), equalTo(union(originalWriterContent, expectedSuccesses)));

    ordered.verify(this.spiedResilienceStrategy)
        .putAllFailure(eq(contentUpdates), any(StoreAccessException.class), this.bulkExceptionCaptor.capture());
    assertThat(this.bulkExceptionCaptor.getValue().getSuccesses(), Matchers.<Set<?>>equalTo(expectedSuccesses.keySet()));
    assertThat(this.bulkExceptionCaptor.getValue().getFailures().keySet(), Matchers.<Set<?>>equalTo(expectedFailures));

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
    final Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    final FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);

    final Map<String, String> originalWriterContent = getEntryMap(KEY_SET_A, KEY_SET_B, KEY_SET_D);
    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent, KEY_SET_C);
    fakeLoaderWriter.setCompleteFailureKey("keyC4");
    this.cacheLoaderWriter = spy(fakeLoaderWriter);

    final InternalCache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    final Map<String, String> contentUpdates = getAltEntryMap("new_", fanIn(KEY_SET_A, KEY_SET_C));
    final Set<String> expectedFailures = KEY_SET_C;
    final Map<String, String> expectedSuccesses = copyWithout(contentUpdates, expectedFailures);
    try {
      ehcache.putAll(contentUpdates);
      fail();
    } catch (BulkCacheWritingException e) {
      // Expected
      assertThat(e.getSuccesses(), Matchers.<Set<?>>equalTo(expectedSuccesses.keySet()));
      assertThat(e.getFailures().keySet(), Matchers.<Set<?>>equalTo(expectedFailures));
    }

    verify(this.store, atLeast(1)).bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), everyItem(isIn(contentUpdates.keySet())));
    assertThat(fakeStore.getEntryMap(), equalTo(union(originalStoreContent, expectedSuccesses)));
    verify(this.cacheLoaderWriter, atLeast(1)).writeAll(getAnyEntryIterable());
    assertThat(fakeLoaderWriter.getEntryMap(), equalTo(union(originalWriterContent, expectedSuccesses)));
    verifyZeroInteractions(this.spiedResilienceStrategy);

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
    final Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    final FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);
    doThrow(new StoreAccessException("")).when(this.store)
        .bulkCompute(getAnyStringSet(), getAnyEntryIterableFunction());

    final Map<String, String> originalWriterContent = getEntryMap(KEY_SET_A, KEY_SET_B, KEY_SET_D);
    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent, KEY_SET_C);
    fakeLoaderWriter.setCompleteFailureKey("keyC4");
    this.cacheLoaderWriter = spy(fakeLoaderWriter);

    final InternalCache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    final Map<String, String> contentUpdates = getAltEntryMap("new_", fanIn(KEY_SET_A, KEY_SET_C));
    final Set<String> expectedFailures = contentUpdates.keySet();

    try {
      ehcache.putAll(contentUpdates);
      fail();
    } catch (BulkCacheWritingException e) {
      // Expected
    }

    final InOrder ordered = inOrder(this.store, this.cacheLoaderWriter, this.spiedResilienceStrategy);
    ordered.verify(this.store, atLeast(1)).bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), everyItem(isIn(contentUpdates.keySet())));
    // ResilienceStrategy invoked; no assertions about Store content
    ordered.verify(this.cacheLoaderWriter, atLeast(1)).writeAll(getAnyEntryIterable());
    ordered.verify(this.spiedResilienceStrategy)
        .putAllFailure(eq(contentUpdates), any(StoreAccessException.class), this.bulkExceptionCaptor.capture());

    @SuppressWarnings("unchecked")
    final Set<String> bcweSuccesses = (Set<String>)this.bulkExceptionCaptor.getValue().getSuccesses();
    @SuppressWarnings("unchecked")
    final Map<String, Exception> bcweFailures = (Map<String, Exception>)this.bulkExceptionCaptor.getValue().getFailures();

    assertThat(bcweSuccesses.isEmpty(), is(true));
    assertThat(bcweFailures.keySet(), equalTo(expectedFailures));
    assertThatAllStoreEntriesWithoutFailuresMatchWriterState(fakeStore, fakeLoaderWriter, bcweFailures);

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
    final Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    final FakeStore fakeStore = new FakeStore(originalStoreContent, Collections.singleton("keyA3"));
    this.store = spy(fakeStore);

    final Map<String, String> originalWriterContent = getEntryMap(KEY_SET_A, KEY_SET_B, KEY_SET_D);
    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent, KEY_SET_C);
    fakeLoaderWriter.setCompleteFailureKey("keyC4");
    this.cacheLoaderWriter = spy(fakeLoaderWriter);

    final InternalCache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    final Map<String, String> contentUpdates = getAltEntryMap("new_", fanIn(KEY_SET_A, KEY_SET_C));
    final Set<String> expectedFailures = KEY_SET_C;
    final Map<String, String> expectedSuccesses = copyWithout(contentUpdates, expectedFailures);
    try {
      ehcache.putAll(contentUpdates);
      fail();
    } catch (BulkCacheWritingException e) {
      // Expected
    }

    final InOrder ordered = inOrder(this.store, this.cacheLoaderWriter, this.spiedResilienceStrategy);
    ordered.verify(this.store, atLeast(1)).bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), everyItem(isIn(contentUpdates.keySet())));
    // ResilienceStrategy invoked; no assertions about Store content
    ordered.verify(this.cacheLoaderWriter, atLeast(1)).writeAll(getAnyEntryIterable());
    ordered.verify(this.spiedResilienceStrategy)
        .putAllFailure(eq(contentUpdates), any(StoreAccessException.class), this.bulkExceptionCaptor.capture());

    @SuppressWarnings("unchecked")
    final Set<String> bcweSuccesses = (Set<String>)this.bulkExceptionCaptor.getValue().getSuccesses();
    @SuppressWarnings("unchecked")
    final Map<String, Exception> bcweFailures = (Map<String, Exception>)this.bulkExceptionCaptor.getValue().getFailures();

    assertThat(union(bcweSuccesses, bcweFailures.keySet()), equalTo(contentUpdates.keySet()));
    assertThat(Collections.disjoint(bcweSuccesses, bcweFailures.keySet()), is(true));
    assertThatAllStoreEntriesWithoutFailuresMatchWriterState(fakeStore, fakeLoaderWriter, bcweFailures);

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
    final Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    final FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);

    final Map<String, String> originalWriterContent = getEntryMap(KEY_SET_A, KEY_SET_B, KEY_SET_D);
    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);
    doThrow(new Exception("writeAll failed")).when(this.cacheLoaderWriter).writeAll(getAnyEntryIterable());

    final InternalCache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    final Map<String, String> contentUpdates = getAltEntryMap("new_", fanIn(KEY_SET_A, KEY_SET_C));
    try {
      ehcache.putAll(contentUpdates);
      fail();
    } catch (BulkCacheWritingException e) {
      // Expected
      assertThat(e.getSuccesses(), empty());
      assertThat(e.getFailures().keySet(), Matchers.<Set<?>>equalTo(contentUpdates.keySet()));
    }

    verify(this.store, atLeast(1)).bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), everyItem(isIn(contentUpdates.keySet())));
    assertThat(fakeStore.getEntryMap(), equalTo(copyWithout(originalStoreContent, contentUpdates.keySet())));
    verify(this.cacheLoaderWriter, atLeast(1)).writeAll(getAnyEntryIterable());
    verifyZeroInteractions(this.spiedResilienceStrategy);

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
    final Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    final FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);
    doThrow(new StoreAccessException("")).when(this.store)
        .bulkCompute(getAnyStringSet(), getAnyEntryIterableFunction());

    final Map<String, String> originalWriterContent = getEntryMap(KEY_SET_A, KEY_SET_B, KEY_SET_D);
    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);
    doThrow(new Exception("writeAll failed")).when(this.cacheLoaderWriter).writeAll(getAnyEntryIterable());

    final InternalCache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    final Map<String, String> contentUpdates = getAltEntryMap("new_", fanIn(KEY_SET_A, KEY_SET_C));
    try {
      ehcache.putAll(contentUpdates);
      fail();
    } catch (BulkCacheWritingException e) {
      // Expected
    }

    final InOrder ordered = inOrder(this.store, this.cacheLoaderWriter, this.spiedResilienceStrategy);
    ordered.verify(this.store, atLeast(1)).bulkCompute(this.bulkComputeSetCaptor.capture(),
        getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), everyItem(isIn(contentUpdates.keySet())));
    // ResilienceStrategy invoked; no assertions about Store content
    ordered.verify(this.cacheLoaderWriter, atLeast(1)).writeAll(getAnyEntryIterable());
    ordered.verify(this.spiedResilienceStrategy)
        .putAllFailure(eq(contentUpdates), any(StoreAccessException.class), this.bulkExceptionCaptor.capture());

    assertThat(this.bulkExceptionCaptor.getValue().getSuccesses(), empty());
    assertThat(this.bulkExceptionCaptor.getValue().getFailures().keySet(),
        Matchers.<Set<?>>equalTo(contentUpdates.keySet()));

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
    final Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    final FakeStore fakeStore = new FakeStore(originalStoreContent, Collections.singleton("keyA3"));
    this.store = spy(fakeStore);

    final Map<String, String> originalWriterContent = getEntryMap(KEY_SET_A, KEY_SET_B, KEY_SET_D);
    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);
    doThrow(new Exception("writeAll failed")).when(this.cacheLoaderWriter).writeAll(getAnyEntryIterable());

    final InternalCache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    final Map<String, String> contentUpdates = getAltEntryMap("new_", fanIn(KEY_SET_A, KEY_SET_C));
    try {
      ehcache.putAll(contentUpdates);
      fail();
    } catch (BulkCacheWritingException e) {
      // Expected
    }

    final InOrder ordered = inOrder(this.store, this.cacheLoaderWriter, this.spiedResilienceStrategy);
    ordered.verify(this.store, atLeast(1)).bulkCompute(this.bulkComputeSetCaptor.capture(),
        getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), everyItem(isIn(contentUpdates.keySet())));
    // ResilienceStrategy invoked; no assertions about Store content
    ordered.verify(this.cacheLoaderWriter, atLeast(1)).writeAll(getAnyEntryIterable());
    ordered.verify(this.spiedResilienceStrategy)
        .putAllFailure(eq(contentUpdates), any(StoreAccessException.class), this.bulkExceptionCaptor.capture());

    assertThat(this.bulkExceptionCaptor.getValue().getSuccesses(), empty());
    assertThat(this.bulkExceptionCaptor.getValue().getFailures().keySet(),
        Matchers.<Set<?>>equalTo(contentUpdates.keySet()));

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
    final Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    final FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);

    final Map<String, String> originalWriterContent = getEntryMap(KEY_SET_A, KEY_SET_B, KEY_SET_C);
    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);

    final InternalCache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    final Map<String, String> contentUpdates = getAltEntryMap("new_", fanIn(KEY_SET_A, KEY_SET_C, KEY_SET_D));
    ehcache.putAll(contentUpdates);

    verify(this.store, atLeast(1)).bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), equalTo(contentUpdates.keySet()));
    assertThat(fakeStore.getEntryMap(), equalTo(union(originalStoreContent, contentUpdates)));
    verify(this.cacheLoaderWriter, atLeast(1)).writeAll(getAnyEntryIterable());
    assertThat(fakeLoaderWriter.getEntryMap(), equalTo(union(originalWriterContent, contentUpdates)));
    verifyZeroInteractions(this.spiedResilienceStrategy);

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
    final Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    final FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);
    doThrow(new StoreAccessException("")).when(this.store)
        .bulkCompute(getAnyStringSet(), getAnyEntryIterableFunction());

    final Map<String, String> originalWriterContent = getEntryMap(KEY_SET_A, KEY_SET_B, KEY_SET_C);
    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);

    final InternalCache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    final Map<String, String> contentUpdates = getAltEntryMap("new_", fanIn(KEY_SET_A, KEY_SET_C, KEY_SET_D));
    ehcache.putAll(contentUpdates);

    final InOrder ordered = inOrder(this.store, this.cacheLoaderWriter, this.spiedResilienceStrategy);
    ordered.verify(this.store, atLeast(1))
        .bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), everyItem(isIn(contentUpdates.keySet())));
    // ResilienceStrategy invoked; no assertions about Store content
    ordered.verify(this.cacheLoaderWriter, atLeast(1)).writeAll(getAnyEntryIterable());
    assertThat(fakeLoaderWriter.getEntryMap(), equalTo(union(originalWriterContent, contentUpdates)));
    ordered.verify(this.spiedResilienceStrategy)
        .putAllFailure(eq(contentUpdates), any(StoreAccessException.class));

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
    final Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    final FakeStore fakeStore = new FakeStore(originalStoreContent, Collections.singleton("keyA3"));
    this.store = spy(fakeStore);

    final Map<String, String> originalWriterContent = getEntryMap(KEY_SET_A, KEY_SET_B, KEY_SET_C);
    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);

    final InternalCache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    final Map<String, String> contentUpdates = getAltEntryMap("new_", fanIn(KEY_SET_A, KEY_SET_C, KEY_SET_D));
    ehcache.putAll(contentUpdates);

    final InOrder ordered = inOrder(this.store, this.cacheLoaderWriter, this.spiedResilienceStrategy);
    ordered.verify(this.store, atLeast(1))
        .bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), everyItem(isIn(contentUpdates.keySet())));
    // ResilienceStrategy invoked; no assertions about Store content
    ordered.verify(this.cacheLoaderWriter, atLeast(1)).writeAll(getAnyEntryIterable());
    assertThat(fakeLoaderWriter.getEntryMap(), equalTo(union(originalWriterContent, contentUpdates)));
    ordered.verify(this.spiedResilienceStrategy)
        .putAllFailure(eq(contentUpdates), any(StoreAccessException.class));

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
    final Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    final FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);

    final Map<String, String> originalWriterContent = getEntryMap(KEY_SET_A, KEY_SET_B, KEY_SET_C);
    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent, KEY_SET_D);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);

    final InternalCache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    final Map<String, String> contentUpdates = getAltEntryMap("new_", fanIn(KEY_SET_A, KEY_SET_C, KEY_SET_D));
    final Set<String> expectedFailures = KEY_SET_D;
    final Map<String, String> expectedSuccesses = copyWithout(contentUpdates, expectedFailures);
    try {
      ehcache.putAll(contentUpdates);
      fail();
    } catch (BulkCacheWritingException e) {
      // Expected
      assertThat(e.getSuccesses(), Matchers.<Set<?>>equalTo(expectedSuccesses.keySet()));
      assertThat(e.getFailures().keySet(), Matchers.<Set<?>>equalTo(expectedFailures));
    }

    verify(this.store, atLeast(1)).bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), everyItem(isIn(contentUpdates.keySet())));
    assertThat(fakeStore.getEntryMap(), equalTo(union(originalStoreContent, expectedSuccesses)));
    verify(this.cacheLoaderWriter, atLeast(1)).writeAll(getAnyEntryIterable());
    assertThat(fakeLoaderWriter.getEntryMap(), equalTo(union(originalWriterContent, expectedSuccesses)));
    verifyZeroInteractions(this.spiedResilienceStrategy);

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
    final Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    final FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);
    doThrow(new StoreAccessException("")).when(this.store)
        .bulkCompute(getAnyStringSet(), getAnyEntryIterableFunction());

    final Map<String, String> originalWriterContent = getEntryMap(KEY_SET_A, KEY_SET_B, KEY_SET_C);
    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent, KEY_SET_D);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);

    final InternalCache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    final Map<String, String> contentUpdates = getAltEntryMap("new_", fanIn(KEY_SET_A, KEY_SET_C, KEY_SET_D));
    final Set<String> expectedFailures = KEY_SET_D;
    final Map<String, String> expectedSuccesses = copyWithout(contentUpdates, expectedFailures);
    try {
      ehcache.putAll(contentUpdates);
      fail();
    } catch (BulkCacheWritingException e) {
      // Expected
    }

    final InOrder ordered = inOrder(this.store, this.cacheLoaderWriter, this.spiedResilienceStrategy);
    ordered.verify(this.store, atLeast(1))
        .bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), everyItem(isIn(contentUpdates.keySet())));
    // ResilienceStrategy invoked; no assertions about Store content
    ordered.verify(this.cacheLoaderWriter, atLeast(1)).writeAll(getAnyEntryIterable());
    assertThat(fakeLoaderWriter.getEntryMap(), equalTo(union(originalWriterContent, expectedSuccesses)));
    ordered.verify(this.spiedResilienceStrategy)
        .putAllFailure(eq(contentUpdates), any(StoreAccessException.class), this.bulkExceptionCaptor.capture());

    assertThat(this.bulkExceptionCaptor.getValue().getSuccesses(), Matchers.<Set<?>>equalTo(expectedSuccesses.keySet()));
    assertThat(this.bulkExceptionCaptor.getValue().getFailures().keySet(), Matchers.<Set<?>>equalTo(expectedFailures));

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
    final Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    final FakeStore fakeStore = new FakeStore(originalStoreContent, Collections.singleton("keyA3"));
    this.store = spy(fakeStore);

    final Map<String, String> originalWriterContent = getEntryMap(KEY_SET_A, KEY_SET_B, KEY_SET_C);
    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent, KEY_SET_D);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);

    final InternalCache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    final Map<String, String> contentUpdates = getAltEntryMap("new_", fanIn(KEY_SET_A, KEY_SET_C, KEY_SET_D));
    final Set<String> expectedFailures = KEY_SET_D;
    final Map<String, String> expectedSuccesses = copyWithout(contentUpdates, expectedFailures);
    try {
      ehcache.putAll(contentUpdates);
      fail();
    } catch (BulkCacheWritingException e) {
      // Expected
    }

    final InOrder ordered = inOrder(this.store, this.cacheLoaderWriter, this.spiedResilienceStrategy);
    ordered.verify(this.store, atLeast(1))
        .bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), everyItem(isIn(contentUpdates.keySet())));
    // ResilienceStrategy invoked; no assertions about Store content
    ordered.verify(this.cacheLoaderWriter, atLeast(1)).writeAll(getAnyEntryIterable());
    assertThat(fakeLoaderWriter.getEntryMap(), equalTo(union(originalWriterContent, expectedSuccesses)));
    ordered.verify(this.spiedResilienceStrategy)
        .putAllFailure(eq(contentUpdates), any(StoreAccessException.class), this.bulkExceptionCaptor.capture());

    assertThat(this.bulkExceptionCaptor.getValue().getSuccesses(), Matchers.<Set<?>>equalTo(expectedSuccesses.keySet()));
    assertThat(this.bulkExceptionCaptor.getValue().getFailures().keySet(), Matchers.<Set<?>>equalTo(expectedFailures));

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
    final Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    final FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);

    final Map<String, String> originalWriterContent = getEntryMap(KEY_SET_A, KEY_SET_B, KEY_SET_C);
    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent, KEY_SET_D);
    fakeLoaderWriter.setCompleteFailureKey("keyC4");
    this.cacheLoaderWriter = spy(fakeLoaderWriter);

    final InternalCache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    final Map<String, String> contentUpdates = getAltEntryMap("new_", fanIn(KEY_SET_A, KEY_SET_C, KEY_SET_D));
    final Set<String> expectedFailures = union(KEY_SET_D, Collections.singleton("keyC4"));
    final Map<String, String> expectedSuccesses = copyWithout(contentUpdates, expectedFailures);
    try {
      ehcache.putAll(contentUpdates);
      fail();
    } catch (BulkCacheWritingException e) {
      // Expected
      assertThat(e.getSuccesses(), Matchers.<Set<?>>equalTo(expectedSuccesses.keySet()));
      assertThat(e.getFailures().keySet(), Matchers.<Set<?>>equalTo(expectedFailures));
    }

    verify(this.store, atLeast(1)).bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), everyItem(isIn(contentUpdates.keySet())));
    assertThat(fakeStore.getEntryMap(), equalTo(union(originalStoreContent, expectedSuccesses)));
    verify(this.cacheLoaderWriter, atLeast(1)).writeAll(getAnyEntryIterable());
    assertThat(fakeLoaderWriter.getEntryMap(), equalTo(union(originalWriterContent, expectedSuccesses)));
    verifyZeroInteractions(this.spiedResilienceStrategy);

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
    final Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    final FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);
    doThrow(new StoreAccessException("")).when(this.store)
        .bulkCompute(getAnyStringSet(), getAnyEntryIterableFunction());

    final Map<String, String> originalWriterContent = getEntryMap(KEY_SET_A, KEY_SET_B, KEY_SET_C);
    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent, KEY_SET_D);
    fakeLoaderWriter.setCompleteFailureKey("keyC4");
    this.cacheLoaderWriter = spy(fakeLoaderWriter);

    final InternalCache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    final Map<String, String> contentUpdates = getAltEntryMap("new_", fanIn(KEY_SET_A, KEY_SET_C, KEY_SET_D));
    final Set<String> expectedFailures = union(KEY_SET_D, Collections.singleton("keyC4"));
    final Map<String, String> expectedSuccesses = copyWithout(contentUpdates, expectedFailures);
    try {
      ehcache.putAll(contentUpdates);
      fail();
    } catch (BulkCacheWritingException e) {
      // Expected
    }

    final InOrder ordered = inOrder(this.store, this.cacheLoaderWriter, this.spiedResilienceStrategy);
    ordered.verify(this.store, atLeast(1)).bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), everyItem(isIn(contentUpdates.keySet())));
    // ResilienceStrategy invoked; no assertions about Store content
    ordered.verify(this.cacheLoaderWriter, atLeast(1)).writeAll(getAnyEntryIterable());
    ordered.verify(this.spiedResilienceStrategy)
        .putAllFailure(eq(contentUpdates), any(StoreAccessException.class), this.bulkExceptionCaptor.capture());

    @SuppressWarnings("unchecked")
    final Set<String> bcweSuccesses = (Set<String>)this.bulkExceptionCaptor.getValue().getSuccesses();
    @SuppressWarnings("unchecked")
    final Map<String, Exception> bcweFailures = (Map<String, Exception>)this.bulkExceptionCaptor.getValue().getFailures();

    assertThat(union(bcweSuccesses, bcweFailures.keySet()), equalTo(contentUpdates.keySet()));
    assertThat(Collections.disjoint(bcweSuccesses, bcweFailures.keySet()), is(true));
    assertThatAllStoreEntriesWithoutFailuresMatchWriterState(fakeStore, fakeLoaderWriter, bcweFailures);


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
    final Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    final FakeStore fakeStore = new FakeStore(originalStoreContent, Collections.singleton("keyA3"));
    this.store = spy(fakeStore);

    final Map<String, String> originalWriterContent = getEntryMap(KEY_SET_A, KEY_SET_B, KEY_SET_C);
    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent, KEY_SET_D);
    fakeLoaderWriter.setCompleteFailureKey("keyC4");
    this.cacheLoaderWriter = spy(fakeLoaderWriter);

    final InternalCache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    final Map<String, String> contentUpdates = getAltEntryMap("new_", fanIn(KEY_SET_A, KEY_SET_C, KEY_SET_D));
    final Set<String> expectedFailures = union(KEY_SET_D, Collections.singleton("keyC4"));
    final Map<String, String> expectedSuccesses = copyWithout(contentUpdates, expectedFailures);
    try {
      ehcache.putAll(contentUpdates);
      fail();
    } catch (BulkCacheWritingException e) {
      // Expected
    }

    final InOrder ordered = inOrder(this.store, this.cacheLoaderWriter, this.spiedResilienceStrategy);
    ordered.verify(this.store, atLeast(1)).bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), everyItem(isIn(contentUpdates.keySet())));
    // ResilienceStrategy invoked; no assertions about Store content
    ordered.verify(this.cacheLoaderWriter, atLeast(1)).writeAll(getAnyEntryIterable());
    ordered.verify(this.spiedResilienceStrategy)
        .putAllFailure(eq(contentUpdates), any(StoreAccessException.class), this.bulkExceptionCaptor.capture());

    @SuppressWarnings("unchecked")
    final Set<String> bcweSuccesses = (Set<String>)this.bulkExceptionCaptor.getValue().getSuccesses();
    @SuppressWarnings("unchecked")
    final Map<String, Exception> bcweFailures = (Map<String, Exception>)this.bulkExceptionCaptor.getValue().getFailures();

    assertThat(union(bcweSuccesses, bcweFailures.keySet()), equalTo(contentUpdates.keySet()));
    assertThat(Collections.disjoint(bcweSuccesses, bcweFailures.keySet()), is(true));
    assertThatAllStoreEntriesWithoutFailuresMatchWriterState(fakeStore, fakeLoaderWriter, bcweFailures);

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
    final Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    final FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);

    final Map<String, String> originalWriterContent = getEntryMap(KEY_SET_A, KEY_SET_B, KEY_SET_C);
    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);
    doThrow(new Exception("writeAll failed")).when(this.cacheLoaderWriter).writeAll(getAnyEntryIterable());

    final InternalCache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    final Map<String, String> contentUpdates = getAltEntryMap("new_", fanIn(KEY_SET_A, KEY_SET_C, KEY_SET_D));
    try {
      ehcache.putAll(contentUpdates);
      fail();
    } catch (BulkCacheWritingException e) {
      // Expected
      assertThat(e.getSuccesses(), empty());
      assertThat(e.getFailures().keySet(), Matchers.<Set<?>>equalTo(contentUpdates.keySet()));
    }

    verify(this.store, atLeast(1)).bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), everyItem(isIn(contentUpdates.keySet())));
    assertThat(fakeStore.getEntryMap(), equalTo(getEntryMap(KEY_SET_B)));
    verify(this.cacheLoaderWriter, atLeast(1)).writeAll(getAnyEntryIterable());
    verifyZeroInteractions(this.spiedResilienceStrategy);

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
    final Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    final FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);
    doThrow(new StoreAccessException("")).when(this.store)
        .bulkCompute(getAnyStringSet(), getAnyEntryIterableFunction());

    final Map<String, String> originalWriterContent = getEntryMap(KEY_SET_A, KEY_SET_B, KEY_SET_C);
    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);
    doThrow(new Exception("writeAll failed")).when(this.cacheLoaderWriter).writeAll(getAnyEntryIterable());

    final InternalCache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    final Map<String, String> contentUpdates = getAltEntryMap("new_", fanIn(KEY_SET_A, KEY_SET_C, KEY_SET_D));
    try {
      ehcache.putAll(contentUpdates);
      fail();
    } catch (BulkCacheWritingException e) {
      // Expected
    }

    final InOrder ordered = inOrder(this.store, this.cacheLoaderWriter, this.spiedResilienceStrategy);
    ordered.verify(this.store, atLeast(1))
        .bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), everyItem(isIn(contentUpdates.keySet())));
    // ResilienceStrategy invoked; no assertions about Store content
    ordered.verify(this.cacheLoaderWriter, atLeast(1)).writeAll(getAnyEntryIterable());
    ordered.verify(this.spiedResilienceStrategy)
        .putAllFailure(eq(contentUpdates), any(StoreAccessException.class), this.bulkExceptionCaptor.capture());

    assertThat(this.bulkExceptionCaptor.getValue().getSuccesses(), empty());
    assertThat(this.bulkExceptionCaptor.getValue().getFailures().keySet(),
        Matchers.<Set<?>>equalTo(contentUpdates.keySet()));

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
    final Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    final FakeStore fakeStore = new FakeStore(originalStoreContent, Collections.singleton("keyA3"));
    this.store = spy(fakeStore);

    final Map<String, String> originalWriterContent = getEntryMap(KEY_SET_A, KEY_SET_B, KEY_SET_C);
    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);
    doThrow(new Exception("writeAll failed")).when(this.cacheLoaderWriter).writeAll(getAnyEntryIterable());

    final InternalCache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    final Map<String, String> contentUpdates = getAltEntryMap("new_", fanIn(KEY_SET_A, KEY_SET_C, KEY_SET_D));
    try {
      ehcache.putAll(contentUpdates);
      fail();
    } catch (BulkCacheWritingException e) {
      // Expected
    }

    final InOrder ordered = inOrder(this.store, this.cacheLoaderWriter, this.spiedResilienceStrategy);
    ordered.verify(this.store, atLeast(1))
        .bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), everyItem(isIn(contentUpdates.keySet())));
    // ResilienceStrategy invoked; no assertions about Store content
    ordered.verify(this.cacheLoaderWriter, atLeast(1)).writeAll(getAnyEntryIterable());
    ordered.verify(this.spiedResilienceStrategy)
        .putAllFailure(eq(contentUpdates), any(StoreAccessException.class), this.bulkExceptionCaptor.capture());

    assertThat(this.bulkExceptionCaptor.getValue().getSuccesses(), empty());
    assertThat(this.bulkExceptionCaptor.getValue().getFailures().keySet(),
        Matchers.<Set<?>>equalTo(contentUpdates.keySet()));

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
    final Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    final FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);

    final Map<String, String> originalWriterContent = getEntryMap(KEY_SET_A, KEY_SET_B, KEY_SET_C);
    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);

    final InternalCache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    final Map<String, String> contentUpdates = getAltEntryMap("new_", fanIn(KEY_SET_B, KEY_SET_C));
    ehcache.putAll(contentUpdates);

    verify(this.store, atLeast(1)).bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), equalTo(contentUpdates.keySet()));
    assertThat(fakeStore.getEntryMap(), equalTo(union(originalStoreContent, contentUpdates)));
    verify(this.cacheLoaderWriter, atLeast(1)).writeAll(getAnyEntryIterable());
    assertThat(fakeLoaderWriter.getEntryMap(), equalTo(union(originalWriterContent, contentUpdates)));
    verifyZeroInteractions(this.spiedResilienceStrategy);

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
    final Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    final FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);
    doThrow(new StoreAccessException("")).when(this.store)
        .bulkCompute(getAnyStringSet(), getAnyEntryIterableFunction());

    final Map<String, String> originalWriterContent = getEntryMap(KEY_SET_A, KEY_SET_B, KEY_SET_C);
    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);

    final InternalCache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    final Map<String, String> contentUpdates = getAltEntryMap("new_", fanIn(KEY_SET_B, KEY_SET_C));
    ehcache.putAll(contentUpdates);

    final InOrder ordered = inOrder(this.store, this.cacheLoaderWriter, this.spiedResilienceStrategy);
    ordered.verify(this.store, atLeast(1))
        .bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), everyItem(isIn(contentUpdates.keySet())));
    // ResilienceStrategy invoked; no assertions about Store content
    ordered.verify(this.cacheLoaderWriter, atLeast(1)).writeAll(getAnyEntryIterable());
    assertThat(fakeLoaderWriter.getEntryMap(), equalTo(union(originalWriterContent, contentUpdates)));
    ordered.verify(this.spiedResilienceStrategy)
        .putAllFailure(eq(contentUpdates), any(StoreAccessException.class));

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
    final Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    final FakeStore fakeStore = new FakeStore(originalStoreContent, Collections.singleton("keyB3"));
    this.store = spy(fakeStore);

    final Map<String, String> originalWriterContent = getEntryMap(KEY_SET_A, KEY_SET_B, KEY_SET_C);
    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);

    final InternalCache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    final Map<String, String> contentUpdates = getAltEntryMap("new_", fanIn(KEY_SET_B, KEY_SET_C));
    ehcache.putAll(contentUpdates);

    final InOrder ordered = inOrder(this.store, this.cacheLoaderWriter, this.spiedResilienceStrategy);
    ordered.verify(this.store, atLeast(1))
        .bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), everyItem(isIn(contentUpdates.keySet())));
    // ResilienceStrategy invoked; no assertions about Store content
    ordered.verify(this.cacheLoaderWriter, atLeast(1)).writeAll(getAnyEntryIterable());
    assertThat(fakeLoaderWriter.getEntryMap(), equalTo(union(originalWriterContent, contentUpdates)));
    ordered.verify(this.spiedResilienceStrategy)
        .putAllFailure(eq(contentUpdates), any(StoreAccessException.class));

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
    final Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    final FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);

    final Map<String, String> originalWriterContent = getEntryMap(KEY_SET_A, KEY_SET_B, KEY_SET_C, KEY_SET_D);
    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent, KEY_SET_D);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);

    final InternalCache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    final Map<String, String> contentUpdates = getAltEntryMap("new_", fanIn(KEY_SET_B, KEY_SET_C, KEY_SET_D));
    final Set<String> expectedFailures = KEY_SET_D;
    final Map<String, String> expectedSuccesses = copyWithout(contentUpdates, expectedFailures);
    try {
      ehcache.putAll(contentUpdates);
      fail();
    } catch (BulkCacheWritingException e) {
      // Expected
      assertThat(e.getSuccesses(), Matchers.<Set<?>>equalTo(expectedSuccesses.keySet()));
      assertThat(e.getFailures().keySet(), Matchers.<Set<?>>equalTo(expectedFailures));
    }

    verify(this.store, atLeast(1)).bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), everyItem(isIn(contentUpdates.keySet())));
    assertThat(fakeStore.getEntryMap(), equalTo(union(originalStoreContent, expectedSuccesses)));
    verify(this.cacheLoaderWriter, atLeast(1)).writeAll(getAnyEntryIterable());
    assertThat(fakeLoaderWriter.getEntryMap(), equalTo(union(originalWriterContent, expectedSuccesses)));
    verifyZeroInteractions(this.spiedResilienceStrategy);

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
    final Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    final FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);
    doThrow(new StoreAccessException("")).when(this.store)
        .bulkCompute(getAnyStringSet(), getAnyEntryIterableFunction());

    final Map<String, String> originalWriterContent = getEntryMap(KEY_SET_A, KEY_SET_B, KEY_SET_C, KEY_SET_D);
    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent, KEY_SET_D);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);

    final InternalCache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    final Map<String, String> contentUpdates = getAltEntryMap("new_", fanIn(KEY_SET_B, KEY_SET_C, KEY_SET_D));
    final Set<String> expectedFailures = KEY_SET_D;
    final Map<String, String> expectedSuccesses = copyWithout(contentUpdates, expectedFailures);
    try {
      ehcache.putAll(contentUpdates);
      fail();
    } catch (BulkCacheWritingException e) {
      // Expected
    }

    final InOrder ordered = inOrder(this.store, this.cacheLoaderWriter, this.spiedResilienceStrategy);
    ordered.verify(this.store, atLeast(1))
        .bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), everyItem(isIn(contentUpdates.keySet())));
    // ResilienceStrategy invoked; no assertions about Store content
    ordered.verify(this.cacheLoaderWriter, atLeast(1)).writeAll(getAnyEntryIterable());
    assertThat(fakeLoaderWriter.getEntryMap(), equalTo(union(originalWriterContent, expectedSuccesses)));
    ordered.verify(this.spiedResilienceStrategy)
        .putAllFailure(eq(contentUpdates), any(StoreAccessException.class), this.bulkExceptionCaptor.capture());

    assertThat(this.bulkExceptionCaptor.getValue().getSuccesses(), Matchers.<Set<?>>equalTo(expectedSuccesses.keySet()));
    assertThat(this.bulkExceptionCaptor.getValue().getFailures().keySet(), Matchers.<Set<?>>equalTo(expectedFailures));

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
    final Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    final FakeStore fakeStore = new FakeStore(originalStoreContent, Collections.singleton("keyB3"));
    this.store = spy(fakeStore);

    final Map<String, String> originalWriterContent = getEntryMap(KEY_SET_A, KEY_SET_B, KEY_SET_C, KEY_SET_D);
    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent, KEY_SET_D);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);

    final InternalCache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    final Map<String, String> contentUpdates = getAltEntryMap("new_", fanIn(KEY_SET_B, KEY_SET_C, KEY_SET_D));
    final Set<String> expectedFailures = KEY_SET_D;
    final Map<String, String> expectedSuccesses = copyWithout(contentUpdates, expectedFailures);
    try {
      ehcache.putAll(contentUpdates);
      fail();
    } catch (BulkCacheWritingException e) {
      // Expected
    }

    final InOrder ordered = inOrder(this.store, this.cacheLoaderWriter, this.spiedResilienceStrategy);
    ordered.verify(this.store, atLeast(1))
        .bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), everyItem(isIn(contentUpdates.keySet())));
    // ResilienceStrategy invoked; no assertions about Store content
    ordered.verify(this.cacheLoaderWriter, atLeast(1)).writeAll(getAnyEntryIterable());
    assertThat(fakeLoaderWriter.getEntryMap(), equalTo(union(originalWriterContent, expectedSuccesses)));
    ordered.verify(this.spiedResilienceStrategy)
        .putAllFailure(eq(contentUpdates), any(StoreAccessException.class), this.bulkExceptionCaptor.capture());

    assertThat(this.bulkExceptionCaptor.getValue().getSuccesses(), Matchers.<Set<?>>equalTo(expectedSuccesses.keySet()));
    assertThat(this.bulkExceptionCaptor.getValue().getFailures().keySet(), Matchers.<Set<?>>equalTo(expectedFailures));

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
    final Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    final FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);

    final Map<String, String> originalWriterContent = getEntryMap(KEY_SET_A, KEY_SET_B, KEY_SET_C, KEY_SET_D);
    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent, KEY_SET_D);
    fakeLoaderWriter.setCompleteFailureKey("keyC4");
    this.cacheLoaderWriter = spy(fakeLoaderWriter);

    final InternalCache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    final Map<String, String> contentUpdates = getAltEntryMap("new_", fanIn(KEY_SET_B, KEY_SET_C, KEY_SET_D));
    final Set<String> expectedFailures = union(KEY_SET_D, Collections.singleton("keyC4"));
    final Map<String, String> expectedSuccesses = copyWithout(contentUpdates, expectedFailures);
    try {
      ehcache.putAll(contentUpdates);
      fail();
    } catch (BulkCacheWritingException e) {
      // Expected
      assertThat(e.getSuccesses(), Matchers.<Set<?>>equalTo(expectedSuccesses.keySet()));
      assertThat(e.getFailures().keySet(), Matchers.<Set<?>>equalTo(expectedFailures));
    }

    verify(this.store, atLeast(1))
        .bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), everyItem(isIn(contentUpdates.keySet())));
    assertThat(fakeStore.getEntryMap(), equalTo(union(originalStoreContent, expectedSuccesses)));
    verify(this.cacheLoaderWriter, atLeast(1)).writeAll(getAnyEntryIterable());
    assertThat(fakeLoaderWriter.getEntryMap(), equalTo(union(originalWriterContent, expectedSuccesses)));
    verifyZeroInteractions(this.spiedResilienceStrategy);

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
    final Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    final FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);
    doThrow(new StoreAccessException("")).when(this.store)
        .bulkCompute(getAnyStringSet(), getAnyEntryIterableFunction());

    final Map<String, String> originalWriterContent = getEntryMap(KEY_SET_A, KEY_SET_B, KEY_SET_C, KEY_SET_D);
    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent, KEY_SET_D);
    fakeLoaderWriter.setCompleteFailureKey("keyC4");
    this.cacheLoaderWriter = spy(fakeLoaderWriter);

    final InternalCache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    final Map<String, String> contentUpdates = getAltEntryMap("new_", fanIn(KEY_SET_B, KEY_SET_C, KEY_SET_D));
    final Set<String> expectedFailures = union(KEY_SET_D, Collections.singleton("keyC4"));
    final Map<String, String> expectedSuccesses = copyWithout(contentUpdates, expectedFailures);
    try {
      ehcache.putAll(contentUpdates);
      fail();
    } catch (BulkCacheWritingException e) {
      // Expected
    }

    final InOrder ordered = inOrder(this.store, this.cacheLoaderWriter, this.spiedResilienceStrategy);
    ordered.verify(this.store, atLeast(1))
        .bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), everyItem(isIn(contentUpdates.keySet())));
    // ResilienceStrategy invoked; no assertions about Store content
    ordered.verify(this.cacheLoaderWriter, atLeast(1)).writeAll(getAnyEntryIterable());
    ordered.verify(this.spiedResilienceStrategy)
        .putAllFailure(eq(contentUpdates), any(StoreAccessException.class), this.bulkExceptionCaptor.capture());

    @SuppressWarnings("unchecked")
    final Set<String> bcweSuccesses = (Set<String>)this.bulkExceptionCaptor.getValue().getSuccesses();
    @SuppressWarnings("unchecked")
    final Map<String, Exception> bcweFailures = (Map<String, Exception>)this.bulkExceptionCaptor.getValue().getFailures();

    assertThat(union(bcweSuccesses, bcweFailures.keySet()), equalTo(contentUpdates.keySet()));
    assertThat(Collections.disjoint(bcweSuccesses, bcweFailures.keySet()), is(true));
    assertThatAllStoreEntriesWithoutFailuresMatchWriterState(fakeStore, fakeLoaderWriter, bcweFailures);

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
    final Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    final FakeStore fakeStore = new FakeStore(originalStoreContent, Collections.singleton("keyB3"));
    this.store = spy(fakeStore);

    final Map<String, String> originalWriterContent = getEntryMap(KEY_SET_A, KEY_SET_B, KEY_SET_C, KEY_SET_D);
    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent, KEY_SET_D);
    fakeLoaderWriter.setCompleteFailureKey("keyC4");
    this.cacheLoaderWriter = spy(fakeLoaderWriter);

    final InternalCache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    final Map<String, String> contentUpdates = getAltEntryMap("new_", fanIn(KEY_SET_B, KEY_SET_C, KEY_SET_D));
    final Set<String> expectedFailures = union(KEY_SET_D, Collections.singleton("keyC4"));
    final Map<String, String> expectedSuccesses = copyWithout(contentUpdates, expectedFailures);
    try {
      ehcache.putAll(contentUpdates);
      fail();
    } catch (BulkCacheWritingException e) {
      // Expected
    }

    final InOrder ordered = inOrder(this.store, this.cacheLoaderWriter, this.spiedResilienceStrategy);
    ordered.verify(this.store, atLeast(1))
        .bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), everyItem(isIn(contentUpdates.keySet())));
    // ResilienceStrategy invoked; no assertions about Store content
    ordered.verify(this.cacheLoaderWriter, atLeast(1)).writeAll(getAnyEntryIterable());
    ordered.verify(this.spiedResilienceStrategy)
        .putAllFailure(eq(contentUpdates), any(StoreAccessException.class), this.bulkExceptionCaptor.capture());

    @SuppressWarnings("unchecked")
    final Set<String> bcweSuccesses = (Set<String>)this.bulkExceptionCaptor.getValue().getSuccesses();
    @SuppressWarnings("unchecked")
    final Map<String, Exception> bcweFailures = (Map<String, Exception>)this.bulkExceptionCaptor.getValue().getFailures();

    assertThat(union(bcweSuccesses, bcweFailures.keySet()), equalTo(contentUpdates.keySet()));
    assertThat(Collections.disjoint(bcweSuccesses, bcweFailures.keySet()), is(true));
    assertThatAllStoreEntriesWithoutFailuresMatchWriterState(fakeStore, fakeLoaderWriter, bcweFailures);

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
    final Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    final FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);

    final Map<String, String> originalWriterContent = getEntryMap(KEY_SET_A, KEY_SET_B, KEY_SET_C, KEY_SET_D);
    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);
    doThrow(new Exception("writeAll failed")).when(this.cacheLoaderWriter).writeAll(getAnyEntryIterable());

    final InternalCache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    final Map<String, String> contentUpdates = getAltEntryMap("new_", fanIn(KEY_SET_B, KEY_SET_C, KEY_SET_D));
    try {
      ehcache.putAll(contentUpdates);
      fail();
    } catch (BulkCacheWritingException e) {
      // Expected
      assertThat(e.getSuccesses(), empty());
      assertThat(e.getFailures().keySet(), Matchers.<Set<?>>equalTo(contentUpdates.keySet()));
    }

    verify(this.store, atLeast(1))
        .bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), everyItem(isIn(contentUpdates.keySet())));
    assertThat(fakeStore.getEntryMap(), equalTo(getEntryMap(KEY_SET_A)));
    verify(this.cacheLoaderWriter, atLeast(1)).writeAll(getAnyEntryIterable());
    verifyZeroInteractions(this.spiedResilienceStrategy);

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
    final Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    final FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);
    doThrow(new StoreAccessException("")).when(this.store)
        .bulkCompute(getAnyStringSet(), getAnyEntryIterableFunction());

    final Map<String, String> originalWriterContent = getEntryMap(KEY_SET_A, KEY_SET_B, KEY_SET_C, KEY_SET_D);
    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);
    doThrow(new Exception("writeAll failed")).when(this.cacheLoaderWriter).writeAll(getAnyEntryIterable());

    final InternalCache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    final Map<String, String> contentUpdates = getAltEntryMap("new_", fanIn(KEY_SET_B, KEY_SET_C, KEY_SET_D));
    try {
      ehcache.putAll(contentUpdates);
      fail();
    } catch (BulkCacheWritingException e) {
      // Expected
    }

    final InOrder ordered = inOrder(this.store, this.cacheLoaderWriter, this.spiedResilienceStrategy);
    ordered.verify(this.store, atLeast(1))
        .bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), everyItem(isIn(contentUpdates.keySet())));
    // ResilienceStrategy invoked; no assertions about Store content
    ordered.verify(this.cacheLoaderWriter, atLeast(1)).writeAll(getAnyEntryIterable());
    assertThat(fakeLoaderWriter.getEntryMap(), equalTo(originalWriterContent));
    ordered.verify(this.spiedResilienceStrategy)
        .putAllFailure(eq(contentUpdates), any(StoreAccessException.class), this.bulkExceptionCaptor.capture());

    assertThat(this.bulkExceptionCaptor.getValue().getSuccesses(), empty());
    assertThat(this.bulkExceptionCaptor.getValue().getFailures().keySet(),
        Matchers.<Set<?>>equalTo(contentUpdates.keySet()));

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
    final Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    final FakeStore fakeStore = new FakeStore(originalStoreContent, Collections.singleton("keyB3"));
    this.store = spy(fakeStore);

    final Map<String, String> originalWriterContent = getEntryMap(KEY_SET_A, KEY_SET_B, KEY_SET_C, KEY_SET_D);
    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);
    doThrow(new Exception("writeAll failed")).when(this.cacheLoaderWriter).writeAll(getAnyEntryIterable());

    final InternalCache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    final Map<String, String> contentUpdates = getAltEntryMap("new_", fanIn(KEY_SET_B, KEY_SET_C, KEY_SET_D));
    try {
      ehcache.putAll(contentUpdates);
      fail();
    } catch (BulkCacheWritingException e) {
      // Expected
    }

    final InOrder ordered = inOrder(this.store, this.cacheLoaderWriter, this.spiedResilienceStrategy);
    ordered.verify(this.store, atLeast(1))
        .bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), everyItem(isIn(contentUpdates.keySet())));
    // ResilienceStrategy invoked; no assertions about Store content
    ordered.verify(this.cacheLoaderWriter, atLeast(1)).writeAll(getAnyEntryIterable());
    assertThat(fakeLoaderWriter.getEntryMap(), equalTo(originalWriterContent));
    ordered.verify(this.spiedResilienceStrategy)
        .putAllFailure(eq(contentUpdates), any(StoreAccessException.class), this.bulkExceptionCaptor.capture());

    assertThat(this.bulkExceptionCaptor.getValue().getSuccesses(), empty());
    assertThat(this.bulkExceptionCaptor.getValue().getFailures().keySet(),
        Matchers.<Set<?>>equalTo(contentUpdates.keySet()));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.PutOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.PutAllOutcome.FAILURE));
    assertThat(ehcache.getBulkMethodEntries().get(BulkOps.PUT_ALL).intValue(), is(0));
  }

  @Test
  public void testPutAllPartialIntersectionsImmediatelyExpiredCreatedEntries() throws Exception {
    final Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    final FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);

    final Map<String, String> originalWriterContent = getEntryMap(KEY_SET_A, KEY_SET_B, KEY_SET_C);
    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);

    @SuppressWarnings("unchecked")
    final Expiry<String, String> expiry = mock(Expiry.class);
    when(expiry.getExpiryForCreation(any(String.class), any(String.class))).thenReturn(Duration.ZERO);

    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(cacheLoaderWriter, expiry);

    final Map<String, String> contentUpdates = getAltEntryMap("new_", fanIn(KEY_SET_A, KEY_SET_C, KEY_SET_D));
    ehcache.putAll(contentUpdates);

    verify(this.store, atLeast(1)).bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), equalTo(contentUpdates.keySet()));
    assertThat(fakeStore.getEntryMap(), equalTo(union(getAltEntryMap("new_", KEY_SET_A), getEntryMap(KEY_SET_B))));
    verify(this.cacheLoaderWriter, atLeast(1)).writeAll(getAnyEntryIterable());
    assertThat(fakeLoaderWriter.getEntryMap(), equalTo(union(originalWriterContent, contentUpdates)));
    verifyZeroInteractions(this.spiedResilienceStrategy);

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.PutOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.PutAllOutcome.SUCCESS));
    assertThat(ehcache.getBulkMethodEntries().get(BulkOps.PUT_ALL).intValue(), is(KEY_SET_A.size()));
  }

  @Test
  public void testPutAllPartialIntersectionsImmediatelyExpiredUpdatedEntries() throws Exception {
    final Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    final FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);

    final Map<String, String> originalWriterContent = getEntryMap(KEY_SET_A, KEY_SET_B, KEY_SET_C);
    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);

    @SuppressWarnings("unchecked")
    final Expiry<String, String> expiry = mock(Expiry.class);
    when(expiry.getExpiryForUpdate(any(String.class), argThat(org.ehcache.core.util.Matchers.<String>holding(instanceOf(String.class))), any(String.class))).thenReturn(Duration.ZERO);

    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(cacheLoaderWriter, expiry);

    final Map<String, String> contentUpdates = getAltEntryMap("new_", fanIn(KEY_SET_A, KEY_SET_C, KEY_SET_D));
    ehcache.putAll(contentUpdates);

    verify(this.store, atLeast(1)).bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), equalTo(contentUpdates.keySet()));
    assertThat(fakeStore.getEntryMap(), equalTo(union(getEntryMap(KEY_SET_B), getAltEntryMap("new_", union(KEY_SET_C, KEY_SET_D)))));
    verify(this.cacheLoaderWriter, atLeast(1)).writeAll(getAnyEntryIterable());
    assertThat(fakeLoaderWriter.getEntryMap(), equalTo(union(originalWriterContent, contentUpdates)));
    verifyZeroInteractions(this.spiedResilienceStrategy);

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.PutOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.PutAllOutcome.SUCCESS));
    assertThat(ehcache.getBulkMethodEntries().get(BulkOps.PUT_ALL).intValue(), is(KEY_SET_C.size() + KEY_SET_D.size()));
  }

  private void assertThatAllStoreEntriesWithoutFailuresMatchWriterState(FakeStore fakeStore, FakeCacheLoaderWriter fakeLoaderWriter, Map<String, Exception> bcweFailures) {
    assertThat(copyWithout(fakeStore.getEntryMap(), bcweFailures.keySet()).entrySet(), everyItem(isIn(fakeLoaderWriter.getEntryMap()
        .entrySet())));
  }

  private EhcacheWithLoaderWriter<String, String> getEhcache(final CacheLoaderWriter<String, String> cacheLoaderWriter) {
    return getEhcache(cacheLoaderWriter, CACHE_CONFIGURATION);
  }

  private EhcacheWithLoaderWriter<String, String> getEhcache(final CacheLoaderWriter<String, String> cacheLoaderWriter, Expiry<String, String> expiry) {
    CacheConfiguration<String, String> config = new BaseCacheConfiguration<String, String>(String.class, String.class, null, null,
        expiry, ResourcePoolsHelper.createHeapOnlyPools());
    return getEhcache(cacheLoaderWriter, config);
  }

  private EhcacheWithLoaderWriter<String, String> getEhcache(CacheLoaderWriter<String, String> cacheLoaderWriter, CacheConfiguration<String, String> config) {
    final EhcacheWithLoaderWriter<String, String> ehcache = new EhcacheWithLoaderWriter<String, String>(config, this.store, cacheLoaderWriter, cacheEventDispatcher, LoggerFactory.getLogger(EhcacheWithLoaderWriter.class + "-" + "EhcacheBasicPutAllTest"));
    ehcache.init();
    assertThat("cache not initialized", ehcache.getStatus(), Matchers.is(Status.AVAILABLE));
    this.spiedResilienceStrategy = this.setResilienceStrategySpy(ehcache);
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
    final Set<String> bulkComputeArgs = new LinkedHashSet<String>();
    for (final Set<String> set : this.bulkComputeSetCaptor.getAllValues()) {
      bulkComputeArgs.addAll(set);
    }
    return bulkComputeArgs;
  }

}
