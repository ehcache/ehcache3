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
import org.ehcache.core.spi.store.Store;
import org.ehcache.core.statistics.CacheOperationOutcomes;
import org.ehcache.spi.loaderwriter.BulkCacheLoadingException;
import org.ehcache.core.spi.store.StoreAccessException;
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
import static org.ehcache.core.EhcacheBasicBulkUtil.KEY_SET_E;
import static org.ehcache.core.EhcacheBasicBulkUtil.KEY_SET_F;
import static org.ehcache.core.EhcacheBasicBulkUtil.TEST_ENTRIES;
import static org.ehcache.core.EhcacheBasicBulkUtil.copyOnly;
import static org.ehcache.core.EhcacheBasicBulkUtil.copyUntil;
import static org.ehcache.core.EhcacheBasicBulkUtil.copyWithout;
import static org.ehcache.core.EhcacheBasicBulkUtil.fanIn;
import static org.ehcache.core.EhcacheBasicBulkUtil.getEntryMap;
import static org.ehcache.core.EhcacheBasicBulkUtil.getNullEntryMap;
import static org.ehcache.core.EhcacheBasicBulkUtil.union;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
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
import static org.ehcache.core.EhcacheBasicGetAllTest.getAnyStringSet;
import static org.ehcache.core.EhcacheBasicGetAllTest.getAnyIterableFunction;
import static org.ehcache.core.EhcacheBasicGetAllTest.validateBulkCounters;

/**
 * Provides testing of basic GET_ALL operations on an {@code EhcacheWithLoaderWriter}.
 * <h3>Note</h3>
 * The current implementation of {@link EhcacheWithLoaderWriter#getAll(java.util.Set) Ehcache.getAll}
 * does <b>not</b> produce partial results while handling a
 * {@link StoreAccessException StoreAccessException}; all keys presented
 * to {@code getAll} succeed or fail based on the recovery call to
 * {@link CacheLoaderWriter#loadAll(Iterable)}.
 *
 * @author Clifford W. Johnson
 */
public class EhcacheWithLoaderWriterBasicGetAllTest extends EhcacheBasicCrudBase {

  @Mock
  private CacheLoaderWriter<String, String> loaderWriter;

  /**
   * A Mockito {@code ArgumentCaptor} for the {@code Set} argument to the
   * {@link CacheLoaderWriter#loadAll(Iterable)}
   * method.
   */
  @Captor
  private ArgumentCaptor<Set<String>> loadAllCaptor;

  /**
   * A Mockito {@code ArgumentCaptor} for the {@code Set} argument to the
   * {@link org.ehcache.core.internal.resilience.ResilienceStrategy#getAllFailure(Iterable, Map, StoreAccessException)
   *    ResilienceStrategy.getAllFailure(Iterable, Map, StoreAccessException)} method.
   */
  @Captor
  private ArgumentCaptor<Map<String, String>> getAllFailureMapCaptor;

  /**
   * A Mockito {@code ArgumentCaptor} for the
   * {@link BulkCacheLoadingException BulkCacheLoadingException}
   * provided to the
   * {@link org.ehcache.core.internal.resilience.ResilienceStrategy#getAllFailure(Iterable, StoreAccessException, BulkCacheLoadingException)
   *    ResilienceStrategy.getAllFailure(Iterable, StoreAccessException, BulkCacheLoadingException)} method.
   */
  @Captor
  private ArgumentCaptor<BulkCacheLoadingException> bulkExceptionCaptor;

  /**
   * Tests {@link EhcacheWithLoaderWriter#getAll(Set)} for
   * <ul>
   *    <li>empty request key set</li>
   *    <li>with a {@code CacheLoaderWriter} (loader-provided entries not relevant)</li>
   * </ul>
   */
  @Test
  public void testGetAllEmptyRequestWithLoader() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.<String, String>emptyMap());
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(TEST_ENTRIES);
    this.loaderWriter = spy(fakeLoaderWriter);

    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.loaderWriter);

    final Map<String, String> actual = ehcache.getAll(Collections.<String>emptySet());
    final Map<String, String> expected = Collections.emptyMap();
    assertThat(actual, equalTo(expected));

    verify(this.store, never()).bulkComputeIfAbsent(eq(Collections.<String>emptySet()), getAnyIterableFunction());
    verify(this.spiedResilienceStrategy, never()).getAllFailure(eq(Collections.<String>emptySet()), any(StoreAccessException.class));
    verify(this.loaderWriter, never()).loadAll(eq(Collections.<String>emptySet()));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.GetOutcome.class));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoadingOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetAllOutcome.SUCCESS));
    validateBulkCounters(ehcache, 0, 0);
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#getAll(Set)} for
   * <ul>
   *    <li>non-empty request key set</li>
   *    <li>no {@link Store} entries match</li>
   *    <li>with a {@code CacheLoaderWriter} (loader-provided entries not relevant)</li>
   *    <li>all {@link CacheLoaderWriter#loadAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testGetAllStoreNoMatchLoaderAllFail() throws Exception {
    final FakeStore fakeStore = new FakeStore(getEntryMap(KEY_SET_B));
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeLoader = new FakeCacheLoaderWriter(TEST_ENTRIES);
    this.loaderWriter = spy(fakeLoader);
    doThrow(new Exception("loadAll failed")).when(this.loaderWriter).loadAll(getAnyStringSet());

    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.loaderWriter);

    try {
      ehcache.getAll(KEY_SET_A);
      fail();
    } catch (BulkCacheLoadingException e) {
      // Expected
      assertThat(e.getSuccesses().keySet(), empty());
      assertThat(e.getFailures().keySet(), Matchers.<Set<?>>equalTo(KEY_SET_A));
    }

    verify(this.store).bulkComputeIfAbsent(eq(KEY_SET_A), getAnyIterableFunction());
    assertThat(fakeStore.getEntryMap(), equalTo(getEntryMap(KEY_SET_B)));
    verify(this.loaderWriter, atLeast(1)).loadAll(this.loadAllCaptor.capture());
    assertThat(this.getLoadAllArgs(), equalTo(KEY_SET_A));
    verifyZeroInteractions(this.spiedResilienceStrategy);

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.GetOutcome.class));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoadingOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetAllOutcome.FAILURE));
    validateBulkCounters(ehcache, 0, 0);
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#getAll(Set)} for
   * <ul>
   *    <li>non-empty request key set</li>
   *    <li>no {@link Store} entries match</li>
   *    <li>with a {@code CacheLoaderWriter} (loader-provided entries not relevant)</li>
   *    <li>all {@link CacheLoaderWriter#loadAll(Iterable)} calls fail with {@link BulkCacheLoadingException}</li>
   * </ul>
   */
  @Test
  public void testGetAllStoreNoMatchLoaderAllFailWithBulkCacheLoadingException() throws Exception {
    final FakeStore fakeStore = new FakeStore(getEntryMap(KEY_SET_B));
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeLoader = new FakeCacheLoaderWriter(TEST_ENTRIES, KEY_SET_A, true);
    this.loaderWriter = spy(fakeLoader);

    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.loaderWriter);

    try {
      ehcache.getAll(KEY_SET_A);
      fail();
    } catch (BulkCacheLoadingException e) {
      // Expected
      assertThat(e.getSuccesses().keySet(), empty());
      assertThat(e.getFailures().keySet(), Matchers.<Set<?>>equalTo(KEY_SET_A));
    }

    verify(this.store).bulkComputeIfAbsent(eq(KEY_SET_A), getAnyIterableFunction());
    assertThat(fakeStore.getEntryMap(), equalTo(getEntryMap(KEY_SET_B)));
    verify(this.loaderWriter, atLeast(1)).loadAll(this.loadAllCaptor.capture());
    assertThat(this.getLoadAllArgs(), equalTo(KEY_SET_A));
    verifyZeroInteractions(this.spiedResilienceStrategy);

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.GetOutcome.class));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoadingOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetAllOutcome.FAILURE));
    validateBulkCounters(ehcache, 0, 0);
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#getAll(Set)} for
   * <ul>
   *    <li>non-empty request key set</li>
   *    <li>no {@link Store} entries match</li>
   *    <li>with a {@code CacheLoaderWriter} (loader-provided entries not relevant)</li>
   *    <li>some {@link CacheLoaderWriter#loadAll(Iterable)} calls fail with {@link BulkCacheLoadingException}</li>
   * </ul>
   */
  @Test
  public void testGetAllStoreNoMatchLoaderSomeFailWithBulkCacheLoadingException() throws Exception {
    final FakeStore fakeStore = new FakeStore(getEntryMap(KEY_SET_B));
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeLoader = new FakeCacheLoaderWriter(TEST_ENTRIES, KEY_SET_A, true);
    this.loaderWriter = spy(fakeLoader);

    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.loaderWriter);
    Set<String> fetchKeys = fanIn(KEY_SET_A, KEY_SET_C);
    try {
      ehcache.getAll(fetchKeys);
      fail();
    } catch (BulkCacheLoadingException e) {
      // Expected
      assertThat(e.getSuccesses().keySet(), Matchers.<Set<?>>equalTo(KEY_SET_C));
      assertThat(e.getFailures().keySet(), Matchers.<Set<?>>equalTo(KEY_SET_A));
    }

    verify(this.store).bulkComputeIfAbsent(eq(fetchKeys), getAnyIterableFunction());
    assertThat(fakeStore.getEntryMap(), equalTo(getEntryMap(KEY_SET_B)));
    verify(this.loaderWriter, atLeast(1)).loadAll(this.loadAllCaptor.capture());
    assertThat(this.getLoadAllArgs(), equalTo(fetchKeys));
    verifyZeroInteractions(this.spiedResilienceStrategy);

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.GetOutcome.class));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoadingOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetAllOutcome.FAILURE));
    validateBulkCounters(ehcache, 0, 0);
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#getAll(Set)} for
   * <ul>
   *    <li>non-empty request key set</li>
   *    <li>no {@link Store} entries match</li>
   *    <li>{@link Store#bulkComputeIfAbsent} throws before accessing loader</li>
   *    <li>with a {@code CacheLoaderWriter} (loader-provided entries not relevant)</li>
   *    <li>all {@link CacheLoaderWriter#loadAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testGetAllStoreNoMatchStoreAccessExceptionBeforeLoaderAllFail() throws Exception {
    final FakeStore fakeStore = new FakeStore(getEntryMap(KEY_SET_B));
    this.store = spy(fakeStore);
    doThrow(new StoreAccessException("")).when(this.store)
        .bulkComputeIfAbsent(getAnyStringSet(), getAnyIterableFunction());

    final FakeCacheLoaderWriter fakeLoader = new FakeCacheLoaderWriter(TEST_ENTRIES);
    this.loaderWriter = spy(fakeLoader);
    doThrow(new Exception("loadAll failed")).when(this.loaderWriter).loadAll(getAnyStringSet());

    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.loaderWriter);

    try {
      ehcache.getAll(KEY_SET_A);
      fail();
    } catch (BulkCacheLoadingException e) {
      // Expected
    }

    final InOrder ordered = inOrder(this.loaderWriter, this.spiedResilienceStrategy);
    verify(this.store).bulkComputeIfAbsent(eq(KEY_SET_A), getAnyIterableFunction());
    // ResilienceStrategy invoked: no assertion for Store content
    ordered.verify(this.loaderWriter, atLeast(1)).loadAll(this.loadAllCaptor.capture());
    assertThat(this.getLoadAllArgs(), equalTo(KEY_SET_A));

    ordered.verify(this.spiedResilienceStrategy)
        .getAllFailure(eq(KEY_SET_A), any(StoreAccessException.class), this.bulkExceptionCaptor.capture());
    assertThat(this.bulkExceptionCaptor.getValue().getSuccesses().keySet(), empty());
    assertThat(this.bulkExceptionCaptor.getValue().getFailures().keySet(), Matchers.<Set<?>>equalTo(KEY_SET_A));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.GetOutcome.class));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoadingOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetAllOutcome.FAILURE));
    validateBulkCounters(ehcache, 0, 0);
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#getAll(Set)} for
   * <ul>
   *    <li>non-empty request key set</li>
   *    <li>no {@link Store} entries match</li>
   *    <li>{@link Store#bulkComputeIfAbsent} throws before accessing loader</li>
   *    <li>with a {@code CacheLoaderWriter} (loader-provided entries not relevant)</li>
   *    <li>all {@link CacheLoaderWriter#loadAll(Iterable)} calls fail with {@link BulkCacheLoadingException}</li>
   * </ul>
   */
  @Test
  public void testGetAllStoreNoMatchStoreAccessExceptionBeforeLoaderAllFailWithBulkCacheLoadingException() throws Exception {
    final FakeStore fakeStore = new FakeStore(getEntryMap(KEY_SET_B));
    this.store = spy(fakeStore);
    doThrow(new StoreAccessException("")).when(this.store)
        .bulkComputeIfAbsent(getAnyStringSet(), getAnyIterableFunction());

    final FakeCacheLoaderWriter fakeLoader = new FakeCacheLoaderWriter(TEST_ENTRIES, KEY_SET_A, true);
    this.loaderWriter = spy(fakeLoader);

    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.loaderWriter);

    try {
      ehcache.getAll(KEY_SET_A);
      fail();
    } catch (BulkCacheLoadingException e) {
      // Expected
      assertThat(e.getSuccesses().keySet(), empty());
      assertThat(e.getFailures().keySet(), Matchers.<Set<?>>equalTo(KEY_SET_A));
    }

    final InOrder ordered = inOrder(this.loaderWriter, this.spiedResilienceStrategy);
    verify(this.store).bulkComputeIfAbsent(eq(KEY_SET_A), getAnyIterableFunction());
    // ResilienceStrategy invoked: no assertion for Store content
    ordered.verify(this.loaderWriter, atLeast(1)).loadAll(this.loadAllCaptor.capture());
    assertThat(this.getLoadAllArgs(), equalTo(KEY_SET_A));

    ordered.verify(this.spiedResilienceStrategy)
        .getAllFailure(eq(KEY_SET_A), any(StoreAccessException.class), this.bulkExceptionCaptor.capture());

    verifyBulkLoadingException(this.bulkExceptionCaptor.getValue(), Collections.<String> emptySet(), KEY_SET_A);
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.GetOutcome.class));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoadingOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetAllOutcome.FAILURE));
    validateBulkCounters(ehcache, 0, 0);
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#getAll(Set)} for
   * <ul>
   *    <li>non-empty request key set</li>
   *    <li>no {@link Store} entries match</li>
   *    <li>{@link Store#bulkComputeIfAbsent} throws before accessing loader</li>
   *    <li>with a {@code CacheLoaderWriter} (loader-provided entries not relevant)</li>
   *    <li>some {@link CacheLoaderWriter#loadAll(Iterable)} calls fail with {@link BulkCacheLoadingException}</li>
   * </ul>
   */
  @Test
  public void testGetAllStoreNoMatchStoreAccessExceptionBeforeLoaderSomeFailWithBulkCacheLoadingException() throws Exception {
    final FakeStore fakeStore = new FakeStore(getEntryMap(KEY_SET_B));
    this.store = spy(fakeStore);
    doThrow(new StoreAccessException("")).when(this.store)
        .bulkComputeIfAbsent(getAnyStringSet(), getAnyIterableFunction());

    final FakeCacheLoaderWriter fakeLoader = new FakeCacheLoaderWriter(TEST_ENTRIES, KEY_SET_A, true);
    this.loaderWriter = spy(fakeLoader);

    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.loaderWriter);
    Set<String> fetchKeys = fanIn(KEY_SET_A, KEY_SET_C);
    try {
      ehcache.getAll(fetchKeys);
      fail();
    } catch (BulkCacheLoadingException e) {
      // Expected
      assertThat(e.getSuccesses().keySet(), Matchers.<Set<?>>equalTo(KEY_SET_C));
      assertThat(e.getFailures().keySet(), Matchers.<Set<?>>equalTo(KEY_SET_A));
    }

    final InOrder ordered = inOrder(this.loaderWriter, this.spiedResilienceStrategy);
    verify(this.store).bulkComputeIfAbsent(eq(fetchKeys), getAnyIterableFunction());
    // ResilienceStrategy invoked: no assertion for Store content
    ordered.verify(this.loaderWriter, atLeast(1)).loadAll(this.loadAllCaptor.capture());
    assertThat(this.getLoadAllArgs(), equalTo(fetchKeys));

    ordered.verify(this.spiedResilienceStrategy)
        .getAllFailure(eq(fetchKeys), any(StoreAccessException.class), this.bulkExceptionCaptor.capture());
    verifyBulkLoadingException(this.bulkExceptionCaptor.getValue(), KEY_SET_C, KEY_SET_A);

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.GetOutcome.class));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoadingOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetAllOutcome.FAILURE));
    validateBulkCounters(ehcache, 0, 0);
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#getAll(Set)} for
   * <ul>
   *    <li>non-empty request key set</li>
   *    <li>no {@link Store} entries match</li>
   *    <li>{@link Store#bulkComputeIfAbsent} throws after accessing loader</li>
   *    <li>with a {@code CacheLoaderWriter} (loader-provided entries not relevant)</li>
   *    <li>all {@link CacheLoaderWriter#loadAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testGetAllStoreNoMatchStoreAccessExceptionAfterLoaderAllFail() throws Exception {
    final FakeStore fakeStore = new FakeStore(getEntryMap(KEY_SET_B), Collections.singleton("keyA3"));
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeLoader = new FakeCacheLoaderWriter(TEST_ENTRIES);
    this.loaderWriter = spy(fakeLoader);
    doThrow(new Exception("loadAll failed")).when(this.loaderWriter).loadAll(getAnyStringSet());

    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.loaderWriter);

    try {
      ehcache.getAll(KEY_SET_A);
      fail();
    } catch (BulkCacheLoadingException e) {
      // Expected
    }

    final InOrder ordered = inOrder(this.loaderWriter, this.spiedResilienceStrategy);
    verify(this.store).bulkComputeIfAbsent(eq(KEY_SET_A), getAnyIterableFunction());
    // ResilienceStrategy invoked: no assertion for Store content
    ordered.verify(this.loaderWriter, atLeast(1)).loadAll(this.loadAllCaptor.capture());
    assertThat(this.getLoadAllArgs(), equalTo(KEY_SET_A));

    ordered.verify(this.spiedResilienceStrategy)
        .getAllFailure(eq(KEY_SET_A), any(StoreAccessException.class), this.bulkExceptionCaptor.capture());
    assertThat(this.bulkExceptionCaptor.getValue().getSuccesses().keySet(), empty());
    assertThat(this.bulkExceptionCaptor.getValue().getFailures().keySet(), Matchers.<Set<?>>equalTo(KEY_SET_A));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.GetOutcome.class));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoadingOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetAllOutcome.FAILURE));
    validateBulkCounters(ehcache, 0, 0);
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#getAll(Set)} for
   * <ul>
   *    <li>non-empty request key set</li>
   *    <li>no {@link Store} entries match</li>
   *    <li>{@link Store#bulkComputeIfAbsent} throws after accessing loader</li>
   *    <li>with a {@code CacheLoaderWriter} (loader-provided entries not relevant)</li>
   *    <li>all {@link CacheLoaderWriter#loadAll(Iterable)} calls fails with {@link BulkCacheLoadingException}</li>
   * </ul>
   */
  @Test
  public void testGetAllStoreNoMatchStoreAccessExceptionAfterLoaderAllFailWithBulkCacheLoadingException() throws Exception {
    final FakeStore fakeStore = new FakeStore(getEntryMap(KEY_SET_B), Collections.singleton("keyA3"));
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeLoader = new FakeCacheLoaderWriter(TEST_ENTRIES, KEY_SET_A, true);
    this.loaderWriter = spy(fakeLoader);

    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.loaderWriter);

    try {
      ehcache.getAll(KEY_SET_A);
      fail();
    } catch (BulkCacheLoadingException e) {
      // Expected
      assertThat(e.getSuccesses().keySet(), empty());
      assertThat(e.getFailures().keySet(), Matchers.<Set<?>>equalTo(KEY_SET_A));
    }

    final InOrder ordered = inOrder(this.loaderWriter, this.spiedResilienceStrategy);
    verify(this.store).bulkComputeIfAbsent(eq(KEY_SET_A), getAnyIterableFunction());
    // ResilienceStrategy invoked: no assertion for Store content
    ordered.verify(this.loaderWriter, atLeast(1)).loadAll(this.loadAllCaptor.capture());
    assertThat(this.getLoadAllArgs(), equalTo(KEY_SET_A));

    ordered.verify(this.spiedResilienceStrategy)
        .getAllFailure(eq(KEY_SET_A), any(StoreAccessException.class), this.bulkExceptionCaptor.capture());
    verifyBulkLoadingException(this.bulkExceptionCaptor.getValue(), Collections.<String> emptySet(), KEY_SET_A);

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.GetOutcome.class));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoadingOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetAllOutcome.FAILURE));
    validateBulkCounters(ehcache, 0, 0);
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#getAll(Set)} for
   * <ul>
   *    <li>non-empty request key set</li>
   *    <li>no {@link Store} entries match</li>
   *    <li>{@link Store#bulkComputeIfAbsent} throws after accessing loader</li>
   *    <li>with a {@code CacheLoaderWriter} (loader-provided entries not relevant)</li>
   *    <li>some {@link CacheLoaderWriter#loadAll(Iterable)} calls fails with {@link BulkCacheLoadingException}</li>
   * </ul>
   */
  @Test
  public void testGetAllStoreNoMatchStoreAccessExceptionAfterLoaderSomeFailWithBulkCacheLoadingException() throws Exception {
    final FakeStore fakeStore = new FakeStore(getEntryMap(KEY_SET_B), Collections.singleton("keyA3"));
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeLoader = new FakeCacheLoaderWriter(TEST_ENTRIES, KEY_SET_A, true);
    this.loaderWriter = spy(fakeLoader);

    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.loaderWriter);
    Set<String> fetchKeys = fanIn(KEY_SET_A, KEY_SET_C);

    try {
      ehcache.getAll(fetchKeys);
      fail();
    } catch (BulkCacheLoadingException e) {
      // Expected
      assertThat(e.getSuccesses().keySet(), Matchers.<Set<?>>equalTo(KEY_SET_C));
      assertThat(e.getFailures().keySet(), Matchers.<Set<?>>equalTo(KEY_SET_A));
    }

    final InOrder ordered = inOrder(this.loaderWriter, this.spiedResilienceStrategy);
    verify(this.store).bulkComputeIfAbsent(eq(fetchKeys), getAnyIterableFunction());
    // ResilienceStrategy invoked: no assertion for Store content
    ordered.verify(this.loaderWriter, atLeast(1)).loadAll(this.loadAllCaptor.capture());
    assertThat(this.getLoadAllArgs(), equalTo(fetchKeys));

    ordered.verify(this.spiedResilienceStrategy)
        .getAllFailure(eq(fetchKeys), any(StoreAccessException.class), this.bulkExceptionCaptor.capture());
    verifyBulkLoadingException(this.bulkExceptionCaptor.getValue(), KEY_SET_C, KEY_SET_A);

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.GetOutcome.class));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoadingOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetAllOutcome.FAILURE));
    validateBulkCounters(ehcache, 0, 0);
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#getAll(Set)} for
   * <ul>
   *    <li>non-empty request key set</li>
   *    <li>no {@link Store} entries match</li>
   *    <li>no {@link CacheLoaderWriter} entries match</li>
   *    <li>no {@link CacheLoaderWriter#loadAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testGetAllStoreNoMatchLoaderNoMatchNoneFail() throws Exception {
    final FakeStore fakeStore = new FakeStore(getEntryMap(KEY_SET_B));
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeLoader = new FakeCacheLoaderWriter(getEntryMap(KEY_SET_C));
    this.loaderWriter = spy(fakeLoader);

    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.loaderWriter);

    final Map<String, String> actual = ehcache.getAll(KEY_SET_A);
    assertThat(actual, equalTo(getNullEntryMap(KEY_SET_A)));

    verify(this.store).bulkComputeIfAbsent(eq(KEY_SET_A), getAnyIterableFunction());
    assertThat(fakeStore.getEntryMap(), equalTo(getEntryMap(KEY_SET_B)));
    verify(this.loaderWriter, atLeast(1)).loadAll(this.loadAllCaptor.capture());
    assertThat(this.getLoadAllArgs(), equalTo(KEY_SET_A));
    verifyZeroInteractions(this.spiedResilienceStrategy);

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.GetOutcome.class));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoadingOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetAllOutcome.SUCCESS));
    validateBulkCounters(ehcache, 0, KEY_SET_A.size());
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#getAll(java.util.Set)} for
   * <ul>
   *    <li>non-empty request key set</li>
   *    <li>no {@link Store} entries match</li>
   *    <li>{@link Store#bulkComputeIfAbsent} throws before accessing loader</li>
   *    <li>no {@link org.ehcache.spi.loaderwriter.CacheLoaderWriter} entries match</li>
   *    <li>no {@link org.ehcache.spi.loaderwriter.CacheLoaderWriter#loadAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testGetAllStoreNoMatchStoreAccessExceptionBeforeLoaderNoMatchNoneFail() throws Exception {
    final FakeStore fakeStore = new FakeStore(getEntryMap(KEY_SET_B));
    this.store = spy(fakeStore);
    doThrow(new StoreAccessException("")).when(this.store)
        .bulkComputeIfAbsent(getAnyStringSet(), getAnyIterableFunction());

    final FakeCacheLoaderWriter fakeLoader = new FakeCacheLoaderWriter(getEntryMap(KEY_SET_C));
    this.loaderWriter = spy(fakeLoader);

    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.loaderWriter);

    final Map<String, String> actual = ehcache.getAll(KEY_SET_A);
    final Map<String, String> expected = getNullEntryMap(KEY_SET_A);
    assertThat(actual, equalTo(expected));

    final InOrder ordered = inOrder(this.loaderWriter, this.spiedResilienceStrategy);
    verify(this.store).bulkComputeIfAbsent(eq(KEY_SET_A), getAnyIterableFunction());
    // ResilienceStrategy invoked: no assertion for Store content
    ordered.verify(this.loaderWriter, atLeast(1)).loadAll(this.loadAllCaptor.capture());
    assertThat(this.getLoadAllArgs(), equalTo(KEY_SET_A));
    ordered.verify(this.spiedResilienceStrategy)
        .getAllFailure(eq(KEY_SET_A), eq(expected), any(StoreAccessException.class));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.GetOutcome.class));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoadingOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetAllOutcome.FAILURE));
    validateBulkCounters(ehcache, 0, 0);
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#getAll(Set)} for
   * <ul>
   *    <li>non-empty request key set</li>
   *    <li>no {@link Store} entries match</li>
   *    <li>{@link Store#bulkComputeIfAbsent} throws after accessing loader</li>
   *    <li>no {@link CacheLoaderWriter} entries match</li>
   *    <li>no {@link CacheLoaderWriter#loadAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testGetAllStoreNoMatchStoreAccessExceptionAfterLoaderNoMatchNoneFail() throws Exception {
    final FakeStore fakeStore = new FakeStore(getEntryMap(KEY_SET_B), Collections.singleton("keyA3"));
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeLoader = new FakeCacheLoaderWriter(getEntryMap(KEY_SET_C));
    this.loaderWriter = spy(fakeLoader);

    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.loaderWriter);

    final Map<String, String> actual = ehcache.getAll(KEY_SET_A);
    final Map<String, String> expected = getNullEntryMap(KEY_SET_A);
    assertThat(actual, equalTo(expected));

    final InOrder ordered = inOrder(this.loaderWriter, this.spiedResilienceStrategy);
    verify(this.store).bulkComputeIfAbsent(eq(KEY_SET_A), getAnyIterableFunction());
    // ResilienceStrategy invoked: no assertion for Store content
    ordered.verify(this.loaderWriter, atLeast(1)).loadAll(this.loadAllCaptor.capture());
    assertThat(this.getLoadAllArgs(), equalTo(KEY_SET_A));
    ordered.verify(this.spiedResilienceStrategy)
        .getAllFailure(eq(KEY_SET_A), eq(expected), any(StoreAccessException.class));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.GetOutcome.class));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoadingOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetAllOutcome.FAILURE));
    validateBulkCounters(ehcache, 0, 0);
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#getAll(Set)} for
   * <ul>
   *    <li>non-empty request key set</li>
   *    <li>no {@link Store} entries match</li>
   *    <li>no {@link CacheLoaderWriter} entries match</li>
   *    <li>some {@link CacheLoaderWriter#loadAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testGetAllStoreNoMatchLoaderNoMatchSomeFail() throws Exception {
    final FakeStore fakeStore = new FakeStore(getEntryMap(KEY_SET_B));
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeLoader = new FakeCacheLoaderWriter(getEntryMap(KEY_SET_C), KEY_SET_F);
    this.loaderWriter = spy(fakeLoader);

    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.loaderWriter);

    final Set<String> fetchKeys = fanIn(KEY_SET_A, KEY_SET_F);
    try {
      ehcache.getAll(fetchKeys);
      fail();
    } catch (BulkCacheLoadingException e) {
      // Expected
      assertThat(e.getSuccesses(), Matchers.<Map<?,?>>equalTo(getNullEntryMap(KEY_SET_A)));
      assertThat(e.getFailures().keySet(), Matchers.<Set<?>>equalTo(KEY_SET_F));
    }
    verify(this.store).bulkComputeIfAbsent(eq(fetchKeys), getAnyIterableFunction());
    assertThat(fakeStore.getEntryMap(), equalTo(getEntryMap(KEY_SET_B)));
    verify(this.loaderWriter, atLeast(1)).loadAll(this.loadAllCaptor.capture());
    assertThat(this.getLoadAllArgs(), equalTo(fetchKeys));
    verifyZeroInteractions(this.spiedResilienceStrategy);

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.GetOutcome.class));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoadingOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetAllOutcome.FAILURE));
    validateBulkCounters(ehcache, 0, 0);
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#getAll(Set)} for
   * <ul>
   *    <li>non-empty request key set</li>
   *    <li>no {@link Store} entries match</li>
   *    <li>{@link Store#bulkComputeIfAbsent} throws before accessing loader</li>
   *    <li>no {@link CacheLoaderWriter} entries match</li>
   *    <li>some {@link CacheLoaderWriter#loadAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testGetAllStoreNoMatchStoreAccessExceptionBeforeLoaderNoMatchSomeFail() throws Exception {
    final FakeStore fakeStore = new FakeStore(getEntryMap(KEY_SET_B));
    this.store = spy(fakeStore);
    doThrow(new StoreAccessException("")).when(this.store)
        .bulkComputeIfAbsent(getAnyStringSet(), getAnyIterableFunction());

    final FakeCacheLoaderWriter fakeLoader = new FakeCacheLoaderWriter(getEntryMap(KEY_SET_C), KEY_SET_F);
    this.loaderWriter = spy(fakeLoader);

    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.loaderWriter);

    final Set<String> fetchKeys = fanIn(KEY_SET_A, KEY_SET_F);
    try {
      ehcache.getAll(fetchKeys);
      fail();
    } catch (BulkCacheLoadingException e) {
      // Expected
    }

    final InOrder ordered = inOrder(this.loaderWriter, this.spiedResilienceStrategy);
    verify(this.store).bulkComputeIfAbsent(eq(fetchKeys), getAnyIterableFunction());
    // ResilienceStrategy invoked: no assertion for Store content
    ordered.verify(this.loaderWriter, atLeast(1)).loadAll(this.loadAllCaptor.capture());
    assertThat(this.getLoadAllArgs(), equalTo(fetchKeys));

    ordered.verify(this.spiedResilienceStrategy)
        .getAllFailure(eq(fetchKeys), any(StoreAccessException.class), this.bulkExceptionCaptor.capture());
    assertThat(this.bulkExceptionCaptor.getValue().getSuccesses().keySet(), empty());
    assertThat(this.bulkExceptionCaptor.getValue().getFailures().keySet(), Matchers.<Set<?>>equalTo(fetchKeys));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.GetOutcome.class));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoadingOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetAllOutcome.FAILURE));
    validateBulkCounters(ehcache, 0, 0);
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#getAll(Set)} for
   * <ul>
   *    <li>non-empty request key set</li>
   *    <li>no {@link Store} entries match</li>
   *    <li>{@link Store#bulkComputeIfAbsent} throws after accessing loader</li>
   *    <li>no {@link CacheLoaderWriter} entries match</li>
   *    <li>some {@link CacheLoaderWriter#loadAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testGetAllStoreNoMatchStoreAccessExceptionAfterLoaderNoMatchSomeFail() throws Exception {
    final FakeStore fakeStore = new FakeStore(getEntryMap(KEY_SET_B), Collections.singleton("keyA3"));
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeLoader = new FakeCacheLoaderWriter(getEntryMap(KEY_SET_C), KEY_SET_F);
    this.loaderWriter = spy(fakeLoader);

    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.loaderWriter);

    final Set<String> fetchKeys = fanIn(KEY_SET_A, KEY_SET_F);
    try {
      ehcache.getAll(fetchKeys);
      fail();
    } catch (BulkCacheLoadingException e) {
      // Expected
    }

    final InOrder ordered = inOrder(this.loaderWriter, this.spiedResilienceStrategy);
    verify(this.store).bulkComputeIfAbsent(eq(fetchKeys), getAnyIterableFunction());
    // ResilienceStrategy invoked: no assertion for Store content
    ordered.verify(this.loaderWriter, atLeast(1)).loadAll(this.loadAllCaptor.capture());
    assertThat(this.getLoadAllArgs(), equalTo(fetchKeys));

    ordered.verify(this.spiedResilienceStrategy)
        .getAllFailure(eq(fetchKeys), any(StoreAccessException.class), this.bulkExceptionCaptor.capture());
    final Set<String> successKeys = copyWithout(copyUntil(fetchKeys, "keyA3"), KEY_SET_F);
    final Set<String> failKeys = copyWithout(fetchKeys, successKeys);
    assertThat(this.bulkExceptionCaptor.getValue().getSuccesses(),
        Matchers.<Map<?, ?>>equalTo(getNullEntryMap(successKeys)));
    assertThat(this.bulkExceptionCaptor.getValue().getFailures().keySet(), Matchers.<Set<?>>equalTo(failKeys));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.GetOutcome.class));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoadingOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetAllOutcome.FAILURE));
    validateBulkCounters(ehcache, 0, 0);
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#getAll(Set)} for
   * <ul>
   *    <li>non-empty request key set</li>
   *    <li>no {@link Store} entries match</li>
   *    <li>some {@link CacheLoaderWriter} entries match</li>
   *    <li>no {@link CacheLoaderWriter#loadAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testGetAllStoreNoMatchLoaderSomeMatchNoneFail() throws Exception {
    final FakeStore fakeStore = new FakeStore(getEntryMap(KEY_SET_B));
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeLoader = new FakeCacheLoaderWriter(getEntryMap(KEY_SET_B, KEY_SET_C));
    this.loaderWriter = spy(fakeLoader);

    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.loaderWriter);

    final Set<String> fetchKeys = fanIn(KEY_SET_A, KEY_SET_C);
    final Map<String, String> actual = ehcache.getAll(fetchKeys);

    assertThat(actual, equalTo(union(getNullEntryMap(KEY_SET_A), getEntryMap(KEY_SET_C))));

    verify(this.store).bulkComputeIfAbsent(eq(fetchKeys), getAnyIterableFunction());
    assertThat(fakeStore.getEntryMap(), equalTo(getEntryMap(KEY_SET_B, KEY_SET_C)));
    verify(this.loaderWriter, atLeast(1)).loadAll(this.loadAllCaptor.capture());
    assertThat(this.getLoadAllArgs(), equalTo(fetchKeys));
    verifyZeroInteractions(this.spiedResilienceStrategy);

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.GetOutcome.class));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoadingOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetAllOutcome.SUCCESS));
    validateBulkCounters(ehcache, KEY_SET_C.size(), KEY_SET_A.size());
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#getAll(Set)} for
   * <ul>
   *    <li>non-empty request key set</li>
   *    <li>no {@link Store} entries match</li>
   *    <li>{@link Store#bulkComputeIfAbsent} throws before accessing loader</li>
   *    <li>some {@link CacheLoaderWriter} entries match</li>
   *    <li>no {@link CacheLoaderWriter#loadAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testGetAllStoreNoMatchStoreAccessExceptionBeforeLoaderSomeMatchNoneFail() throws Exception {
    final FakeStore fakeStore = new FakeStore(getEntryMap(KEY_SET_B));
    this.store = spy(fakeStore);
    doThrow(new StoreAccessException("")).when(this.store)
        .bulkComputeIfAbsent(getAnyStringSet(), getAnyIterableFunction());

    final FakeCacheLoaderWriter fakeLoader = new FakeCacheLoaderWriter(getEntryMap(KEY_SET_B, KEY_SET_C));
    this.loaderWriter = spy(fakeLoader);

    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.loaderWriter);

    final Set<String> fetchKeys = fanIn(KEY_SET_A, KEY_SET_C);
    final Map<String, String> actual = ehcache.getAll(fetchKeys);

    final Map<String, String> expected = union(getNullEntryMap(KEY_SET_A), getEntryMap(KEY_SET_C));
    assertThat(actual, equalTo(expected));

    final InOrder ordered = inOrder(this.loaderWriter, this.spiedResilienceStrategy);
    verify(this.store).bulkComputeIfAbsent(eq(fetchKeys), getAnyIterableFunction());
    // ResilienceStrategy invoked: no assertion for Store content
    ordered.verify(this.loaderWriter, atLeast(1)).loadAll(this.loadAllCaptor.capture());
    assertThat(this.getLoadAllArgs(), equalTo(fetchKeys));
    ordered.verify(this.spiedResilienceStrategy)
        .getAllFailure(eq(fetchKeys), eq(expected), any(StoreAccessException.class));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.GetOutcome.class));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoadingOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetAllOutcome.FAILURE));
    validateBulkCounters(ehcache, 0, 0);
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#getAll(Set)} for
   * <ul>
   *    <li>non-empty request key set</li>
   *    <li>no {@link Store} entries match</li>
   *    <li>{@link Store#bulkComputeIfAbsent} throws after accessing loader</li>
   *    <li>some {@link CacheLoaderWriter} entries match</li>
   *    <li>no {@link CacheLoaderWriter#loadAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testGetAllStoreNoMatchStoreAccessExceptionAfterLoaderSomeMatchNoneFail() throws Exception {
    final FakeStore fakeStore = new FakeStore(getEntryMap(KEY_SET_B), Collections.singleton("keyA3"));
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeLoader = new FakeCacheLoaderWriter(getEntryMap(KEY_SET_B, KEY_SET_C));
    this.loaderWriter = spy(fakeLoader);

    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.loaderWriter);

    final Set<String> fetchKeys = fanIn(KEY_SET_A, KEY_SET_C);
    final Map<String, String> actual = ehcache.getAll(fetchKeys);

    final Map<String, String> expected = union(getNullEntryMap(KEY_SET_A), getEntryMap(KEY_SET_C));
    assertThat(actual, equalTo(expected));

    final InOrder ordered = inOrder(this.loaderWriter, this.spiedResilienceStrategy);
    verify(this.store).bulkComputeIfAbsent(eq(fetchKeys), getAnyIterableFunction());
    // ResilienceStrategy invoked: no assertion for Store content
    ordered.verify(this.loaderWriter, atLeast(1)).loadAll(this.loadAllCaptor.capture());
    assertThat(this.getLoadAllArgs(), equalTo(fetchKeys));
    ordered.verify(this.spiedResilienceStrategy)
        .getAllFailure(eq(fetchKeys), eq(expected), any(StoreAccessException.class));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.GetOutcome.class));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoadingOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetAllOutcome.FAILURE));
    validateBulkCounters(ehcache, 0, 0);
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#getAll(Set)} for
   * <ul>
   *    <li>non-empty request key set</li>
   *    <li>no {@link Store} entries match</li>
   *    <li>some {@link CacheLoaderWriter} entries match</li>
   *    <li>non-matching {@link CacheLoaderWriter#loadAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testGetAllStoreNoMatchLoaderSomeMatchDisjointFail() throws Exception {
    final FakeStore fakeStore = new FakeStore(getEntryMap(KEY_SET_B));
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeLoader = new FakeCacheLoaderWriter(getEntryMap(KEY_SET_C), KEY_SET_B);
    this.loaderWriter = spy(fakeLoader);

    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.loaderWriter);

    final Set<String> fetchKeys = fanIn(KEY_SET_A, KEY_SET_C);
    final Map<String, String> actual = ehcache.getAll(fetchKeys);

    assertThat(actual, equalTo(union(getNullEntryMap(KEY_SET_A), getEntryMap(KEY_SET_C))));

    verify(this.store).bulkComputeIfAbsent(eq(fetchKeys), getAnyIterableFunction());
    assertThat(fakeStore.getEntryMap(), equalTo(getEntryMap(KEY_SET_B, KEY_SET_C)));
    verify(this.loaderWriter, atLeast(1)).loadAll(this.loadAllCaptor.capture());
    assertThat(this.getLoadAllArgs(), equalTo(fetchKeys));
    verifyZeroInteractions(this.spiedResilienceStrategy);

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.GetOutcome.class));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoadingOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetAllOutcome.SUCCESS));
    validateBulkCounters(ehcache, KEY_SET_C.size(), KEY_SET_A.size());
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#getAll(Set)} for
   * <ul>
   *    <li>non-empty request key set</li>
   *    <li>no {@link Store} entries match</li>
   *    <li>{@link Store#bulkComputeIfAbsent} throws before accessing loader</li>
   *    <li>some {@link CacheLoaderWriter} entries match</li>
   *    <li>non-matching {@link CacheLoaderWriter#loadAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testGetAllStoreNoMatchStoreAccessExceptionBeforeLoaderSomeMatchDisjointFail() throws Exception {
    final FakeStore fakeStore = new FakeStore(getEntryMap(KEY_SET_B));
    this.store = spy(fakeStore);
    doThrow(new StoreAccessException("")).when(this.store)
        .bulkComputeIfAbsent(getAnyStringSet(), getAnyIterableFunction());

    final FakeCacheLoaderWriter fakeLoader = new FakeCacheLoaderWriter(getEntryMap(KEY_SET_C), KEY_SET_B);
    this.loaderWriter = spy(fakeLoader);

    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.loaderWriter);

    final Set<String> fetchKeys = fanIn(KEY_SET_A, KEY_SET_C);
    final Map<String, String> actual = ehcache.getAll(fetchKeys);

    final Map<String, String> expected = union(getNullEntryMap(KEY_SET_A), getEntryMap(KEY_SET_C));
    assertThat(actual, equalTo(expected));

    final InOrder ordered = inOrder(this.loaderWriter, this.spiedResilienceStrategy);
    verify(this.store).bulkComputeIfAbsent(eq(fetchKeys), getAnyIterableFunction());
    // ResilienceStrategy invoked: no assertion for Store content
    ordered.verify(this.loaderWriter, atLeast(1)).loadAll(this.loadAllCaptor.capture());
    assertThat(this.getLoadAllArgs(), equalTo(fetchKeys));
    ordered.verify(this.spiedResilienceStrategy)
        .getAllFailure(eq(fetchKeys), eq(expected), any(StoreAccessException.class));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.GetOutcome.class));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoadingOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetAllOutcome.FAILURE));
    validateBulkCounters(ehcache, 0, 0);
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#getAll(Set)} for
   * <ul>
   *    <li>non-empty request key set</li>
   *    <li>no {@link Store} entries match</li>
   *    <li>{@link Store#bulkComputeIfAbsent} throws after accessing loader</li>
   *    <li>some {@link CacheLoaderWriter} entries match</li>
   *    <li>non-matching {@link CacheLoaderWriter#loadAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testGetAllStoreNoMatchStoreAccessExceptionAfterLoaderSomeMatchDisjointFail() throws Exception {
    final FakeStore fakeStore = new FakeStore(getEntryMap(KEY_SET_B), Collections.singleton("keyA3"));
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeLoader = new FakeCacheLoaderWriter(getEntryMap(KEY_SET_C), KEY_SET_B);
    this.loaderWriter = spy(fakeLoader);

    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.loaderWriter);

    final Set<String> fetchKeys = fanIn(KEY_SET_A, KEY_SET_C);
    final Map<String, String> actual = ehcache.getAll(fetchKeys);

    final Map<String, String> expected = union(getNullEntryMap(KEY_SET_A), getEntryMap(KEY_SET_C));
    assertThat(actual, equalTo(expected));

    final InOrder ordered = inOrder(this.loaderWriter, this.spiedResilienceStrategy);
    verify(this.store).bulkComputeIfAbsent(eq(fetchKeys), getAnyIterableFunction());
    // ResilienceStrategy invoked: no assertion for Store content
    ordered.verify(this.loaderWriter, atLeast(1)).loadAll(this.loadAllCaptor.capture());
    assertThat(this.getLoadAllArgs(), equalTo(fetchKeys));
    ordered.verify(this.spiedResilienceStrategy)
        .getAllFailure(eq(fetchKeys), eq(expected), any(StoreAccessException.class));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.GetOutcome.class));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoadingOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetAllOutcome.FAILURE));
    validateBulkCounters(ehcache, 0, 0);
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#getAll(Set)} for
   * <ul>
   *    <li>non-empty request key set</li>
   *    <li>no {@link Store} entries match</li>
   *    <li>all {@link CacheLoaderWriter} entries match</li>
   *    <li>no {@link CacheLoaderWriter#loadAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testGetAllStoreNoMatchLoaderAllMatchNoneFail() throws Exception {
    final FakeStore fakeStore = new FakeStore(getEntryMap(KEY_SET_B));
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeLoader = new FakeCacheLoaderWriter(getEntryMap(KEY_SET_A, KEY_SET_C));
    this.loaderWriter = spy(fakeLoader);

    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.loaderWriter);

    final Set<String> fetchKeys = fanIn(KEY_SET_A, KEY_SET_C);
    final Map<String, String> actual = ehcache.getAll(fetchKeys);

    assertThat(actual, equalTo(getEntryMap(fetchKeys)));

    verify(this.store).bulkComputeIfAbsent(eq(fetchKeys), getAnyIterableFunction());
    assertThat(fakeStore.getEntryMap(), equalTo(getEntryMap(KEY_SET_A, KEY_SET_B, KEY_SET_C)));
    verify(this.loaderWriter, atLeast(1)).loadAll(this.loadAllCaptor.capture());
    assertThat(this.getLoadAllArgs(), equalTo(fetchKeys));
    verifyZeroInteractions(this.spiedResilienceStrategy);

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.GetOutcome.class));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoadingOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetAllOutcome.SUCCESS));
    validateBulkCounters(ehcache, KEY_SET_A.size() + KEY_SET_C.size(), 0);
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#getAll(Set)} for
   * <ul>
   *    <li>non-empty request key set</li>
   *    <li>no {@link Store} entries match</li>
   *    <li>{@link Store#bulkComputeIfAbsent} throws before accessing loader</li>
   *    <li>all {@link CacheLoaderWriter} entries match</li>
   *    <li>no {@link CacheLoaderWriter#loadAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testGetAllStoreNoMatchStoreAccessExceptionBeforeLoaderAllMatchNoneFail() throws Exception {
    final FakeStore fakeStore = new FakeStore(getEntryMap(KEY_SET_B));
    this.store = spy(fakeStore);
    doThrow(new StoreAccessException("")).when(this.store)
        .bulkComputeIfAbsent(getAnyStringSet(), getAnyIterableFunction());

    final FakeCacheLoaderWriter fakeLoader = new FakeCacheLoaderWriter(getEntryMap(KEY_SET_A, KEY_SET_C));
    this.loaderWriter = spy(fakeLoader);

    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.loaderWriter);

    final Set<String> fetchKeys = fanIn(KEY_SET_A, KEY_SET_C);
    final Map<String, String> actual = ehcache.getAll(fetchKeys);

    final Map<String, String> expected = getEntryMap(fetchKeys);
    assertThat(actual, equalTo(expected));

    final InOrder ordered = inOrder(this.loaderWriter, this.spiedResilienceStrategy);
    verify(this.store).bulkComputeIfAbsent(eq(fetchKeys), getAnyIterableFunction());
    // ResilienceStrategy invoked: no assertion for Store content
    ordered.verify(this.loaderWriter, atLeast(1)).loadAll(this.loadAllCaptor.capture());
    assertThat(this.getLoadAllArgs(), equalTo(fetchKeys));
    ordered.verify(this.spiedResilienceStrategy)
        .getAllFailure(eq(fetchKeys), eq(expected), any(StoreAccessException.class));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.GetOutcome.class));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoadingOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetAllOutcome.FAILURE));
    validateBulkCounters(ehcache, 0, 0);
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#getAll(Set)} for
   * <ul>
   *    <li>non-empty request key set</li>
   *    <li>no {@link Store} entries match</li>
   *    <li>{@link Store#bulkComputeIfAbsent} throws after accessing loader</li>
   *    <li>all {@link CacheLoaderWriter} entries match</li>
   *    <li>no {@link CacheLoaderWriter#loadAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testGetAllStoreNoMatchStoreAccessExceptionAfterLoaderAllMatchNoneFail() throws Exception {
    final FakeStore fakeStore = new FakeStore(getEntryMap(KEY_SET_B), Collections.singleton("keyA3"));
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeLoader = new FakeCacheLoaderWriter(getEntryMap(KEY_SET_A, KEY_SET_C));
    this.loaderWriter = spy(fakeLoader);

    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.loaderWriter);

    final Set<String> fetchKeys = fanIn(KEY_SET_A, KEY_SET_C);
    final Map<String, String> actual = ehcache.getAll(fetchKeys);

    final Map<String, String> expected = getEntryMap(fetchKeys);
    assertThat(actual, equalTo(expected));

    final InOrder ordered = inOrder(this.loaderWriter, this.spiedResilienceStrategy);
    verify(this.store).bulkComputeIfAbsent(eq(fetchKeys), getAnyIterableFunction());
    // ResilienceStrategy invoked: no assertion for Store content
    ordered.verify(this.loaderWriter, atLeast(1)).loadAll(this.loadAllCaptor.capture());
    assertThat(this.getLoadAllArgs(), equalTo(fetchKeys));
    ordered.verify(this.spiedResilienceStrategy)
        .getAllFailure(eq(fetchKeys), eq(expected), any(StoreAccessException.class));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.GetOutcome.class));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoadingOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetAllOutcome.FAILURE));
    validateBulkCounters(ehcache, 0, 0);
  }




  /**
   * Tests {@link EhcacheWithLoaderWriter#getAll(Set)} for
   * <ul>
   *    <li>non-empty request key set</li>
   *    <li>some {@link Store} entries match</li>
   *    <li>with a {@code CacheLoaderWriter} (loader-provided entries not relevant)</li>
   *    <li>all {@link CacheLoaderWriter#loadAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testGetAllStoreSomeMatchLoaderAllFail() throws Exception {
    final FakeStore fakeStore = new FakeStore(getEntryMap(KEY_SET_A, KEY_SET_B));
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeLoader = new FakeCacheLoaderWriter(TEST_ENTRIES);
    this.loaderWriter = spy(fakeLoader);
    doThrow(new Exception("loadAll failed")).when(this.loaderWriter).loadAll(getAnyStringSet());

    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.loaderWriter);

    final Set<String> fetchKeys = fanIn(KEY_SET_A, KEY_SET_C);
    try {
      ehcache.getAll(fetchKeys);
      fail();
    } catch (BulkCacheLoadingException e) {
      // Expected
      assertThat(e.getSuccesses(), Matchers.<Map<?,?>>equalTo(getEntryMap(KEY_SET_A)));
      assertThat(e.getFailures().keySet(), Matchers.<Set<?>>equalTo(KEY_SET_C));
    }
    verify(this.store).bulkComputeIfAbsent(eq(fetchKeys), getAnyIterableFunction());
    assertThat(fakeStore.getEntryMap(), equalTo(getEntryMap(KEY_SET_A, KEY_SET_B)));
    verify(this.loaderWriter, atLeast(1)).loadAll(this.loadAllCaptor.capture());
    assertThat(this.getLoadAllArgs(), equalTo(KEY_SET_C));
    verifyZeroInteractions(this.spiedResilienceStrategy);

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.GetOutcome.class));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoadingOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetAllOutcome.FAILURE));
    validateBulkCounters(ehcache, KEY_SET_A.size(), 0);
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#getAll(Set)} for
   * <ul>
   *    <li>non-empty request key set</li>
   *    <li>some {@link Store} entries match</li>
   *    <li>with a {@code CacheLoaderWriter} (loader-provided entries not relevant)</li>
   *    <li>all {@link CacheLoaderWriter#loadAll(Iterable)} calls fails with {@link BulkCacheLoadingException}</li>
   * </ul>
   */
  @Test
  public void testGetAllStoreSomeMatchLoaderAllFailWithBulkCacheLoadingException() throws Exception {
    final FakeStore fakeStore = new FakeStore(getEntryMap(KEY_SET_A, KEY_SET_B));
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeLoader = new FakeCacheLoaderWriter(TEST_ENTRIES, KEY_SET_C, true);
    this.loaderWriter = spy(fakeLoader);

    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.loaderWriter);

    final Set<String> fetchKeys = fanIn(KEY_SET_A, KEY_SET_C);
    try {
      ehcache.getAll(fetchKeys);
      fail();
    } catch (BulkCacheLoadingException e) {
      // Expected
      assertThat(e.getSuccesses(), Matchers.<Map<?,?>>equalTo(getEntryMap(KEY_SET_A)));
      assertThat(e.getFailures().keySet(), Matchers.<Set<?>>equalTo(KEY_SET_C));
    }
    verify(this.store).bulkComputeIfAbsent(eq(fetchKeys), getAnyIterableFunction());
    assertThat(fakeStore.getEntryMap(), equalTo(getEntryMap(KEY_SET_A, KEY_SET_B)));
    verify(this.loaderWriter, atLeast(1)).loadAll(this.loadAllCaptor.capture());
    assertThat(this.getLoadAllArgs(), equalTo(KEY_SET_C));
    verifyZeroInteractions(this.spiedResilienceStrategy);

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.GetOutcome.class));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoadingOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetAllOutcome.FAILURE));
    validateBulkCounters(ehcache, KEY_SET_A.size(), 0);
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#getAll(Set)} for
   * <ul>
   *    <li>non-empty request key set</li>
   *    <li>some {@link Store} entries match</li>
   *    <li>with a {@code CacheLoaderWriter} (loader-provided entries not relevant)</li>
   *    <li>some {@link CacheLoaderWriter#loadAll(Iterable)} calls fails with {@link BulkCacheLoadingException}</li>
   * </ul>
   */
  @Test
  public void testGetAllStoreSomeMatchLoaderSomeFailWithBulkCacheLoadingException() throws Exception {
    final FakeStore fakeStore = new FakeStore(getEntryMap(KEY_SET_A, KEY_SET_B));
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeLoader = new FakeCacheLoaderWriter(TEST_ENTRIES, KEY_SET_C, true);
    this.loaderWriter = spy(fakeLoader);

    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.loaderWriter);

    final Set<String> fetchKeys = fanIn(KEY_SET_A, KEY_SET_C, KEY_SET_D);
    try {
      ehcache.getAll(fetchKeys);
      fail();
    } catch (BulkCacheLoadingException e) {
      // Expected
      assertThat(e.getSuccesses(), Matchers.<Map<?, ?>>equalTo(getEntryMap(KEY_SET_A, KEY_SET_D)));
      assertThat(e.getFailures().keySet(), Matchers.<Set<?>>equalTo(KEY_SET_C));
    }
    verify(this.store).bulkComputeIfAbsent(eq(fetchKeys), getAnyIterableFunction());
    assertThat(fakeStore.getEntryMap(), equalTo(getEntryMap(KEY_SET_A, KEY_SET_B)));
    verify(this.loaderWriter, atLeast(1)).loadAll(this.loadAllCaptor.capture());
    assertThat(this.getLoadAllArgs(), equalTo(union(KEY_SET_C, KEY_SET_D)));
    verifyZeroInteractions(this.spiedResilienceStrategy);

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.GetOutcome.class));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoadingOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetAllOutcome.FAILURE));
    validateBulkCounters(ehcache, KEY_SET_A.size(), 0);
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#getAll(Set)} for
   * <ul>
   *    <li>non-empty request key set</li>
   *    <li>some {@link Store} entries match</li>
   *    <li>{@link Store#bulkComputeIfAbsent} throws before accessing loader</li>
   *    <li>with a {@code CacheLoaderWriter} (loader-provided entries not relevant)</li>
   *    <li>all {@link CacheLoaderWriter#loadAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testGetAllStoreSomeMatchStoreAccessExceptionBeforeLoaderAllFail() throws Exception {
    final FakeStore fakeStore = new FakeStore(getEntryMap(KEY_SET_A, KEY_SET_B));
    this.store = spy(fakeStore);
    doThrow(new StoreAccessException("")).when(this.store)
        .bulkComputeIfAbsent(getAnyStringSet(), getAnyIterableFunction());

    final FakeCacheLoaderWriter fakeLoader = new FakeCacheLoaderWriter(TEST_ENTRIES);
    this.loaderWriter = spy(fakeLoader);
    doThrow(new Exception("loadAll failed")).when(this.loaderWriter).loadAll(getAnyStringSet());

    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.loaderWriter);

    final Set<String> fetchKeys = fanIn(KEY_SET_A, KEY_SET_C);
    try {
      ehcache.getAll(fetchKeys);
      fail();
    } catch (BulkCacheLoadingException e) {
      // Expected
    }

    final InOrder ordered = inOrder(this.loaderWriter, this.spiedResilienceStrategy);
    verify(this.store).bulkComputeIfAbsent(eq(fetchKeys), getAnyIterableFunction());
    // ResilienceStrategy invoked: no assertion for Store content
    ordered.verify(this.loaderWriter, atLeast(1)).loadAll(this.loadAllCaptor.capture());
    assertThat(this.getLoadAllArgs(), equalTo(fetchKeys));

    ordered.verify(this.spiedResilienceStrategy)
        .getAllFailure(eq(fetchKeys), any(StoreAccessException.class), this.bulkExceptionCaptor.capture());
    assertThat(this.bulkExceptionCaptor.getValue().getSuccesses().keySet(), empty());
    assertThat(this.bulkExceptionCaptor.getValue().getFailures().keySet(), Matchers.<Set<?>>equalTo(fetchKeys));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.GetOutcome.class));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoadingOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetAllOutcome.FAILURE));
    validateBulkCounters(ehcache, 0, 0);
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#getAll(Set)} for
   * <ul>
   *    <li>non-empty request key set</li>
   *    <li>some {@link Store} entries match</li>
   *    <li>{@link Store#bulkComputeIfAbsent} throws before accessing loader</li>
   *    <li>with a {@code CacheLoaderWriter} (loader-provided entries not relevant)</li>
   *    <li>all {@link CacheLoaderWriter#loadAll(Iterable)} calls fail  with {@link BulkCacheLoadingException}</li>
   * </ul>
   */
  @Test
  public void testGetAllStoreSomeMatchStoreAccessExceptionBeforeLoaderAllFailWithBulkCacheLoadingException() throws Exception {
    final FakeStore fakeStore = new FakeStore(getEntryMap(KEY_SET_A, KEY_SET_B));
    this.store = spy(fakeStore);
    doThrow(new StoreAccessException("")).when(this.store)
        .bulkComputeIfAbsent(getAnyStringSet(), getAnyIterableFunction());

    final FakeCacheLoaderWriter fakeLoader = new FakeCacheLoaderWriter(TEST_ENTRIES, union(KEY_SET_A, KEY_SET_C), true);
    this.loaderWriter = spy(fakeLoader);

    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.loaderWriter);

    final Set<String> fetchKeys = fanIn(KEY_SET_A, KEY_SET_C);
    try {
      ehcache.getAll(fetchKeys);
      fail();
    } catch (BulkCacheLoadingException e) {
      // Expected
      assertThat(e.getSuccesses().keySet(), empty());
      assertThat(e.getFailures().keySet(), Matchers.<Set<?>>equalTo(fetchKeys));
    }

    final InOrder ordered = inOrder(this.loaderWriter, this.spiedResilienceStrategy);
    verify(this.store).bulkComputeIfAbsent(eq(fetchKeys), getAnyIterableFunction());
    // ResilienceStrategy invoked: no assertion for Store content
    ordered.verify(this.loaderWriter, atLeast(1)).loadAll(this.loadAllCaptor.capture());
    assertThat(this.getLoadAllArgs(), equalTo(fetchKeys));

    ordered.verify(this.spiedResilienceStrategy)
        .getAllFailure(eq(fetchKeys), any(StoreAccessException.class), this.bulkExceptionCaptor.capture());
    verifyBulkLoadingException(this.bulkExceptionCaptor.getValue(), Collections.<String> emptySet(), fetchKeys);

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.GetOutcome.class));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoadingOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetAllOutcome.FAILURE));
    validateBulkCounters(ehcache, 0, 0);
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#getAll(Set)} for
   * <ul>
   *    <li>non-empty request key set</li>
   *    <li>some {@link Store} entries match</li>
   *    <li>{@link Store#bulkComputeIfAbsent} throws before accessing loader</li>
   *    <li>with a {@code CacheLoaderWriter} (loader-provided entries not relevant)</li>
   *    <li>some {@link CacheLoaderWriter#loadAll(Iterable)} calls fail  with {@link BulkCacheLoadingException}</li>
   * </ul>
   */
  @Test
  public void testGetAllStoreSomeMatchStoreAccessExceptionBeforeLoaderLSomeFailWithBulkCacheLoadingException() throws Exception {
    final FakeStore fakeStore = new FakeStore(getEntryMap(KEY_SET_A, KEY_SET_B));
    this.store = spy(fakeStore);
    doThrow(new StoreAccessException("")).when(this.store)
        .bulkComputeIfAbsent(getAnyStringSet(), getAnyIterableFunction());

    final FakeCacheLoaderWriter fakeLoader = new FakeCacheLoaderWriter(TEST_ENTRIES, union(KEY_SET_A, KEY_SET_C), true);
    this.loaderWriter = spy(fakeLoader);

    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.loaderWriter);

    final Set<String> fetchKeys = fanIn(KEY_SET_A, KEY_SET_C, KEY_SET_D);
    try {
      ehcache.getAll(fetchKeys);
      fail();
    } catch (BulkCacheLoadingException e) {
      // Expected
      assertThat(e.getSuccesses().keySet(), Matchers.<Set<?>>equalTo(KEY_SET_D));
      assertThat(e.getFailures().keySet(), Matchers.<Set<?>>equalTo(union(KEY_SET_A, KEY_SET_C)));
    }

    final InOrder ordered = inOrder(this.loaderWriter, this.spiedResilienceStrategy);
    verify(this.store).bulkComputeIfAbsent(eq(fetchKeys), getAnyIterableFunction());
    // ResilienceStrategy invoked: no assertion for Store content
    ordered.verify(this.loaderWriter, atLeast(1)).loadAll(this.loadAllCaptor.capture());
    assertThat(this.getLoadAllArgs(), equalTo(fetchKeys));

    ordered.verify(this.spiedResilienceStrategy)
        .getAllFailure(eq(fetchKeys), any(StoreAccessException.class), this.bulkExceptionCaptor.capture());
    verifyBulkLoadingException(this.bulkExceptionCaptor.getValue(), KEY_SET_D, union(KEY_SET_A, KEY_SET_C));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.GetOutcome.class));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoadingOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetAllOutcome.FAILURE));
    validateBulkCounters(ehcache, 0, 0);
  }


  /**
   * Tests {@link EhcacheWithLoaderWriter#getAll(Set)} for
   * <ul>
   *    <li>non-empty request key set</li>
   *    <li>some {@link Store} entries match</li>
   *    <li>{@link Store#bulkComputeIfAbsent} throws after accessing loader</li>
   *    <li>with a {@code CacheLoaderWriter} (loader-provided entries not relevant)</li>
   *    <li>all {@link CacheLoaderWriter#loadAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testGetAllStoreSomeMatchStoreAccessExceptionAfterLoaderAllFail() throws Exception {
    final FakeStore fakeStore = new FakeStore(getEntryMap(KEY_SET_A, KEY_SET_B), Collections.singleton("keyA3"));
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeLoader = new FakeCacheLoaderWriter(TEST_ENTRIES);
    this.loaderWriter = spy(fakeLoader);
    doThrow(new Exception("loadAll failed")).when(this.loaderWriter).loadAll(getAnyStringSet());

    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.loaderWriter);

    final Set<String> fetchKeys = fanIn(KEY_SET_A, KEY_SET_C);
    try {
      ehcache.getAll(fetchKeys);
      fail();
    } catch (BulkCacheLoadingException e) {
      // Expected
    }

    final InOrder ordered = inOrder(this.loaderWriter, this.spiedResilienceStrategy);
    verify(this.store).bulkComputeIfAbsent(eq(fetchKeys), getAnyIterableFunction());
    // ResilienceStrategy invoked: no assertion for Store content
    ordered.verify(this.loaderWriter, atLeast(1)).loadAll(this.loadAllCaptor.capture());
    assertThat(this.getLoadAllArgs(), equalTo(fetchKeys));

    ordered.verify(this.spiedResilienceStrategy)
        .getAllFailure(eq(fetchKeys), any(StoreAccessException.class), this.bulkExceptionCaptor.capture());
    assertThat(this.bulkExceptionCaptor.getValue().getSuccesses().isEmpty(), is(true));
    assertThat(this.bulkExceptionCaptor.getValue().getFailures().keySet(), Matchers.<Set<?>>equalTo(fetchKeys));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.GetOutcome.class));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoadingOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetAllOutcome.FAILURE));
    validateBulkCounters(ehcache, 0, 0);
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#getAll(Set)} for
   * <ul>
   *    <li>non-empty request key set</li>
   *    <li>some {@link Store} entries match</li>
   *    <li>{@link Store#bulkComputeIfAbsent} throws after accessing loader</li>
   *    <li>with a {@code CacheLoaderWriter} (loader-provided entries not relevant)</li>
   *    <li>all {@link CacheLoaderWriter#loadAll(Iterable)} calls fail with {@link BulkCacheLoadingException}</li>
   * </ul>
   */
  @Test
  public void testGetAllStoreSomeMatchStoreAccessExceptionAfterLoaderAllFailWithBulkCacheLoadingException() throws Exception {
    final FakeStore fakeStore = new FakeStore(getEntryMap(KEY_SET_A, KEY_SET_B), Collections.singleton("keyA3"));
    this.store = spy(fakeStore);

    final Set<String> fetchKeys = fanIn(KEY_SET_A, KEY_SET_C);
    final FakeCacheLoaderWriter fakeLoader = new FakeCacheLoaderWriter(TEST_ENTRIES, fetchKeys, true);
    this.loaderWriter = spy(fakeLoader);

    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.loaderWriter);
    try {
      ehcache.getAll(fetchKeys);
      fail();
    } catch (BulkCacheLoadingException e) {
      // Expected
      assertThat(e.getSuccesses().keySet(), empty());
      assertThat(e.getFailures().keySet(), Matchers.<Set<?>>equalTo(fetchKeys));
    }

    final InOrder ordered = inOrder(this.loaderWriter, this.spiedResilienceStrategy);
    verify(this.store).bulkComputeIfAbsent(eq(fetchKeys), getAnyIterableFunction());
    // ResilienceStrategy invoked: no assertion for Store content
    ordered.verify(this.loaderWriter, atLeast(1)).loadAll(this.loadAllCaptor.capture());
    assertThat(this.getLoadAllArgs(), equalTo(fetchKeys));

    ordered.verify(this.spiedResilienceStrategy)
        .getAllFailure(eq(fetchKeys), any(StoreAccessException.class), this.bulkExceptionCaptor.capture());
    verifyBulkLoadingException(this.bulkExceptionCaptor.getValue(), Collections.<String> emptySet(), fetchKeys);

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.GetOutcome.class));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoadingOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetAllOutcome.FAILURE));
    validateBulkCounters(ehcache, 0, 0);
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#getAll(Set)} for
   * <ul>
   *    <li>non-empty request key set</li>
   *    <li>some {@link Store} entries match</li>
   *    <li>{@link Store#bulkComputeIfAbsent} throws after accessing loader</li>
   *    <li>with a {@code CacheLoaderWriter} (loader-provided entries not relevant)</li>
   *    <li>some {@link CacheLoaderWriter#loadAll(Iterable)} calls fail with {@link BulkCacheLoadingException}</li>
   * </ul>
   */
  @Test
  public void testGetAllStoreSomeMatchStoreAccessExceptionAfterLoaderSomeFailWithBulkCacheLoadingException() throws Exception {
    final FakeStore fakeStore = new FakeStore(getEntryMap(KEY_SET_A, KEY_SET_B), Collections.singleton("keyA3"));
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeLoader = new FakeCacheLoaderWriter(TEST_ENTRIES, KEY_SET_A, true);
    this.loaderWriter = spy(fakeLoader);

    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.loaderWriter);

    final Set<String> fetchKeys = fanIn(KEY_SET_A, KEY_SET_C);
    try {
      ehcache.getAll(fetchKeys);
      fail();
    } catch (BulkCacheLoadingException e) {
      // Expected
      assertThat(e.getSuccesses().keySet(), Matchers.<Set<?>>equalTo(KEY_SET_C));
      assertThat(e.getFailures().keySet(), Matchers.<Set<?>>equalTo(KEY_SET_A));
    }

    final InOrder ordered = inOrder(this.loaderWriter, this.spiedResilienceStrategy);
    verify(this.store).bulkComputeIfAbsent(eq(fetchKeys), getAnyIterableFunction());
    // ResilienceStrategy invoked: no assertion for Store content
    ordered.verify(this.loaderWriter, atLeast(1)).loadAll(this.loadAllCaptor.capture());
    assertThat(this.getLoadAllArgs(), equalTo(fetchKeys));

    ordered.verify(this.spiedResilienceStrategy)
        .getAllFailure(eq(fetchKeys), any(StoreAccessException.class), this.bulkExceptionCaptor.capture());
    verifyBulkLoadingException(this.bulkExceptionCaptor.getValue(), KEY_SET_C, KEY_SET_A);

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.GetOutcome.class));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoadingOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetAllOutcome.FAILURE));
    validateBulkCounters(ehcache, 0, 0);
  }


  /**
   * Tests {@link EhcacheWithLoaderWriter#getAll(Set)} for
   * <ul>
   *    <li>non-empty request key set</li>
   *    <li>some {@link Store} entries match</li>
   *    <li>no {@link CacheLoaderWriter} entries match</li>
   *    <li>no {@link CacheLoaderWriter#loadAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testGetAllStoreSomeMatchLoaderNoMatchNoneFail() throws Exception {
    final FakeStore fakeStore = new FakeStore(getEntryMap(KEY_SET_A, KEY_SET_B));
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeLoader = new FakeCacheLoaderWriter(getEntryMap(KEY_SET_F));
    this.loaderWriter = spy(fakeLoader);

    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.loaderWriter);

    final Set<String> fetchKeys = fanIn(KEY_SET_A, KEY_SET_C);
    final Map<String, String> actual = ehcache.getAll(fetchKeys);

    assertThat(actual, equalTo(union(getEntryMap(KEY_SET_A), getNullEntryMap(KEY_SET_C))));

    verify(this.store).bulkComputeIfAbsent(eq(fetchKeys), getAnyIterableFunction());
    assertThat(fakeStore.getEntryMap(), equalTo(getEntryMap(KEY_SET_A, KEY_SET_B)));
    verify(this.loaderWriter, atLeast(1)).loadAll(this.loadAllCaptor.capture());
    assertThat(this.getLoadAllArgs(), equalTo(KEY_SET_C));
    verifyZeroInteractions(this.spiedResilienceStrategy);

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.GetOutcome.class));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoadingOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetAllOutcome.SUCCESS));
    validateBulkCounters(ehcache, KEY_SET_A.size(), KEY_SET_C.size());
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#getAll(Set)} for
   * <ul>
   *    <li>non-empty request key set</li>
   *    <li>some {@link Store} entries match</li>
   *    <li>{@link Store#bulkComputeIfAbsent} throws before accessing loader</li>
   *    <li>no {@link CacheLoaderWriter} entries match</li>
   *    <li>no {@link CacheLoaderWriter#loadAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testGetAllStoreSomeMatchStoreAccessExceptionBeforeLoaderNoMatchNoneFail() throws Exception {
    final FakeStore fakeStore = new FakeStore(getEntryMap(KEY_SET_A, KEY_SET_B));
    this.store = spy(fakeStore);
    doThrow(new StoreAccessException("")).when(this.store)
        .bulkComputeIfAbsent(getAnyStringSet(), getAnyIterableFunction());

    final FakeCacheLoaderWriter fakeLoader = new FakeCacheLoaderWriter(getEntryMap(KEY_SET_F));
    this.loaderWriter = spy(fakeLoader);

    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.loaderWriter);

    final Set<String> fetchKeys = fanIn(KEY_SET_A, KEY_SET_C);
    final Map<String, String> actual = ehcache.getAll(fetchKeys);

    final Map<String, String> expected = getNullEntryMap(fetchKeys);
    assertThat(actual, equalTo(expected));

    final InOrder ordered = inOrder(this.loaderWriter, this.spiedResilienceStrategy);
    verify(this.store).bulkComputeIfAbsent(eq(fetchKeys), getAnyIterableFunction());
    // ResilienceStrategy invoked: no assertion for Store content
    ordered.verify(this.loaderWriter, atLeast(1)).loadAll(this.loadAllCaptor.capture());
    assertThat(this.getLoadAllArgs(), equalTo(fetchKeys));
    ordered.verify(this.spiedResilienceStrategy)
        .getAllFailure(eq(fetchKeys), eq(expected), any(StoreAccessException.class));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.GetOutcome.class));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoadingOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetAllOutcome.FAILURE));
    validateBulkCounters(ehcache, 0, 0);
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#getAll(Set)} for
   * <ul>
   *    <li>non-empty request key set</li>
   *    <li>some {@link Store} entries match</li>
   *    <li>{@link Store#bulkComputeIfAbsent} throws after accessing loader</li>
   *    <li>no {@link CacheLoaderWriter} entries match</li>
   *    <li>no {@link CacheLoaderWriter#loadAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testGetAllStoreSomeMatchStoreAccessExceptionAfterLoaderNoMatchNoneFail() throws Exception {
    final FakeStore fakeStore = new FakeStore(getEntryMap(KEY_SET_A, KEY_SET_B), Collections.singleton("keyA3"));
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeLoader = new FakeCacheLoaderWriter(getEntryMap(KEY_SET_A, KEY_SET_B));
    this.loaderWriter = spy(fakeLoader);

    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.loaderWriter);

    final Set<String> fetchKeys = fanIn(KEY_SET_A, KEY_SET_C);
    final Map<String, String> actual = ehcache.getAll(fetchKeys);

    assertThat(actual, equalTo(union(getNullEntryMap(KEY_SET_C), getEntryMap(KEY_SET_A))));

    final InOrder ordered = inOrder(this.loaderWriter, this.spiedResilienceStrategy);
    verify(this.store).bulkComputeIfAbsent(eq(fetchKeys), getAnyIterableFunction());
    // ResilienceStrategy invoked: no assertion for Store content
    ordered.verify(this.loaderWriter, atLeast(1)).loadAll(this.loadAllCaptor.capture());
    assertThat(this.getLoadAllArgs(), equalTo(fetchKeys));

    ordered.verify(this.spiedResilienceStrategy)
        .getAllFailure(eq(fetchKeys), eq(actual), any(StoreAccessException.class));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.GetOutcome.class));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoadingOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetAllOutcome.FAILURE));
    validateBulkCounters(ehcache, 0, 0);
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#getAll(Set)} for
   * <ul>
   *    <li>non-empty request key set</li>
   *    <li>some {@link Store} entries match</li>
   *    <li>no {@link CacheLoaderWriter} entries match</li>
   *    <li>some {@link CacheLoaderWriter#loadAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testGetAllStoreSomeMatchLoaderNoMatchSomeFail() throws Exception {
    final FakeStore fakeStore = new FakeStore(getEntryMap(KEY_SET_A, KEY_SET_B));
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeLoader = new FakeCacheLoaderWriter(getEntryMap(KEY_SET_F), KEY_SET_D);
    this.loaderWriter = spy(fakeLoader);

    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.loaderWriter);

    final Set<String> fetchKeys = fanIn(KEY_SET_A, KEY_SET_C, KEY_SET_D);
    try {
      ehcache.getAll(fetchKeys);
      fail();
    } catch (BulkCacheLoadingException e) {
      // Expected
      assertThat(e.getSuccesses(), Matchers.<Map<?,?>>equalTo(union(getEntryMap(KEY_SET_A), getNullEntryMap(KEY_SET_C))));
      assertThat(e.getFailures().keySet(), Matchers.<Set<?>>equalTo(KEY_SET_D));
    }

    verify(this.store).bulkComputeIfAbsent(eq(fetchKeys), getAnyIterableFunction());
    // ResilienceStrategy invoked: no assertion for Store content
    verify(this.loaderWriter, atLeast(1)).loadAll(this.loadAllCaptor.capture());
    assertThat(this.getLoadAllArgs(), equalTo(copyWithout(fetchKeys, KEY_SET_A)));
    verifyZeroInteractions(this.spiedResilienceStrategy);

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.GetOutcome.class));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoadingOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetAllOutcome.FAILURE));
    validateBulkCounters(ehcache, KEY_SET_A.size(), 0);
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#getAll(Set)} for
   * <ul>
   *    <li>non-empty request key set</li>
   *    <li>some {@link Store} entries match</li>
   *    <li>no {@link CacheLoaderWriter} entries match</li>
   *    <li>some {@link CacheLoaderWriter#loadAll(Iterable)} calls fail with {@link BulkCacheLoadingException}</li>
   * </ul>
   */
  @Test
  public void testGetAllStoreSomeMatchLoaderNoMatchSomeFailWithBulkCacheLoadingException() throws Exception {
    final FakeStore fakeStore = new FakeStore(getEntryMap(KEY_SET_A, KEY_SET_B));
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeLoader = new FakeCacheLoaderWriter(TEST_ENTRIES, KEY_SET_D, true);
    this.loaderWriter = spy(fakeLoader);

    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.loaderWriter);

    final Set<String> fetchKeys = fanIn(KEY_SET_A, KEY_SET_C, KEY_SET_D);
    try {
      ehcache.getAll(fetchKeys);
      fail();
    } catch (BulkCacheLoadingException e) {
      // Expected
      assertThat(e.getSuccesses(), Matchers.<Map<?,?>>equalTo(union(getEntryMap(KEY_SET_A), getEntryMap(KEY_SET_C))));
      assertThat(e.getFailures().keySet(), Matchers.<Set<?>>equalTo(KEY_SET_D));
    }

    verify(this.store).bulkComputeIfAbsent(eq(fetchKeys), getAnyIterableFunction());
    // ResilienceStrategy invoked: no assertion for Store content
    verify(this.loaderWriter, atLeast(1)).loadAll(this.loadAllCaptor.capture());
    assertThat(this.getLoadAllArgs(), equalTo(copyWithout(fetchKeys, KEY_SET_A)));
    verifyZeroInteractions(this.spiedResilienceStrategy);

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.GetOutcome.class));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoadingOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetAllOutcome.FAILURE));
    validateBulkCounters(ehcache, KEY_SET_A.size(), 0);
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#getAll(Set)} for
   * <ul>
   *    <li>non-empty request key set</li>
   *    <li>some {@link Store} entries match</li>
   *    <li>{@link Store#bulkComputeIfAbsent} throws before accessing loader</li>
   *    <li>no {@link CacheLoaderWriter} entries match</li>
   *    <li>some {@link CacheLoaderWriter#loadAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testGetAllStoreSomeMatchStoreAccessExceptionBeforeLoaderNoMatchSomeFail() throws Exception {
    final FakeStore fakeStore = new FakeStore(getEntryMap(KEY_SET_A, KEY_SET_B));
    this.store = spy(fakeStore);
    doThrow(new StoreAccessException("")).when(this.store)
        .bulkComputeIfAbsent(getAnyStringSet(), getAnyIterableFunction());

    final FakeCacheLoaderWriter fakeLoader = new FakeCacheLoaderWriter(getEntryMap(KEY_SET_F), KEY_SET_D);
    this.loaderWriter = spy(fakeLoader);

    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.loaderWriter);

    final Set<String> fetchKeys = fanIn(KEY_SET_A, KEY_SET_C, KEY_SET_D);
    try {
      ehcache.getAll(fetchKeys);
      fail();
    } catch (BulkCacheLoadingException e) {
      // Expected
    }

    final InOrder ordered = inOrder(this.loaderWriter, this.spiedResilienceStrategy);
    verify(this.store).bulkComputeIfAbsent(eq(fetchKeys), getAnyIterableFunction());
    // ResilienceStrategy invoked: no assertion for Store content
    ordered.verify(this.loaderWriter, atLeast(1)).loadAll(this.loadAllCaptor.capture());
    assertThat(this.getLoadAllArgs(), equalTo(fetchKeys));

    ordered.verify(this.spiedResilienceStrategy)
        .getAllFailure(eq(fetchKeys), any(StoreAccessException.class), this.bulkExceptionCaptor.capture());
    assertThat(this.bulkExceptionCaptor.getValue().getSuccesses().keySet(), empty());
    assertThat(this.bulkExceptionCaptor.getValue().getFailures().keySet(), Matchers.<Set<?>>equalTo(fetchKeys));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.GetOutcome.class));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoadingOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetAllOutcome.FAILURE));
    validateBulkCounters(ehcache, 0, 0);
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#getAll(Set)} for
   * <ul>
   *    <li>non-empty request key set</li>
   *    <li>some {@link Store} entries match</li>
   *    <li>{@link Store#bulkComputeIfAbsent} throws before accessing loader</li>
   *    <li>no {@link CacheLoaderWriter} entries match</li>
   *    <li>all {@link CacheLoaderWriter#loadAll(Iterable)} calls fail with {@link BulkCacheLoadingException}</li>
   * </ul>
   */
  @Test
  public void testGetAllStoreSomeMatchStoreAccessExceptionBeforeLoaderNoMatchAllFailWithBulkCacheLoadingException() throws Exception {
    final FakeStore fakeStore = new FakeStore(getEntryMap(KEY_SET_A, KEY_SET_B));
    this.store = spy(fakeStore);
    doThrow(new StoreAccessException("")).when(this.store)
        .bulkComputeIfAbsent(getAnyStringSet(), getAnyIterableFunction());

    final Set<String> fetchKeys = fanIn(KEY_SET_A, KEY_SET_C);
    final FakeCacheLoaderWriter fakeLoader = new FakeCacheLoaderWriter(TEST_ENTRIES, fetchKeys, true);
    this.loaderWriter = spy(fakeLoader);

    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.loaderWriter);

    try {
      ehcache.getAll(fetchKeys);
      fail();
    } catch (BulkCacheLoadingException e) {
      // Expected
      assertThat(e.getSuccesses().keySet(), empty());
      assertThat(e.getFailures().keySet(), Matchers.<Set<?>>equalTo(fetchKeys));
    }

    final InOrder ordered = inOrder(this.loaderWriter, this.spiedResilienceStrategy);
    verify(this.store).bulkComputeIfAbsent(eq(fetchKeys), getAnyIterableFunction());
    // ResilienceStrategy invoked: no assertion for Store content
    ordered.verify(this.loaderWriter, atLeast(1)).loadAll(this.loadAllCaptor.capture());
    assertThat(this.getLoadAllArgs(), equalTo(fetchKeys));

    ordered.verify(this.spiedResilienceStrategy)
        .getAllFailure(eq(fetchKeys), any(StoreAccessException.class), this.bulkExceptionCaptor.capture());
    verifyBulkLoadingException(this.bulkExceptionCaptor.getValue(), Collections.<String> emptySet(), fetchKeys);

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.GetOutcome.class));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoadingOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetAllOutcome.FAILURE));
    validateBulkCounters(ehcache, 0, 0);
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#getAll(Set)} for
   * <ul>
   *    <li>non-empty request key set</li>
   *    <li>some {@link Store} entries match</li>
   *    <li>{@link Store#bulkComputeIfAbsent} throws after accessing loader</li>
   *    <li>no {@link CacheLoaderWriter} entries match</li>
   *    <li>some {@link CacheLoaderWriter#loadAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testGetAllStoreSomeMatchStoreAccessExceptionAfterLoaderNoMatchSomeFail() throws Exception {
    final FakeStore fakeStore = new FakeStore(getEntryMap(KEY_SET_A, KEY_SET_B), Collections.singleton("keyA3"));
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeLoader = new FakeCacheLoaderWriter(getEntryMap(KEY_SET_A, KEY_SET_B), KEY_SET_D);
    this.loaderWriter = spy(fakeLoader);

    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.loaderWriter);

    final Set<String> fetchKeys = fanIn(KEY_SET_A, KEY_SET_C, KEY_SET_D);
    try {
      ehcache.getAll(fetchKeys);
      fail();
    } catch (BulkCacheLoadingException e) {
      // Expected
    }

    final InOrder ordered = inOrder(this.loaderWriter, this.spiedResilienceStrategy);
    verify(this.store).bulkComputeIfAbsent(eq(fetchKeys), getAnyIterableFunction());
    // ResilienceStrategy invoked: no assertion for Store content
    ordered.verify(this.loaderWriter, atLeast(1)).loadAll(this.loadAllCaptor.capture());
    assertThat(this.getLoadAllArgs(), equalTo(fetchKeys));

    ordered.verify(this.spiedResilienceStrategy)
        .getAllFailure(eq(fetchKeys), any(StoreAccessException.class), this.bulkExceptionCaptor.capture());
    final Set<String> successKeys = copyWithout(copyUntil(fetchKeys, "keyA3"), fanIn(KEY_SET_A, KEY_SET_D));
    final Set<String> failKeys = copyWithout(fetchKeys, successKeys);
    assertThat(this.bulkExceptionCaptor.getValue().getSuccesses(),
        Matchers.<Map<?, ?>>equalTo(getNullEntryMap(successKeys)));
    assertThat(this.bulkExceptionCaptor.getValue().getFailures().keySet(), Matchers.<Set<?>>equalTo(failKeys));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.GetOutcome.class));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoadingOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetAllOutcome.FAILURE));
    validateBulkCounters(ehcache, 0, 0);
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#getAll(Set)} for
   * <ul>
   *    <li>non-empty request key set</li>
   *    <li>some {@link Store} entries match</li>
   *    <li>some {@link CacheLoaderWriter} entries match</li>
   *    <li>no {@link CacheLoaderWriter#loadAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testGetAllStoreSomeMatchLoaderSomeMatchNoneFail() throws Exception {
    final FakeStore fakeStore = new FakeStore(getEntryMap(KEY_SET_A, KEY_SET_B));
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeLoader = new FakeCacheLoaderWriter(getEntryMap(KEY_SET_C, KEY_SET_F));
    this.loaderWriter = spy(fakeLoader);

    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.loaderWriter);

    final Set<String> fetchKeys = fanIn(KEY_SET_A, KEY_SET_C, KEY_SET_D);
    final Map<String, String> actual = ehcache.getAll(fetchKeys);

    assertThat(actual, equalTo(union(getEntryMap(KEY_SET_A, KEY_SET_C), getNullEntryMap(KEY_SET_D))));

    verify(this.store).bulkComputeIfAbsent(eq(fetchKeys), getAnyIterableFunction());
    assertThat(fakeStore.getEntryMap(), equalTo(getEntryMap(KEY_SET_A, KEY_SET_B, KEY_SET_C)));
    verify(this.loaderWriter, atLeast(1)).loadAll(this.loadAllCaptor.capture());
    assertThat(this.getLoadAllArgs(), equalTo(copyWithout(fetchKeys, KEY_SET_A)));
    verifyZeroInteractions(this.spiedResilienceStrategy);

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.GetOutcome.class));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoadingOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetAllOutcome.SUCCESS));
    validateBulkCounters(ehcache, KEY_SET_A.size() + KEY_SET_C.size(), KEY_SET_D.size());
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#getAll(Set)} for
   * <ul>
   *    <li>non-empty request key set</li>
   *    <li>some {@link Store} entries match</li>
   *    <li>{@link Store#bulkComputeIfAbsent} throws before accessing loader</li>
   *    <li>some {@link CacheLoaderWriter} entries match</li>
   *    <li>no {@link CacheLoaderWriter#loadAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testGetAllStoreSomeMatchStoreAccessExceptionBeforeLoaderSomeMatchNoneFail() throws Exception {
    final FakeStore fakeStore = new FakeStore(getEntryMap(KEY_SET_A, KEY_SET_B));
    this.store = spy(fakeStore);
    doThrow(new StoreAccessException("")).when(this.store)
        .bulkComputeIfAbsent(getAnyStringSet(), getAnyIterableFunction());

    final FakeCacheLoaderWriter fakeLoader = new FakeCacheLoaderWriter(getEntryMap(KEY_SET_C, KEY_SET_F));
    this.loaderWriter = spy(fakeLoader);

    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.loaderWriter);

    final Set<String> fetchKeys = fanIn(KEY_SET_A, KEY_SET_C, KEY_SET_D);
    final Map<String, String> actual = ehcache.getAll(fetchKeys);

    final Map<String, String> expected = union(getNullEntryMap(KEY_SET_A, KEY_SET_D), getEntryMap(KEY_SET_C));
    assertThat(actual, equalTo(expected));

    final InOrder ordered = inOrder(this.loaderWriter, this.spiedResilienceStrategy);
    verify(this.store).bulkComputeIfAbsent(eq(fetchKeys), getAnyIterableFunction());
    // ResilienceStrategy invoked: no assertion for Store content
    ordered.verify(this.loaderWriter, atLeast(1)).loadAll(this.loadAllCaptor.capture());
    assertThat(this.getLoadAllArgs(), equalTo(fetchKeys));
    ordered.verify(this.spiedResilienceStrategy)
        .getAllFailure(eq(fetchKeys), eq(expected), any(StoreAccessException.class));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.GetOutcome.class));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoadingOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetAllOutcome.FAILURE));
    validateBulkCounters(ehcache, 0, 0);
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#getAll(Set)} for
   * <ul>
   *    <li>non-empty request key set</li>
   *    <li>some {@link Store} entries match</li>
   *    <li>{@link Store#bulkComputeIfAbsent} throws after accessing loader</li>
   *    <li>some {@link CacheLoaderWriter} entries match</li>
   *    <li>no {@link CacheLoaderWriter#loadAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testGetAllStoreSomeMatchStoreAccessExceptionAfterLoaderSomeMatchNoneFail() throws Exception {
    final FakeStore fakeStore = new FakeStore(getEntryMap(KEY_SET_A, KEY_SET_B), Collections.singleton("keyA3"));
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeLoader = new FakeCacheLoaderWriter(getEntryMap(KEY_SET_A, KEY_SET_B, KEY_SET_C, KEY_SET_F));
    this.loaderWriter = spy(fakeLoader);

    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.loaderWriter);

    final Set<String> fetchKeys = fanIn(KEY_SET_A, KEY_SET_C, KEY_SET_D);
    final Map<String, String> actual = ehcache.getAll(fetchKeys);

    assertThat(actual, equalTo(union(getNullEntryMap(KEY_SET_D), getEntryMap(KEY_SET_A, KEY_SET_C))));

    final InOrder ordered = inOrder(this.loaderWriter, this.spiedResilienceStrategy);
    verify(this.store).bulkComputeIfAbsent(eq(fetchKeys), getAnyIterableFunction());
    // ResilienceStrategy invoked: no assertion for Store content
    ordered.verify(this.loaderWriter, atLeast(1)).loadAll(this.loadAllCaptor.capture());
    assertThat(this.getLoadAllArgs(), equalTo(fetchKeys));

    ordered.verify(this.spiedResilienceStrategy)
        .getAllFailure(eq(fetchKeys), eq(actual), any(StoreAccessException.class));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.GetOutcome.class));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoadingOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetAllOutcome.FAILURE));
    validateBulkCounters(ehcache, 0, 0);
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#getAll(Set)} for
   * <ul>
   *    <li>non-empty request key set</li>
   *    <li>some {@link Store} entries match</li>
   *    <li>some {@link CacheLoaderWriter} entries match</li>
   *    <li>non-matching {@link CacheLoaderWriter#loadAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testGetAllStoreSomeMatchLoaderSomeMatchDisjointFail() throws Exception {
    final FakeStore fakeStore = new FakeStore(getEntryMap(KEY_SET_A, KEY_SET_B));
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeLoader = new FakeCacheLoaderWriter(getEntryMap(KEY_SET_C, KEY_SET_E), KEY_SET_F);
    this.loaderWriter = spy(fakeLoader);

    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.loaderWriter);

    final Set<String> fetchKeys = fanIn(KEY_SET_A, KEY_SET_C, KEY_SET_D, KEY_SET_F);
    try {
      ehcache.getAll(fetchKeys);
      fail();
    } catch (BulkCacheLoadingException e) {
      // Expected
      assertThat(e.getSuccesses(),
          Matchers.<Map<?,?>>equalTo(union(getEntryMap(KEY_SET_A, KEY_SET_C), getNullEntryMap(KEY_SET_D))));
      assertThat(e.getFailures().keySet(), Matchers.<Set<?>>equalTo(KEY_SET_F));
    }

    verify(this.store).bulkComputeIfAbsent(eq(fetchKeys), getAnyIterableFunction());
    assertThat(fakeStore.getEntryMap(), equalTo(getEntryMap(KEY_SET_A, KEY_SET_B, KEY_SET_C)));
    verify(this.loaderWriter, atLeast(1)).loadAll(this.loadAllCaptor.capture());
    assertThat(this.getLoadAllArgs(), equalTo(copyWithout(fetchKeys, KEY_SET_A)));
    verifyZeroInteractions(this.spiedResilienceStrategy);

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.GetOutcome.class));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoadingOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetAllOutcome.FAILURE));
    validateBulkCounters(ehcache, KEY_SET_A.size() + KEY_SET_C.size(), 0);
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#getAll(Set)} for
   * <ul>
   *    <li>non-empty request key set</li>
   *    <li>some {@link Store} entries match</li>
   *    <li>some {@link CacheLoaderWriter} entries match</li>
   *    <li>non-matching {@link CacheLoaderWriter#loadAll(Iterable)} calls fails with {@link BulkCacheLoadingException}</li>
   * </ul>
   */
  @Test
  public void testGetAllStoreSomeMatchLoaderSomeMatchDisjointFailWithBulkCacheLoadingException() throws Exception {
    final FakeStore fakeStore = new FakeStore(getEntryMap(KEY_SET_A, KEY_SET_B));
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeLoader = new FakeCacheLoaderWriter(TEST_ENTRIES, union(KEY_SET_D, KEY_SET_F), true);
    this.loaderWriter = spy(fakeLoader);

    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.loaderWriter);

    final Set<String> fetchKeys = fanIn(KEY_SET_A, KEY_SET_C, KEY_SET_D, KEY_SET_F);
    try {
      ehcache.getAll(fetchKeys);
      fail();
    } catch (BulkCacheLoadingException e) {
      // Expected
      assertThat(e.getSuccesses(),Matchers.<Map<?,?>>equalTo(getEntryMap(KEY_SET_A, KEY_SET_C)));
      assertThat(e.getFailures().keySet(), Matchers.<Set<?>>equalTo(union(KEY_SET_D, KEY_SET_F)));
    }

    verify(this.store).bulkComputeIfAbsent(eq(fetchKeys), getAnyIterableFunction());
    assertThat(fakeStore.getEntryMap(), equalTo(getEntryMap(KEY_SET_A, KEY_SET_B)));
    verify(this.loaderWriter, atLeast(1)).loadAll(this.loadAllCaptor.capture());
    assertThat(this.getLoadAllArgs(), equalTo(copyWithout(fetchKeys, KEY_SET_A)));
    verifyZeroInteractions(this.spiedResilienceStrategy);

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.GetOutcome.class));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoadingOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetAllOutcome.FAILURE));
    validateBulkCounters(ehcache, KEY_SET_A.size(), 0);
  }


  /**
   * Tests {@link EhcacheWithLoaderWriter#getAll(Set)} for
   * <ul>
   *    <li>non-empty request key set</li>
   *    <li>some {@link Store} entries match</li>
   *    <li>{@link Store#bulkComputeIfAbsent} throws before accessing loader</li>
   *    <li>some {@link CacheLoaderWriter} entries match</li>
   *    <li>non-matching {@link CacheLoaderWriter#loadAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testGetAllStoreSomeMatchStoreAccessExceptionBeforeLoaderSomeMatchDisjointFail() throws Exception {
    final FakeStore fakeStore = new FakeStore(getEntryMap(KEY_SET_A, KEY_SET_B));
    this.store = spy(fakeStore);
    doThrow(new StoreAccessException("")).when(this.store)
        .bulkComputeIfAbsent(getAnyStringSet(), getAnyIterableFunction());

    final FakeCacheLoaderWriter fakeLoader = new FakeCacheLoaderWriter(getEntryMap(KEY_SET_C, KEY_SET_E), KEY_SET_F);
    this.loaderWriter = spy(fakeLoader);

    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.loaderWriter);

    final Set<String> fetchKeys = fanIn(KEY_SET_A, KEY_SET_C, KEY_SET_D, KEY_SET_F);
    try {
      ehcache.getAll(fetchKeys);
      fail();
    } catch (BulkCacheLoadingException e) {
      // Expected
    }

    final InOrder ordered = inOrder(this.loaderWriter, this.spiedResilienceStrategy);
    verify(this.store).bulkComputeIfAbsent(eq(fetchKeys), getAnyIterableFunction());
    // ResilienceStrategy invoked: no assertion for Store content
    ordered.verify(this.loaderWriter, atLeast(1)).loadAll(this.loadAllCaptor.capture());
    assertThat(this.getLoadAllArgs(), equalTo(fetchKeys));

    ordered.verify(this.spiedResilienceStrategy)
        .getAllFailure(eq(fetchKeys), any(StoreAccessException.class), this.bulkExceptionCaptor.capture());
    assertThat(this.bulkExceptionCaptor.getValue().getSuccesses().keySet(), empty());
    assertThat(this.bulkExceptionCaptor.getValue().getFailures().keySet(), Matchers.<Set<?>>equalTo(fetchKeys));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.GetOutcome.class));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoadingOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetAllOutcome.FAILURE));
    validateBulkCounters(ehcache, 0, 0);
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#getAll(Set)} for
   * <ul>
   *    <li>non-empty request key set</li>
   *    <li>some {@link Store} entries match</li>
   *    <li>{@link Store#bulkComputeIfAbsent} throws before accessing loader</li>
   *    <li>some {@link CacheLoaderWriter} entries match</li>
   *    <li>non-matching {@link CacheLoaderWriter#loadAll(Iterable)} calls fail with {@link BulkCacheLoadingException}</li>
   * </ul>
   */
  @Test
  public void testGetAllStoreSomeMatchStoreAccessExceptionBeforeLoaderSomeMatchDisjointFailWithBulkCacheLoadingException() throws Exception {
    final FakeStore fakeStore = new FakeStore(getEntryMap(KEY_SET_A, KEY_SET_B));
    this.store = spy(fakeStore);
    doThrow(new StoreAccessException("")).when(this.store)
        .bulkComputeIfAbsent(getAnyStringSet(), getAnyIterableFunction());

    final FakeCacheLoaderWriter fakeLoader = new FakeCacheLoaderWriter(TEST_ENTRIES, union(KEY_SET_A, KEY_SET_C), true);
    this.loaderWriter = spy(fakeLoader);

    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.loaderWriter);

    final Set<String> fetchKeys = fanIn(KEY_SET_A, KEY_SET_C, KEY_SET_E);
    try {
      ehcache.getAll(fetchKeys);
      fail();
    } catch (BulkCacheLoadingException e) {
      // Expected
      assertThat(e.getSuccesses(), Matchers.<Map<?,?>>equalTo(getEntryMap(KEY_SET_E)));
      assertThat(e.getFailures().keySet(), Matchers.<Set<?>>equalTo(union(KEY_SET_A, KEY_SET_C)));
    }

    final InOrder ordered = inOrder(this.loaderWriter, this.spiedResilienceStrategy);
    verify(this.store).bulkComputeIfAbsent(eq(fetchKeys), getAnyIterableFunction());
    // ResilienceStrategy invoked: no assertion for Store content
    ordered.verify(this.loaderWriter, atLeast(1)).loadAll(this.loadAllCaptor.capture());
    assertThat(this.getLoadAllArgs(), equalTo(fetchKeys));

    ordered.verify(this.spiedResilienceStrategy)
        .getAllFailure(eq(fetchKeys), any(StoreAccessException.class), this.bulkExceptionCaptor.capture());
    verifyBulkLoadingException(this.bulkExceptionCaptor.getValue(), KEY_SET_E, union(KEY_SET_A, KEY_SET_C));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.GetOutcome.class));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoadingOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetAllOutcome.FAILURE));
    validateBulkCounters(ehcache, 0, 0);
  }


  /**
   * Tests {@link EhcacheWithLoaderWriter#getAll(Set)} for
   * <ul>
   *    <li>non-empty request key set</li>
   *    <li>some {@link Store} entries match</li>
   *    <li>{@link Store#bulkComputeIfAbsent} throws after accessing loader</li>
   *    <li>some {@link CacheLoaderWriter} entries match</li>
   *    <li>non-matching {@link CacheLoaderWriter#loadAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testGetAllStoreSomeMatchStoreAccessExceptionAfterLoaderSomeMatchDisjointFail() throws Exception {
    final FakeStore fakeStore = new FakeStore(getEntryMap(KEY_SET_A, KEY_SET_B), Collections.singleton("keyA3"));
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeLoader = new FakeCacheLoaderWriter(getEntryMap(KEY_SET_A, KEY_SET_B, KEY_SET_C, KEY_SET_E), KEY_SET_F);
    this.loaderWriter = spy(fakeLoader);

    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.loaderWriter);

    final Set<String> fetchKeys = fanIn(KEY_SET_A, KEY_SET_C, KEY_SET_D, KEY_SET_F);
    try {
      ehcache.getAll(fetchKeys);
      fail();
    } catch (BulkCacheLoadingException e) {
      // Expected
    }

    final InOrder ordered = inOrder(this.loaderWriter, this.spiedResilienceStrategy);
    verify(this.store).bulkComputeIfAbsent(eq(fetchKeys), getAnyIterableFunction());
    // ResilienceStrategy invoked: no assertion for Store content
    ordered.verify(this.loaderWriter, atLeast(1)).loadAll(this.loadAllCaptor.capture());
    assertThat(this.getLoadAllArgs(), equalTo(fetchKeys));

    ordered.verify(this.spiedResilienceStrategy)
        .getAllFailure(eq(fetchKeys), any(StoreAccessException.class), this.bulkExceptionCaptor.capture());
    final Set<String> successKeys = copyWithout(copyUntil(fetchKeys, "keyA3"), fanIn(KEY_SET_A, KEY_SET_F));
    final Set<String> failKeys = copyWithout(fetchKeys, successKeys);
    assertThat(this.bulkExceptionCaptor.getValue().getSuccesses(),
        Matchers.<Map<?,?>>equalTo(union(getEntryMap(copyOnly(KEY_SET_C, successKeys)),
            getNullEntryMap(copyOnly(KEY_SET_D, successKeys)))));
    assertThat(this.bulkExceptionCaptor.getValue().getFailures().keySet(), Matchers.<Set<?>>equalTo(failKeys));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.GetOutcome.class));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoadingOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetAllOutcome.FAILURE));
    validateBulkCounters(ehcache, 0, 0);
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#getAll(Set)} for
   * <ul>
   *    <li>non-empty request key set</li>
   *    <li>some {@link Store} entries match</li>
   *    <li>{@link Store#bulkComputeIfAbsent} throws after accessing loader</li>
   *    <li>some {@link CacheLoaderWriter} entries match</li>
   *    <li>non-matching {@link CacheLoaderWriter#loadAll(Iterable)} calls fail with {@link BulkCacheLoadingException}</li>
   * </ul>
   */
  @Test
  public void testGetAllStoreSomeMatchStoreAccessExceptionAfterLoaderSomeMatchDisjointFailWithBulkCacheLoadingException() throws Exception {
    final FakeStore fakeStore = new FakeStore(getEntryMap(KEY_SET_A, KEY_SET_B), Collections.singleton("keyA3"));
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeLoader = new FakeCacheLoaderWriter(TEST_ENTRIES, union(KEY_SET_D, KEY_SET_F), true);
    this.loaderWriter = spy(fakeLoader);

    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.loaderWriter);

    final Set<String> fetchKeys = fanIn(KEY_SET_A, KEY_SET_C, KEY_SET_D, KEY_SET_F);
    try {
      ehcache.getAll(fetchKeys);
      fail();
    } catch (BulkCacheLoadingException e) {
      // Expected
      assertThat(e.getSuccesses(), Matchers.<Map<?,?>>equalTo(union(getEntryMap(KEY_SET_A), getEntryMap(KEY_SET_C))));
      assertThat(e.getFailures().keySet(), Matchers.<Set<?>>equalTo(union(KEY_SET_D, KEY_SET_F)));
    }

    final InOrder ordered = inOrder(this.loaderWriter, this.spiedResilienceStrategy);
    verify(this.store).bulkComputeIfAbsent(eq(fetchKeys), getAnyIterableFunction());
    // ResilienceStrategy invoked: no assertion for Store content
    ordered.verify(this.loaderWriter, atLeast(1)).loadAll(this.loadAllCaptor.capture());
    assertThat(this.getLoadAllArgs(), equalTo(fetchKeys));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.GetOutcome.class));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoadingOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetAllOutcome.FAILURE));
    validateBulkCounters(ehcache, 0, 0);
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#getAll(Set)} for
   * <ul>
   *    <li>non-empty request key set</li>
   *    <li>some {@link Store} entries match</li>
   *    <li>all {@link CacheLoaderWriter} entries match</li>
   *    <li>no {@link CacheLoaderWriter#loadAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testGetAllStoreSomeMatchLoaderAllMatchNoneFail() throws Exception {
    final FakeStore fakeStore = new FakeStore(getEntryMap(KEY_SET_A, KEY_SET_B));
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeLoader = new FakeCacheLoaderWriter(getEntryMap(KEY_SET_A, KEY_SET_C, KEY_SET_D));
    this.loaderWriter = spy(fakeLoader);

    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.loaderWriter);

    final Set<String> fetchKeys = fanIn(KEY_SET_A, KEY_SET_C, KEY_SET_D);
    final Map<String, String> actual = ehcache.getAll(fetchKeys);

    assertThat(actual, equalTo(getEntryMap(fetchKeys)));

    verify(this.store).bulkComputeIfAbsent(eq(fetchKeys), getAnyIterableFunction());
    assertThat(fakeStore.getEntryMap(), equalTo(getEntryMap(KEY_SET_A, KEY_SET_B, KEY_SET_C, KEY_SET_D)));
    verify(this.loaderWriter, atLeast(1)).loadAll(this.loadAllCaptor.capture());
    assertThat(this.getLoadAllArgs(), equalTo(copyWithout(fetchKeys, KEY_SET_A)));
    verifyZeroInteractions(this.spiedResilienceStrategy);

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.GetOutcome.class));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoadingOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetAllOutcome.SUCCESS));
    validateBulkCounters(ehcache, fetchKeys.size(), 0);
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#getAll(Set)} for
   * <ul>
   *    <li>non-empty request key set</li>
   *    <li>some {@link Store} entries match</li>
   *    <li>{@link Store#bulkComputeIfAbsent} throws before accessing loader</li>
   *    <li>all {@link CacheLoaderWriter} entries match</li>
   *    <li>no {@link CacheLoaderWriter#loadAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testGetAllStoreSomeMatchStoreAccessExceptionBeforeLoaderAllMatchNoneFail() throws Exception {
    final FakeStore fakeStore = new FakeStore(getEntryMap(KEY_SET_A, KEY_SET_B));
    this.store = spy(fakeStore);
    doThrow(new StoreAccessException("")).when(this.store)
        .bulkComputeIfAbsent(getAnyStringSet(), getAnyIterableFunction());

    final FakeCacheLoaderWriter fakeLoader = new FakeCacheLoaderWriter(getEntryMap(KEY_SET_A, KEY_SET_C, KEY_SET_D));
    this.loaderWriter = spy(fakeLoader);

    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.loaderWriter);

    final Set<String> fetchKeys = fanIn(KEY_SET_A, KEY_SET_C, KEY_SET_D);
    final Map<String, String> actual = ehcache.getAll(fetchKeys);

    final Map<String, String> expected = getEntryMap(fetchKeys);
    assertThat(actual, equalTo(expected));

    final InOrder ordered = inOrder(this.loaderWriter, this.spiedResilienceStrategy);
    verify(this.store).bulkComputeIfAbsent(eq(fetchKeys), getAnyIterableFunction());
    // ResilienceStrategy invoked: no assertion for Store content
    ordered.verify(this.loaderWriter, atLeast(1)).loadAll(this.loadAllCaptor.capture());
    assertThat(this.getLoadAllArgs(), equalTo(fetchKeys));
    ordered.verify(this.spiedResilienceStrategy)
        .getAllFailure(eq(fetchKeys), eq(expected), any(StoreAccessException.class));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.GetOutcome.class));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoadingOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetAllOutcome.FAILURE));
    validateBulkCounters(ehcache, 0, 0);
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#getAll(Set)} for
   * <ul>
   *    <li>non-empty request key set</li>
   *    <li>some {@link Store} entries match</li>
   *    <li>{@link Store#bulkComputeIfAbsent} throws after accessing loader</li>
   *    <li>all {@link CacheLoaderWriter} entries match</li>
   *    <li>no {@link CacheLoaderWriter#loadAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testGetAllStoreSomeMatchStoreAccessExceptionAfterLoaderAllMatchNoneFail() throws Exception {
    final FakeStore fakeStore = new FakeStore(getEntryMap(KEY_SET_A, KEY_SET_B), Collections.singleton("keyA3"));
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeLoader = new FakeCacheLoaderWriter(getEntryMap(KEY_SET_A, KEY_SET_C, KEY_SET_D));
    this.loaderWriter = spy(fakeLoader);

    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.loaderWriter);

    final Set<String> fetchKeys = fanIn(KEY_SET_A, KEY_SET_C, KEY_SET_D);
    final Map<String, String> actual = ehcache.getAll(fetchKeys);

    final Map<String, String> expected = getEntryMap(fetchKeys);
    assertThat(actual, equalTo(expected));

    final InOrder ordered = inOrder(this.loaderWriter, this.spiedResilienceStrategy);
    verify(this.store).bulkComputeIfAbsent(eq(fetchKeys), getAnyIterableFunction());
    // ResilienceStrategy invoked: no assertion for Store content
    ordered.verify(this.loaderWriter, atLeast(1)).loadAll(this.loadAllCaptor.capture());
    assertThat(this.getLoadAllArgs(), equalTo(fetchKeys));
    ordered.verify(this.spiedResilienceStrategy)
        .getAllFailure(eq(fetchKeys), eq(expected), any(StoreAccessException.class));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.GetOutcome.class));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoadingOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetAllOutcome.FAILURE));
    validateBulkCounters(ehcache, 0, 0);
  }





  /**
   * Tests {@link EhcacheWithLoaderWriter#getAll(Set)} for
   * <ul>
   *    <li>non-empty request key set</li>
   *    <li>all {@link Store} entries match</li>
   *    <li>with a {@code CacheLoaderWriter} (loader-provided entries not relevant)</li>
   *    <li>all {@link CacheLoaderWriter#loadAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testGetAllStoreAllMatchLoaderAllFail() throws Exception {
    final FakeStore fakeStore = new FakeStore(getEntryMap(KEY_SET_A, KEY_SET_B));
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeLoader = new FakeCacheLoaderWriter(TEST_ENTRIES);
    this.loaderWriter = spy(fakeLoader);
    doThrow(new Exception("loadAll failed")).when(this.loaderWriter).loadAll(getAnyStringSet());

    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.loaderWriter);

    final Set<String> fetchKeys = fanIn(KEY_SET_A, KEY_SET_B);
    final Map<String, String> actual = ehcache.getAll(fetchKeys);

    assertThat(actual, equalTo(getEntryMap(fetchKeys)));

    verify(this.store).bulkComputeIfAbsent(eq(fetchKeys), getAnyIterableFunction());
    assertThat(fakeStore.getEntryMap(), equalTo(getEntryMap(KEY_SET_A, KEY_SET_B)));
    verify(this.loaderWriter, never()).loadAll(getAnyStringSet());
    verifyZeroInteractions(this.spiedResilienceStrategy);

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.GetOutcome.class));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoadingOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetAllOutcome.SUCCESS));
    validateBulkCounters(ehcache, fetchKeys.size(), 0);
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#getAll(Set)} for
   * <ul>
   *    <li>non-empty request key set</li>
   *    <li>all {@link Store} entries match</li>
   *    <li>{@link Store#bulkComputeIfAbsent} throws before accessing loader</li>
   *    <li>with a {@code CacheLoaderWriter} (loader-provided entries not relevant)</li>
   *    <li>all {@link CacheLoaderWriter#loadAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testGetAllStoreAllMatchStoreAccessExceptionBeforeLoaderAllFail() throws Exception {
    final FakeStore fakeStore = new FakeStore(getEntryMap(KEY_SET_A, KEY_SET_B));
    this.store = spy(fakeStore);
    doThrow(new StoreAccessException("")).when(this.store)
        .bulkComputeIfAbsent(getAnyStringSet(), getAnyIterableFunction());

    final FakeCacheLoaderWriter fakeLoader = new FakeCacheLoaderWriter(TEST_ENTRIES);
    this.loaderWriter = spy(fakeLoader);
    doThrow(new Exception("loadAll failed")).when(this.loaderWriter).loadAll(getAnyStringSet());

    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.loaderWriter);

    final Set<String> fetchKeys = fanIn(KEY_SET_A, KEY_SET_B);
    try {
      ehcache.getAll(fetchKeys);
      fail();
    } catch (BulkCacheLoadingException e) {
      // Expected
    }

    final InOrder ordered = inOrder(this.loaderWriter, this.spiedResilienceStrategy);
    verify(this.store).bulkComputeIfAbsent(eq(fetchKeys), getAnyIterableFunction());
    // ResilienceStrategy invoked: no assertion for Store content
    ordered.verify(this.loaderWriter, atLeast(1)).loadAll(this.loadAllCaptor.capture());
    assertThat(this.getLoadAllArgs(), equalTo(fetchKeys));

    ordered.verify(this.spiedResilienceStrategy)
        .getAllFailure(eq(fetchKeys), any(StoreAccessException.class), this.bulkExceptionCaptor.capture());
    assertThat(this.bulkExceptionCaptor.getValue().getSuccesses().keySet(), empty());
    assertThat(this.bulkExceptionCaptor.getValue().getFailures().keySet(), Matchers.<Set<?>>equalTo(fetchKeys));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.GetOutcome.class));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoadingOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetAllOutcome.FAILURE));
    validateBulkCounters(ehcache, 0, 0);
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#getAll(Set)} for
   * <ul>
   *    <li>non-empty request key set</li>
   *    <li>all {@link Store} entries match</li>
   *    <li>{@link Store#bulkComputeIfAbsent} throws before accessing loader</li>
   *    <li>with a {@code CacheLoaderWriter} (loader-provided entries not relevant)</li>
   *    <li>all {@link CacheLoaderWriter#loadAll(Iterable)} calls fail with {@link BulkCacheLoadingException}</li>
   * </ul>
   */
  @Test
  public void testGetAllStoreAllMatchStoreAccessExceptionBeforeLoaderAllFailWithBulkCacheLoadingException() throws Exception {
    final FakeStore fakeStore = new FakeStore(getEntryMap(KEY_SET_A, KEY_SET_B));
    this.store = spy(fakeStore);
    doThrow(new StoreAccessException("")).when(this.store)
        .bulkComputeIfAbsent(getAnyStringSet(), getAnyIterableFunction());

    final Set<String> fetchKeys = fanIn(KEY_SET_A, KEY_SET_B);
    final FakeCacheLoaderWriter fakeLoader = new FakeCacheLoaderWriter(TEST_ENTRIES, fetchKeys, true);
    this.loaderWriter = spy(fakeLoader);

    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.loaderWriter);
    try {
      ehcache.getAll(fetchKeys);
      fail();
    } catch (BulkCacheLoadingException e) {
      // Expected
      assertThat(e.getSuccesses().keySet(), empty());
      assertThat(e.getFailures().keySet(), Matchers.<Set<?>>equalTo(union(KEY_SET_A, KEY_SET_B)));

    }

    final InOrder ordered = inOrder(this.loaderWriter, this.spiedResilienceStrategy);
    verify(this.store).bulkComputeIfAbsent(eq(fetchKeys), getAnyIterableFunction());
    // ResilienceStrategy invoked: no assertion for Store content
    ordered.verify(this.loaderWriter, atLeast(1)).loadAll(this.loadAllCaptor.capture());
    assertThat(this.getLoadAllArgs(), equalTo(fetchKeys));

    ordered.verify(this.spiedResilienceStrategy)
        .getAllFailure(eq(fetchKeys), any(StoreAccessException.class), this.bulkExceptionCaptor.capture());
    verifyBulkLoadingException(this.bulkExceptionCaptor.getValue(), Collections.<String> emptySet(), fetchKeys);

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.GetOutcome.class));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoadingOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetAllOutcome.FAILURE));
    validateBulkCounters(ehcache, 0, 0);
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#getAll(Set)} for
   * <ul>
   *    <li>non-empty request key set</li>
   *    <li>all {@link Store} entries match</li>
   *    <li>{@link Store#bulkComputeIfAbsent} throws before accessing loader</li>
   *    <li>with a {@code CacheLoaderWriter} (loader-provided entries not relevant)</li>
   *    <li>some {@link CacheLoaderWriter#loadAll(Iterable)} calls fail with {@link BulkCacheLoadingException}</li>
   * </ul>
   */
  @Test
  public void testGetAllStoreAllMatchStoreAccessExceptionBeforeLoaderSomeFailWithBulkCacheLoadingException() throws Exception {
    final FakeStore fakeStore = new FakeStore(getEntryMap(KEY_SET_A, KEY_SET_B));
    this.store = spy(fakeStore);
    doThrow(new StoreAccessException("")).when(this.store)
        .bulkComputeIfAbsent(getAnyStringSet(), getAnyIterableFunction());

    final FakeCacheLoaderWriter fakeLoader = new FakeCacheLoaderWriter(TEST_ENTRIES, KEY_SET_A, true);
    this.loaderWriter = spy(fakeLoader);

    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.loaderWriter);

    final Set<String> fetchKeys = fanIn(KEY_SET_A, KEY_SET_B);
    try {
      ehcache.getAll(fetchKeys);
      fail();
    } catch (BulkCacheLoadingException e) {
      // Expected
      assertThat(e.getSuccesses().keySet(), Matchers.<Set<?>>equalTo(KEY_SET_B));
      assertThat(e.getFailures().keySet(), Matchers.<Set<?>>equalTo(KEY_SET_A));

    }

    final InOrder ordered = inOrder(this.loaderWriter, this.spiedResilienceStrategy);
    verify(this.store).bulkComputeIfAbsent(eq(fetchKeys), getAnyIterableFunction());
    // ResilienceStrategy invoked: no assertion for Store content
    ordered.verify(this.loaderWriter, atLeast(1)).loadAll(this.loadAllCaptor.capture());
    assertThat(this.getLoadAllArgs(), equalTo(fetchKeys));

    ordered.verify(this.spiedResilienceStrategy)
        .getAllFailure(eq(fetchKeys), any(StoreAccessException.class), this.bulkExceptionCaptor.capture());
    verifyBulkLoadingException(this.bulkExceptionCaptor.getValue(), KEY_SET_B, KEY_SET_A);

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.GetOutcome.class));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoadingOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetAllOutcome.FAILURE));
    validateBulkCounters(ehcache, 0, 0);
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#getAll(Set)} for
   * <ul>
   *    <li>non-empty request key set</li>
   *    <li>all {@link Store} entries match</li>
   *    <li>{@link Store#bulkComputeIfAbsent} throws after accessing loader</li>
   *    <li>with a {@code CacheLoaderWriter} (loader-provided entries not relevant)</li>
   *    <li>all {@link CacheLoaderWriter#loadAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testGetAllStoreAllMatchStoreAccessExceptionAfterLoaderAllFail() throws Exception {
    final FakeStore fakeStore = new FakeStore(getEntryMap(KEY_SET_A, KEY_SET_B), Collections.singleton("keyA3"));
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeLoader = new FakeCacheLoaderWriter(TEST_ENTRIES);
    this.loaderWriter = spy(fakeLoader);
    doThrow(new Exception("loadAll failed")).when(this.loaderWriter).loadAll(getAnyStringSet());

    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.loaderWriter);

    final Set<String> fetchKeys = fanIn(KEY_SET_A, KEY_SET_B);
    try {
      ehcache.getAll(fetchKeys);
      fail();
    } catch (BulkCacheLoadingException e) {
      // Expected
    }

    final InOrder ordered = inOrder(this.loaderWriter, this.spiedResilienceStrategy);
    verify(this.store).bulkComputeIfAbsent(eq(fetchKeys), getAnyIterableFunction());
    // ResilienceStrategy invoked: no assertion for Store content
    ordered.verify(this.loaderWriter, atLeast(1)).loadAll(this.loadAllCaptor.capture());
    assertThat(this.getLoadAllArgs(), equalTo(fetchKeys));

    ordered.verify(this.spiedResilienceStrategy)
        .getAllFailure(eq(fetchKeys), any(StoreAccessException.class), this.bulkExceptionCaptor.capture());
    assertThat(this.bulkExceptionCaptor.getValue().getSuccesses().keySet(), empty());
    assertThat(this.bulkExceptionCaptor.getValue().getFailures().keySet(), Matchers.<Set<?>>equalTo(fetchKeys));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.GetOutcome.class));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoadingOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetAllOutcome.FAILURE));
    validateBulkCounters(ehcache, 0, 0);
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#getAll(Set)} for
   * <ul>
   *    <li>non-empty request key set</li>
   *    <li>all {@link Store} entries match</li>
   *    <li>{@link Store#bulkComputeIfAbsent} throws after accessing loader</li>
   *    <li>with a {@code CacheLoaderWriter} (loader-provided entries not relevant)</li>
   *    <li>all {@link CacheLoaderWriter#loadAll(Iterable)} calls fail with {@link BulkCacheLoadingException}</li>
   * </ul>
   */
  @Test
  public void testGetAllStoreAllMatchStoreAccessExceptionAfterLoaderAllFailWithBulkCacheLoadingException() throws Exception {
    final FakeStore fakeStore = new FakeStore(getEntryMap(KEY_SET_A, KEY_SET_B), Collections.singleton("keyA3"));
    this.store = spy(fakeStore);

    final Set<String> fetchKeys = fanIn(KEY_SET_A, KEY_SET_B);
    final FakeCacheLoaderWriter fakeLoader = new FakeCacheLoaderWriter(TEST_ENTRIES, fetchKeys, true);
    this.loaderWriter = spy(fakeLoader);

    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.loaderWriter);
    try {
      ehcache.getAll(fetchKeys);
      fail();
    } catch (BulkCacheLoadingException e) {
      // Expected
      assertThat(e.getSuccesses().keySet(), empty());
      assertThat(e.getFailures().keySet(), Matchers.<Set<?>>equalTo(fetchKeys));
    }

    final InOrder ordered = inOrder(this.loaderWriter, this.spiedResilienceStrategy);
    verify(this.store).bulkComputeIfAbsent(eq(fetchKeys), getAnyIterableFunction());
    // ResilienceStrategy invoked: no assertion for Store content
    ordered.verify(this.loaderWriter, atLeast(1)).loadAll(this.loadAllCaptor.capture());
    assertThat(this.getLoadAllArgs(), equalTo(fetchKeys));

    ordered.verify(this.spiedResilienceStrategy)
        .getAllFailure(eq(fetchKeys), any(StoreAccessException.class), this.bulkExceptionCaptor.capture());
    verifyBulkLoadingException(this.bulkExceptionCaptor.getValue(), Collections.<String> emptySet(), fetchKeys);

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.GetOutcome.class));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoadingOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetAllOutcome.FAILURE));
    validateBulkCounters(ehcache, 0, 0);
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#getAll(Set)} for
   * <ul>
   *    <li>non-empty request key set</li>
   *    <li>all {@link Store} entries match</li>
   *    <li>{@link Store#bulkComputeIfAbsent} throws after accessing loader</li>
   *    <li>with a {@code CacheLoaderWriter} (loader-provided entries not relevant)</li>
   *    <li>some {@link CacheLoaderWriter#loadAll(Iterable)} calls fail with {@link BulkCacheLoadingException}</li>
   * </ul>
   */
  @Test
  public void testGetAllStoreAllMatchStoreAccessExceptionAfterLoaderSomeFailWithBulkCacheLoadingException() throws Exception {
    final FakeStore fakeStore = new FakeStore(getEntryMap(KEY_SET_A, KEY_SET_B), Collections.singleton("keyA3"));
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeLoader = new FakeCacheLoaderWriter(TEST_ENTRIES, KEY_SET_A, true);
    this.loaderWriter = spy(fakeLoader);

    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.loaderWriter);

    final Set<String> fetchKeys = fanIn(KEY_SET_A, KEY_SET_B);
    try {
      ehcache.getAll(fetchKeys);
      fail();
    } catch (BulkCacheLoadingException e) {
      // Expected
      assertThat(e.getSuccesses().keySet(), Matchers.<Set<?>>equalTo(KEY_SET_B));
      assertThat(e.getFailures().keySet(), Matchers.<Set<?>>equalTo(KEY_SET_A));
    }

    final InOrder ordered = inOrder(this.loaderWriter, this.spiedResilienceStrategy);
    verify(this.store).bulkComputeIfAbsent(eq(fetchKeys), getAnyIterableFunction());
    // ResilienceStrategy invoked: no assertion for Store content
    ordered.verify(this.loaderWriter, atLeast(1)).loadAll(this.loadAllCaptor.capture());
    assertThat(this.getLoadAllArgs(), equalTo(fetchKeys));

    ordered.verify(this.spiedResilienceStrategy)
        .getAllFailure(eq(fetchKeys), any(StoreAccessException.class), this.bulkExceptionCaptor.capture());
    verifyBulkLoadingException(this.bulkExceptionCaptor.getValue(), KEY_SET_B, KEY_SET_A);

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.GetOutcome.class));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoadingOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetAllOutcome.FAILURE));
    validateBulkCounters(ehcache, 0, 0);
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#getAll(Set)} for
   * <ul>
   *    <li>non-empty request key set</li>
   *    <li>all {@link Store} entries match</li>
   *    <li>no {@link CacheLoaderWriter} entries match</li>
   *    <li>no {@link CacheLoaderWriter#loadAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testGetAllStoreAllMatchLoaderNoMatchNoneFail() throws Exception {
    final FakeStore fakeStore = new FakeStore(getEntryMap(KEY_SET_A, KEY_SET_B));
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeLoader = new FakeCacheLoaderWriter(getEntryMap(KEY_SET_C));
    this.loaderWriter = spy(fakeLoader);

    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.loaderWriter);

    final Set<String> fetchKeys = fanIn(KEY_SET_A, KEY_SET_B);
    final Map<String, String> actual = ehcache.getAll(fetchKeys);

    assertThat(actual, equalTo(getEntryMap(fetchKeys)));

    verify(this.store).bulkComputeIfAbsent(eq(fetchKeys), getAnyIterableFunction());
    assertThat(fakeStore.getEntryMap(), equalTo(getEntryMap(KEY_SET_A, KEY_SET_B)));
    verify(this.loaderWriter, never()).loadAll(getAnyStringSet());
    verifyZeroInteractions(this.spiedResilienceStrategy);

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.GetOutcome.class));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoadingOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetAllOutcome.SUCCESS));
    validateBulkCounters(ehcache, fetchKeys.size(), 0);
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#getAll(Set)} for
   * <ul>
   *    <li>non-empty request key set</li>
   *    <li>all {@link Store} entries match</li>
   *    <li>{@link Store#bulkComputeIfAbsent} throws before accessing loader</li>
   *    <li>no {@link CacheLoaderWriter} entries match</li>
   *    <li>no {@link CacheLoaderWriter#loadAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testGetAllStoreAllMatchStoreAccessExceptionBeforeLoaderNoMatchNoneFail() throws Exception {
    final FakeStore fakeStore = new FakeStore(getEntryMap(KEY_SET_A, KEY_SET_B));
    this.store = spy(fakeStore);
    doThrow(new StoreAccessException("")).when(this.store)
        .bulkComputeIfAbsent(getAnyStringSet(), getAnyIterableFunction());

    final FakeCacheLoaderWriter fakeLoader = new FakeCacheLoaderWriter(getEntryMap(KEY_SET_C));
    this.loaderWriter = spy(fakeLoader);

    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.loaderWriter);

    final Set<String> fetchKeys = fanIn(KEY_SET_A, KEY_SET_B);
    final Map<String, String> actual = ehcache.getAll(fetchKeys);

    final Map<String, String> expected = getNullEntryMap(fetchKeys);
    assertThat(actual, equalTo(expected));

    final InOrder ordered = inOrder(this.loaderWriter, this.spiedResilienceStrategy);
    verify(this.store).bulkComputeIfAbsent(eq(fetchKeys), getAnyIterableFunction());
    // ResilienceStrategy invoked: no assertion for Store content
    ordered.verify(this.loaderWriter, atLeast(1)).loadAll(this.loadAllCaptor.capture());
    assertThat(this.getLoadAllArgs(), equalTo(fetchKeys));
    ordered.verify(this.spiedResilienceStrategy)
        .getAllFailure(eq(fetchKeys), eq(expected), any(StoreAccessException.class));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.GetOutcome.class));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoadingOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetAllOutcome.FAILURE));
    validateBulkCounters(ehcache, 0, 0);
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#getAll(Set)} for
   * <ul>
   *    <li>non-empty request key set</li>
   *    <li>all {@link Store} entries match</li>
   *    <li>no {@link CacheLoaderWriter} entries match</li>
   *    <li>some {@link CacheLoaderWriter#loadAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testGetAllStoreAllMatchLoaderNoMatchSomeFail() throws Exception {
    final FakeStore fakeStore = new FakeStore(getEntryMap(KEY_SET_A, KEY_SET_B));
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeLoader = new FakeCacheLoaderWriter(getEntryMap(KEY_SET_C, KEY_SET_F), KEY_SET_B);
    this.loaderWriter = spy(fakeLoader);

    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.loaderWriter);

    final Set<String> fetchKeys = fanIn(KEY_SET_A, KEY_SET_B);
    final Map<String, String> actual = ehcache.getAll(fetchKeys);

    assertThat(actual, equalTo(getEntryMap(fetchKeys)));

    verify(this.store).bulkComputeIfAbsent(eq(fetchKeys), getAnyIterableFunction());
    assertThat(fakeStore.getEntryMap(), equalTo(getEntryMap(KEY_SET_A, KEY_SET_B)));
    verify(this.loaderWriter, never()).loadAll(getAnyStringSet());
    verifyZeroInteractions(this.spiedResilienceStrategy);

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.GetOutcome.class));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoadingOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetAllOutcome.SUCCESS));
    validateBulkCounters(ehcache, fetchKeys.size(), 0);
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#getAll(Set)} for
   * <ul>
   *    <li>non-empty request key set</li>
   *    <li>all {@link Store} entries match</li>
   *    <li>{@link Store#bulkComputeIfAbsent} throws before accessing loader</li>
   *    <li>no {@link CacheLoaderWriter} entries match</li>
   *    <li>some {@link CacheLoaderWriter#loadAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testGetAllStoreAllMatchStoreAccessExceptionBeforeLoaderNoMatchSomeFail() throws Exception {
    final FakeStore fakeStore = new FakeStore(getEntryMap(KEY_SET_A, KEY_SET_B));
    this.store = spy(fakeStore);
    doThrow(new StoreAccessException("")).when(this.store)
        .bulkComputeIfAbsent(getAnyStringSet(), getAnyIterableFunction());

    final FakeCacheLoaderWriter fakeLoader = new FakeCacheLoaderWriter(getEntryMap(KEY_SET_C, KEY_SET_F), KEY_SET_B);
    this.loaderWriter = spy(fakeLoader);

    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.loaderWriter);

    final Set<String> fetchKeys = fanIn(KEY_SET_A, KEY_SET_B);
    try {
      ehcache.getAll(fetchKeys);
      fail();
    } catch (BulkCacheLoadingException e) {
      // Expected
    }

    final InOrder ordered = inOrder(this.loaderWriter, this.spiedResilienceStrategy);
    verify(this.store).bulkComputeIfAbsent(eq(fetchKeys), getAnyIterableFunction());
    // ResilienceStrategy invoked: no assertion for Store content
    ordered.verify(this.loaderWriter, atLeast(1)).loadAll(this.loadAllCaptor.capture());
    assertThat(this.getLoadAllArgs(), equalTo(fetchKeys));

    ordered.verify(this.spiedResilienceStrategy)
        .getAllFailure(eq(fetchKeys), any(StoreAccessException.class), this.bulkExceptionCaptor.capture());
    assertThat(this.bulkExceptionCaptor.getValue().getSuccesses().keySet(), empty());
    assertThat(this.bulkExceptionCaptor.getValue().getFailures().keySet(), Matchers.<Set<?>>equalTo(fetchKeys));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.GetOutcome.class));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoadingOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetAllOutcome.FAILURE));
    validateBulkCounters(ehcache, 0, 0);
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#getAll(Set)} for
   * <ul>
   *    <li>non-empty request key set</li>
   *    <li>all {@link Store} entries match</li>
   *    <li>{@link Store#bulkComputeIfAbsent} throws before accessing loader</li>
   *    <li>no {@link CacheLoaderWriter} entries match</li>
   *    <li>some {@link CacheLoaderWriter#loadAll(Iterable)} calls fail with {@link BulkCacheLoadingException}</li>
   * </ul>
   */
  @Test
  public void testGetAllStoreAllMatchStoreAccessExceptionBeforeLoaderNoMatchSomeFailWithBulkCacheLoadingException() throws Exception {
    final FakeStore fakeStore = new FakeStore(getEntryMap(KEY_SET_A, KEY_SET_B));
    this.store = spy(fakeStore);
    doThrow(new StoreAccessException("")).when(this.store)
        .bulkComputeIfAbsent(getAnyStringSet(), getAnyIterableFunction());

    final Set<String> fetchKeys = fanIn(KEY_SET_A, KEY_SET_B);
    final FakeCacheLoaderWriter fakeLoader = new FakeCacheLoaderWriter(TEST_ENTRIES, fetchKeys, true);
    this.loaderWriter = spy(fakeLoader);

    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.loaderWriter);
    try {
      ehcache.getAll(fetchKeys);
      fail();
    } catch (BulkCacheLoadingException e) {
      // Expected
      assertThat(e.getSuccesses().keySet(), empty());
      assertThat(e.getFailures().keySet(), Matchers.<Set<?>>equalTo(fetchKeys));
    }

    final InOrder ordered = inOrder(this.loaderWriter, this.spiedResilienceStrategy);
    verify(this.store).bulkComputeIfAbsent(eq(fetchKeys), getAnyIterableFunction());
    // ResilienceStrategy invoked: no assertion for Store content
    ordered.verify(this.loaderWriter, atLeast(1)).loadAll(this.loadAllCaptor.capture());
    assertThat(this.getLoadAllArgs(), equalTo(fetchKeys));

    ordered.verify(this.spiedResilienceStrategy)
        .getAllFailure(eq(fetchKeys), any(StoreAccessException.class), this.bulkExceptionCaptor.capture());
    verifyBulkLoadingException(this.bulkExceptionCaptor.getValue(), Collections.<String> emptySet(), fetchKeys);

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.GetOutcome.class));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoadingOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetAllOutcome.FAILURE));
    validateBulkCounters(ehcache, 0, 0);
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#getAll(Set)} for
   * <ul>
   *    <li>non-empty request key set</li>
   *    <li>all {@link Store} entries match</li>
   *    <li>{@link Store#bulkComputeIfAbsent} throws after accessing loader</li>
   *    <li>no {@link CacheLoaderWriter} entries match</li>
   *    <li>some {@link CacheLoaderWriter#loadAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testGetAllStoreAllMatchStoreAccessExceptionAfterLoaderNoMatchSomeFail() throws Exception {
    final FakeStore fakeStore = new FakeStore(getEntryMap(KEY_SET_A, KEY_SET_B), Collections.singleton("keyA3"));
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeLoader = new FakeCacheLoaderWriter(getEntryMap(KEY_SET_A, KEY_SET_C, KEY_SET_F), KEY_SET_B);
    this.loaderWriter = spy(fakeLoader);

    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.loaderWriter);

    final Set<String> fetchKeys = fanIn(KEY_SET_A, KEY_SET_B);
    try {
      ehcache.getAll(fetchKeys);
      fail();
    } catch (BulkCacheLoadingException e) {
      // Expected
    }

    final InOrder ordered = inOrder(this.loaderWriter, this.spiedResilienceStrategy);
    verify(this.store).bulkComputeIfAbsent(eq(fetchKeys), getAnyIterableFunction());
    // ResilienceStrategy invoked: no assertion for Store content
    ordered.verify(this.loaderWriter, atLeast(1)).loadAll(this.loadAllCaptor.capture());
    assertThat(this.getLoadAllArgs(), equalTo(fetchKeys));

    ordered.verify(this.spiedResilienceStrategy)
        .getAllFailure(eq(fetchKeys), any(StoreAccessException.class), this.bulkExceptionCaptor.capture());
    assertThat(this.bulkExceptionCaptor.getValue().getSuccesses().keySet(), empty());
    assertThat(this.bulkExceptionCaptor.getValue().getFailures().keySet(), Matchers.<Set<?>>equalTo(fetchKeys));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.GetOutcome.class));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoadingOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetAllOutcome.FAILURE));
    validateBulkCounters(ehcache, 0, 0);
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#getAll(Set)} for
   * <ul>
   *    <li>non-empty request key set</li>
   *    <li>all {@link Store} entries match</li>
   *    <li>some {@link CacheLoaderWriter} entries match</li>
   *    <li>no {@link CacheLoaderWriter#loadAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testGetAllStoreAllMatchLoaderSomeMatchNoneFail() throws Exception {
    final FakeStore fakeStore = new FakeStore(getEntryMap(KEY_SET_A, KEY_SET_B));
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeLoader = new FakeCacheLoaderWriter(getEntryMap(KEY_SET_B, KEY_SET_C));
    this.loaderWriter = spy(fakeLoader);

    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.loaderWriter);

    final Set<String> fetchKeys = fanIn(KEY_SET_A, KEY_SET_B);
    final Map<String, String> actual = ehcache.getAll(fetchKeys);

    assertThat(actual, equalTo(getEntryMap(fetchKeys)));

    verify(this.store).bulkComputeIfAbsent(eq(fetchKeys), getAnyIterableFunction());
    assertThat(fakeStore.getEntryMap(), equalTo(getEntryMap(KEY_SET_A, KEY_SET_B)));
    verify(this.loaderWriter, never()).loadAll(getAnyStringSet());
    verifyZeroInteractions(this.spiedResilienceStrategy);

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.GetOutcome.class));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoadingOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetAllOutcome.SUCCESS));
    validateBulkCounters(ehcache, fetchKeys.size(), 0);
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#getAll(Set)} for
   * <ul>
   *    <li>non-empty request key set</li>
   *    <li>all {@link Store} entries match</li>
   *    <li>{@link Store#bulkComputeIfAbsent} throws before accessing loader</li>
   *    <li>some {@link CacheLoaderWriter} entries match</li>
   *    <li>no {@link CacheLoaderWriter#loadAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testGetAllStoreAllMatchStoreAccessExceptionBeforeLoaderSomeMatchNoneFail() throws Exception {
    final FakeStore fakeStore = new FakeStore(getEntryMap(KEY_SET_A, KEY_SET_B));
    this.store = spy(fakeStore);
    doThrow(new StoreAccessException("")).when(this.store)
        .bulkComputeIfAbsent(getAnyStringSet(), getAnyIterableFunction());

    final FakeCacheLoaderWriter fakeLoader = new FakeCacheLoaderWriter(getEntryMap(KEY_SET_B, KEY_SET_C));
    this.loaderWriter = spy(fakeLoader);

    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.loaderWriter);

    final Set<String> fetchKeys = fanIn(KEY_SET_A, KEY_SET_B);
    final Map<String, String> actual = ehcache.getAll(fetchKeys);

    final Map<String, String> expected = union(getNullEntryMap(KEY_SET_A), getEntryMap(KEY_SET_B));
    assertThat(actual, equalTo(expected));

    final InOrder ordered = inOrder(this.loaderWriter, this.spiedResilienceStrategy);
    verify(this.store).bulkComputeIfAbsent(eq(fetchKeys), getAnyIterableFunction());
    // ResilienceStrategy invoked: no assertion for Store content
    ordered.verify(this.loaderWriter, atLeast(1)).loadAll(fetchKeys);
    ordered.verify(this.spiedResilienceStrategy)
        .getAllFailure(eq(fetchKeys), eq(expected), any(StoreAccessException.class));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.GetOutcome.class));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoadingOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetAllOutcome.FAILURE));
    validateBulkCounters(ehcache, 0, 0);
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#getAll(Set)} for
   * <ul>
   *    <li>non-empty request key set</li>
   *    <li>all {@link Store} entries match</li>
   *    <li>{@link Store#bulkComputeIfAbsent} throws after accessing loader</li>
   *    <li>some {@link CacheLoaderWriter} entries match</li>
   *    <li>no {@link CacheLoaderWriter#loadAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testGetAllStoreAllMatchStoreAccessExceptionAfterLoaderSomeMatchNoneFail() throws Exception {
    final FakeStore fakeStore = new FakeStore(getEntryMap(KEY_SET_A, KEY_SET_B), Collections.singleton("keyA3"));
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeLoader = new FakeCacheLoaderWriter(getEntryMap(KEY_SET_A, KEY_SET_B, KEY_SET_C));
    this.loaderWriter = spy(fakeLoader);

    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.loaderWriter);

    final Set<String> fetchKeys = fanIn(KEY_SET_A, KEY_SET_B);
    final Map<String, String> actual = ehcache.getAll(fetchKeys);

    final Map<String, String> expected = getEntryMap(KEY_SET_A, KEY_SET_B);
    assertThat(actual, equalTo(expected));

    final InOrder ordered = inOrder(this.loaderWriter, this.spiedResilienceStrategy);
    verify(this.store).bulkComputeIfAbsent(eq(fetchKeys), getAnyIterableFunction());
    // ResilienceStrategy invoked: no assertion for Store content
    ordered.verify(this.loaderWriter, atLeast(1)).loadAll(fetchKeys);
    ordered.verify(this.spiedResilienceStrategy)
        .getAllFailure(eq(fetchKeys), eq(expected), any(StoreAccessException.class));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.GetOutcome.class));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoadingOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetAllOutcome.FAILURE));
    validateBulkCounters(ehcache, 0, 0);
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#getAll(Set)} for
   * <ul>
   *    <li>non-empty request key set</li>
   *    <li>all {@link Store} entries match</li>
   *    <li>some {@link CacheLoaderWriter} entries match</li>
   *    <li>non-matching {@link CacheLoaderWriter#loadAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testGetAllStoreAllMatchLoaderSomeMatchDisjointFail() throws Exception {
    final FakeStore fakeStore = new FakeStore(getEntryMap(KEY_SET_A, KEY_SET_B));
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeLoader = new FakeCacheLoaderWriter(getEntryMap(KEY_SET_B, KEY_SET_C), KEY_SET_A);
    this.loaderWriter = spy(fakeLoader);

    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.loaderWriter);

    final Set<String> fetchKeys = fanIn(KEY_SET_A, KEY_SET_B);
    final Map<String, String> actual = ehcache.getAll(fetchKeys);

    final Map<String, String> expected = getEntryMap(KEY_SET_A, KEY_SET_B);
    assertThat(actual, equalTo(expected));

    verify(this.store).bulkComputeIfAbsent(eq(fetchKeys), getAnyIterableFunction());
    assertThat(fakeStore.getEntryMap(), equalTo(getEntryMap(KEY_SET_A, KEY_SET_B)));
    verify(this.loaderWriter, never()).loadAll(getAnyStringSet());
    verifyZeroInteractions(this.spiedResilienceStrategy);

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.GetOutcome.class));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoadingOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetAllOutcome.SUCCESS));
    validateBulkCounters(ehcache, fetchKeys.size(), 0);
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#getAll(Set)} for
   * <ul>
   *    <li>non-empty request key set</li>
   *    <li>all {@link Store} entries match</li>
   *    <li>{@link Store#bulkComputeIfAbsent} throws before accessing loader</li>
   *    <li>some {@link CacheLoaderWriter} entries match</li>
   *    <li>non-matching {@link CacheLoaderWriter#loadAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testGetAllStoreAllMatchStoreAccessExceptionBeforeLoaderSomeMatchDisjointFail() throws Exception {
    final FakeStore fakeStore = new FakeStore(getEntryMap(KEY_SET_A, KEY_SET_B));
    this.store = spy(fakeStore);
    doThrow(new StoreAccessException("")).when(this.store)
        .bulkComputeIfAbsent(getAnyStringSet(), getAnyIterableFunction());

    final FakeCacheLoaderWriter fakeLoader = new FakeCacheLoaderWriter(getEntryMap(KEY_SET_B, KEY_SET_C), KEY_SET_A);
    this.loaderWriter = spy(fakeLoader);

    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.loaderWriter);

    final Set<String> fetchKeys = fanIn(KEY_SET_A, KEY_SET_B);
    try {
      ehcache.getAll(fetchKeys);
      fail();
    } catch (BulkCacheLoadingException e) {
      // Expected
    }

    final InOrder ordered = inOrder(this.loaderWriter, this.spiedResilienceStrategy);
    verify(this.store).bulkComputeIfAbsent(eq(fetchKeys), getAnyIterableFunction());
    // ResilienceStrategy invoked: no assertion for Store content
    ordered.verify(this.loaderWriter, atLeast(1)).loadAll(this.loadAllCaptor.capture());
    assertThat(this.getLoadAllArgs(), equalTo(fetchKeys));

    ordered.verify(this.spiedResilienceStrategy)
        .getAllFailure(eq(fetchKeys), any(StoreAccessException.class), this.bulkExceptionCaptor.capture());
    assertThat(this.bulkExceptionCaptor.getValue().getSuccesses(), Matchers.<Map<?,?>>equalTo(Collections.emptyMap()));
    assertThat(this.bulkExceptionCaptor.getValue().getFailures().keySet(), Matchers.<Set<?>>equalTo(fetchKeys));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.GetOutcome.class));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoadingOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetAllOutcome.FAILURE));
    validateBulkCounters(ehcache, 0, 0);
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#getAll(Set)} for
   * <ul>
   *    <li>non-empty request key set</li>
   *    <li>all {@link Store} entries match</li>
   *    <li>{@link Store#bulkComputeIfAbsent} throws before accessing loader</li>
   *    <li>some {@link CacheLoaderWriter} entries match</li>
   *    <li>non-matching {@link CacheLoaderWriter#loadAll(Iterable)} calls fail with {@link BulkCacheLoadingException}</li>
   * </ul>
   */
  @Test
  public void testGetAllStoreAllMatchStoreAccessExceptionBeforeLoaderSomeMatchDisjointFailWithBulkCacheLoadingException() throws Exception {
    final FakeStore fakeStore = new FakeStore(getEntryMap(KEY_SET_A, KEY_SET_B));
    this.store = spy(fakeStore);
    doThrow(new StoreAccessException("")).when(this.store)
        .bulkComputeIfAbsent(getAnyStringSet(), getAnyIterableFunction());

    final FakeCacheLoaderWriter fakeLoader = new FakeCacheLoaderWriter(TEST_ENTRIES, KEY_SET_A, true);
    this.loaderWriter = spy(fakeLoader);

    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.loaderWriter);

    final Set<String> fetchKeys = fanIn(KEY_SET_A, KEY_SET_B);
    try {
      ehcache.getAll(fetchKeys);
      fail();
    } catch (BulkCacheLoadingException e) {
      // Expected
      assertThat(e.getSuccesses().keySet(), Matchers.<Set<?>>equalTo(KEY_SET_B));
      assertThat(e.getFailures().keySet(), Matchers.<Set<?>>equalTo(KEY_SET_A));

    }

    final InOrder ordered = inOrder(this.loaderWriter, this.spiedResilienceStrategy);
    verify(this.store).bulkComputeIfAbsent(eq(fetchKeys), getAnyIterableFunction());
    // ResilienceStrategy invoked: no assertion for Store content
    ordered.verify(this.loaderWriter, atLeast(1)).loadAll(this.loadAllCaptor.capture());
    assertThat(this.getLoadAllArgs(), equalTo(fetchKeys));

    ordered.verify(this.spiedResilienceStrategy)
        .getAllFailure(eq(fetchKeys), any(StoreAccessException.class), this.bulkExceptionCaptor.capture());
    verifyBulkLoadingException(this.bulkExceptionCaptor.getValue(), KEY_SET_B, KEY_SET_A);

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.GetOutcome.class));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoadingOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetAllOutcome.FAILURE));
    validateBulkCounters(ehcache, 0, 0);
  }


  /**
   * Tests {@link EhcacheWithLoaderWriter#getAll(Set)} for
   * <ul>
   *    <li>non-empty request key set</li>
   *    <li>all {@link Store} entries match</li>
   *    <li>{@link Store#bulkComputeIfAbsent} throws after accessing loader</li>
   *    <li>some {@link CacheLoaderWriter} entries match</li>
   *    <li>non-matching {@link CacheLoaderWriter#loadAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testGetAllStoreAllMatchStoreAccessExceptionAfterLoaderSomeMatchDisjointFail() throws Exception {
    final FakeStore fakeStore = new FakeStore(getEntryMap(KEY_SET_A, KEY_SET_B), Collections.singleton("keyA3"));
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeLoader = new FakeCacheLoaderWriter(getEntryMap(KEY_SET_B, KEY_SET_C), KEY_SET_A);
    this.loaderWriter = spy(fakeLoader);

    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.loaderWriter);

    final Set<String> fetchKeys = fanIn(KEY_SET_A, KEY_SET_B);
    try {
      ehcache.getAll(fetchKeys);
      fail();
    } catch (BulkCacheLoadingException e) {
      // Expected
    }

    final InOrder ordered = inOrder(this.loaderWriter, this.spiedResilienceStrategy);
    verify(this.store).bulkComputeIfAbsent(eq(fetchKeys), getAnyIterableFunction());
    // ResilienceStrategy invoked: no assertion for Store content
    ordered.verify(this.loaderWriter, atLeast(1)).loadAll(this.loadAllCaptor.capture());
    assertThat(this.getLoadAllArgs(), equalTo(fetchKeys));

    ordered.verify(this.spiedResilienceStrategy)
        .getAllFailure(eq(fetchKeys), any(StoreAccessException.class), this.bulkExceptionCaptor.capture());
    assertThat(this.bulkExceptionCaptor.getValue().getSuccesses(), Matchers.<Map<?,?>>equalTo(Collections.emptyMap()));
    assertThat(this.bulkExceptionCaptor.getValue().getFailures().keySet(), Matchers.<Set<?>>equalTo(fetchKeys));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.GetOutcome.class));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoadingOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetAllOutcome.FAILURE));
    validateBulkCounters(ehcache, 0, 0);
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#getAll(Set)} for
   * <ul>
   *    <li>non-empty request key set</li>
   *    <li>all {@link Store} entries match</li>
   *    <li>{@link Store#bulkComputeIfAbsent} throws after accessing loader</li>
   *    <li>some {@link CacheLoaderWriter} entries match</li>
   *    <li>non-matching {@link CacheLoaderWriter#loadAll(Iterable)} calls fail with {@link BulkCacheLoadingException}</li>
   * </ul>
   */
  @Test
  public void testGetAllStoreAllMatchStoreAccessExceptionAfterLoaderSomeMatchDisjointFailWithBulkCacheLoadingException() throws Exception {
    final FakeStore fakeStore = new FakeStore(getEntryMap(KEY_SET_A, KEY_SET_B), Collections.singleton("keyA3"));
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeLoader = new FakeCacheLoaderWriter(TEST_ENTRIES, KEY_SET_A, true);
    this.loaderWriter = spy(fakeLoader);

    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.loaderWriter);

    final Set<String> fetchKeys = fanIn(KEY_SET_A, KEY_SET_B);
    try {
      ehcache.getAll(fetchKeys);
      fail();
    } catch (BulkCacheLoadingException e) {
      // Expected
      assertThat(e.getSuccesses().keySet(), Matchers.<Set<?>>equalTo(KEY_SET_B));
      assertThat(e.getFailures().keySet(), Matchers.<Set<?>>equalTo(KEY_SET_A));
    }

    final InOrder ordered = inOrder(this.loaderWriter, this.spiedResilienceStrategy);
    verify(this.store).bulkComputeIfAbsent(eq(fetchKeys), getAnyIterableFunction());
    // ResilienceStrategy invoked: no assertion for Store content
    ordered.verify(this.loaderWriter, atLeast(1)).loadAll(this.loadAllCaptor.capture());
    assertThat(this.getLoadAllArgs(), equalTo(fetchKeys));

    ordered.verify(this.spiedResilienceStrategy)
        .getAllFailure(eq(fetchKeys), any(StoreAccessException.class), this.bulkExceptionCaptor.capture());
    verifyBulkLoadingException(this.bulkExceptionCaptor.getValue(), KEY_SET_B, KEY_SET_A);

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.GetOutcome.class));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoadingOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetAllOutcome.FAILURE));
    validateBulkCounters(ehcache, 0, 0);
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#getAll(Set)} for
   * <ul>
   *    <li>non-empty request key set</li>
   *    <li>all {@link Store} entries match</li>
   *    <li>all {@link CacheLoaderWriter} entries match</li>
   *    <li>no {@link CacheLoaderWriter#loadAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testGetAllStoreAllMatchLoaderAllMatchNoneFail() throws Exception {
    final FakeStore fakeStore = new FakeStore(getEntryMap(KEY_SET_A, KEY_SET_B));
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeLoader = new FakeCacheLoaderWriter(getEntryMap(KEY_SET_A, KEY_SET_B));
    this.loaderWriter = spy(fakeLoader);

    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.loaderWriter);

    final Set<String> fetchKeys = fanIn(KEY_SET_A, KEY_SET_B);
    final Map<String, String> actual = ehcache.getAll(fetchKeys);

    assertThat(actual, equalTo(getEntryMap(fetchKeys)));

    verify(this.store).bulkComputeIfAbsent(eq(fetchKeys), getAnyIterableFunction());
    assertThat(fakeStore.getEntryMap(), equalTo(getEntryMap(KEY_SET_A, KEY_SET_B)));
    verify(this.loaderWriter, never()).loadAll(getAnyStringSet());
    verifyZeroInteractions(this.spiedResilienceStrategy);

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.GetOutcome.class));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoadingOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetAllOutcome.SUCCESS));
    validateBulkCounters(ehcache, fetchKeys.size(), 0);
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#getAll(Set)} for
   * <ul>
   *    <li>non-empty request key set</li>
   *    <li>all {@link Store} entries match</li>
   *    <li>{@link Store#bulkComputeIfAbsent} throws before accessing loader</li>
   *    <li>all {@link CacheLoaderWriter} entries match</li>
   *    <li>no {@link CacheLoaderWriter#loadAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testGetAllStoreAllMatchStoreAccessExceptionBeforeLoaderAllMatchNoneFail() throws Exception {
    final FakeStore fakeStore = new FakeStore(getEntryMap(KEY_SET_A, KEY_SET_B));
    this.store = spy(fakeStore);
    doThrow(new StoreAccessException("")).when(this.store)
        .bulkComputeIfAbsent(getAnyStringSet(), getAnyIterableFunction());

    final FakeCacheLoaderWriter fakeLoader = new FakeCacheLoaderWriter(getEntryMap(KEY_SET_A, KEY_SET_B));
    this.loaderWriter = spy(fakeLoader);

    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.loaderWriter);

    final Set<String> fetchKeys = fanIn(KEY_SET_A, KEY_SET_B);
    final Map<String, String> actual = ehcache.getAll(fetchKeys);


    final Map<String, String> expected = getEntryMap(fetchKeys);
    assertThat(actual, equalTo(expected));

    final InOrder ordered = inOrder(this.loaderWriter, this.spiedResilienceStrategy);
    verify(this.store).bulkComputeIfAbsent(eq(fetchKeys), getAnyIterableFunction());
    // ResilienceStrategy invoked: no assertion for Store content
    ordered.verify(this.loaderWriter, atLeast(1)).loadAll(this.loadAllCaptor.capture());
    assertThat(this.getLoadAllArgs(), equalTo(fetchKeys));
    ordered.verify(this.spiedResilienceStrategy)
        .getAllFailure(eq(fetchKeys), eq(expected), any(StoreAccessException.class));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.GetOutcome.class));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoadingOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetAllOutcome.FAILURE));
    validateBulkCounters(ehcache, 0, 0);
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#getAll(Set)} for
   * <ul>
   *    <li>non-empty request key set</li>
   *    <li>all {@link Store} entries match</li>
   *    <li>{@link Store#bulkComputeIfAbsent} throws after accessing loader</li>
   *    <li>all {@link CacheLoaderWriter} entries match</li>
   *    <li>no {@link CacheLoaderWriter#loadAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testGetAllStoreAllMatchStoreAccessExceptionAfterLoaderAllMatchNoneFail() throws Exception {
    final FakeStore fakeStore = new FakeStore(getEntryMap(KEY_SET_A, KEY_SET_B), Collections.singleton("keyA3"));
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeLoader = new FakeCacheLoaderWriter(getEntryMap(KEY_SET_A, KEY_SET_B));
    this.loaderWriter = spy(fakeLoader);

    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.loaderWriter);

    final Set<String> fetchKeys = fanIn(KEY_SET_A, KEY_SET_B);
    final Map<String, String> actual = ehcache.getAll(fetchKeys);


    final Map<String, String> expected = getEntryMap(fetchKeys);
    assertThat(actual, equalTo(expected));

    final InOrder ordered = inOrder(this.loaderWriter, this.spiedResilienceStrategy);
    verify(this.store).bulkComputeIfAbsent(eq(fetchKeys), getAnyIterableFunction());
    // ResilienceStrategy invoked: no assertion for Store content
    ordered.verify(this.loaderWriter, atLeast(1)).loadAll(this.loadAllCaptor.capture());
    assertThat(this.getLoadAllArgs(), equalTo(fetchKeys));
    ordered.verify(this.spiedResilienceStrategy)
        .getAllFailure(eq(fetchKeys), eq(expected), any(StoreAccessException.class));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.GetOutcome.class));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoadingOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetAllOutcome.FAILURE));
    validateBulkCounters(ehcache, 0, 0);
  }

  private EhcacheWithLoaderWriter<String, String> getEhcache(final CacheLoaderWriter<String, String> cacheLoaderWriter) {
    final EhcacheWithLoaderWriter<String, String> ehcache = new EhcacheWithLoaderWriter<String, String>(CACHE_CONFIGURATION, this.store, cacheLoaderWriter, cacheEventDispatcher, LoggerFactory.getLogger(EhcacheWithLoaderWriter.class + "-" + "EhcacheWithLoaderWriterBasicGetAllTest"));
    ehcache.init();
    assertThat("cache not initialized", ehcache.getStatus(), Matchers.is(Status.AVAILABLE));
    this.spiedResilienceStrategy = this.setResilienceStrategySpy(ehcache);
    return ehcache;
  }

  private void verifyBulkLoadingException(BulkCacheLoadingException e, Set<String> successKeys, Set<String> failureKeys) {
    assertThat(this.bulkExceptionCaptor.getValue().getSuccesses().keySet(), Matchers.<Set<?>> equalTo(successKeys));
    assertThat(this.bulkExceptionCaptor.getValue().getFailures().keySet(), Matchers.<Set<?>> equalTo(failureKeys));
  }

  /**
   * Collects all arguments captured by {@link #loadAllCaptor}.
   *
   * @return the argument values collected by {@link #loadAllCaptor}; the
   *    {@code Iterator} over the resulting {@code Set} returns the values
   *    in the order observed by the captor.
   */
  private Set<String> getLoadAllArgs() {
    final Set<String> loadAllArgs = new LinkedHashSet<String>();
    for (final Set<String> set : this.loadAllCaptor.getAllValues()) {
      loadAllArgs.addAll(set);
    }
    return loadAllArgs;
  }

}
