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

package org.ehcache;

import org.ehcache.exceptions.BulkCacheLoadingException;
import org.ehcache.exceptions.CacheAccessException;
import org.ehcache.function.Function;
import org.ehcache.spi.cache.Store;
import org.ehcache.statistics.CacheOperationOutcomes;
import org.hamcrest.Matchers;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.EnumSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import static org.ehcache.EhcacheBasicBulkUtil.*;

import org.ehcache.spi.loaderwriter.CacheLoaderWriter;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

/**
 * Provides testing of basic GET_ALL operations on an {@code Ehcache}.
 * <h3>Note</h3>
 * The current implementation of {@link org.ehcache.Ehcache#getAll(java.util.Set) Ehcache.getAll}
 * does <b>not</b> produce partial results while handling a
 * {@link org.ehcache.exceptions.CacheAccessException CacheAccessException}; all keys presented
 * to {@code getAll} succeed or fail based on the recovery call to
 * {@link CacheLoaderWriter#loadAll(Iterable)}.
 *
 * @author Clifford W. Johnson
 */
@SuppressWarnings("ThrowableResultOfMethodCallIgnored")
public class EhcacheBasicGetAllTest extends EhcacheBasicCrudBase {

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
   * {@link org.ehcache.resilience.ResilienceStrategy#getAllFailure(Iterable, Map, CacheAccessException)
   *    ResilienceStrategy.getAllFailure(Iterable, Map, CacheAccessException)} method.
   */
  @Captor
  private ArgumentCaptor<Map<String, String>> getAllFailureMapCaptor;

  /**
   * A Mockito {@code ArgumentCaptor} for the
   * {@link org.ehcache.exceptions.BulkCacheLoadingException BulkCacheLoadingException}
   * provided to the
   * {@link org.ehcache.resilience.ResilienceStrategy#getAllFailure(Iterable, CacheAccessException, BulkCacheLoadingException)
   *    ResilienceStrategy.getAllFailure(Iterable, CacheAccessException, BulkCacheLoadingException)} method.
   */
  @Captor
  private ArgumentCaptor<BulkCacheLoadingException> bulkExceptionCaptor;

  @Test
  public void testGetAllNull() throws Exception {
    final Ehcache<String, String> ehcache = this.getEhcache(null);
    try {
      ehcache.getAll(null);
      fail();
    } catch (NullPointerException e) {
      // Expected
    }
  }

  @Test
  public void testGetAllNullKey() throws Exception {
    final Set<String> keys = new LinkedHashSet<String>();
    for (final String key : KEY_SET_A) {
      keys.add(key);
      if ("keyA2".equals(key)) {
        keys.add(null);     // Add a null element
      }
    }
    final Ehcache<String, String> ehcache = this.getEhcache(null);
    try {
      ehcache.getAll(keys);
      fail();
    } catch (NullPointerException e) {
      // Expected
    }
  }

  /**
   * Tests {@link Ehcache#getAll(Set)} for
   * <ul>
   *    <li>empty request key set</li>
   *    <li>{@link Store#bulkComputeIfAbsent} throws before accessing loader</li>
   *    <li>no {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testGetAllEmptyRequestCacheAccessExceptionBeforeNoLoader() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.<String, String>emptyMap());
    this.store = spy(fakeStore);
    doThrow(new CacheAccessException("")).when(this.store)
        .bulkComputeIfAbsent(getAnyStringSet(), getAnyIterableFunction());

    final Ehcache<String, String> ehcache = this.getEhcache(null);

    final Map<String, String> actual = ehcache.getAll(Collections.<String>emptySet());
    assertThat(actual, is(notNullValue()));
    assertThat(actual.isEmpty(), is(true));

    verify(this.store).bulkComputeIfAbsent(eq(Collections.<String>emptySet()), getAnyIterableFunction());
    verify(this.spiedResilienceStrategy)
        .getAllFailure(eq(Collections.<String>emptySet()), any(CacheAccessException.class));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.GetOutcome.class));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoadingOutcome.class));
  }

  /**
   * Tests {@link Ehcache#getAll(Set)} for
   * <ul>
   *    <li>empty request key set</li>
   *    <li>{@link Store#bulkComputeIfAbsent} throws before accessing loader</li>
   *    <li>with a {@code CacheLoaderWriter} (loader-provided entries not relevant)</li>
   * </ul>
   */
  @Test
  public void testGetAllEmptyRequestCacheAccessExceptionBeforeWithLoader() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.<String, String>emptyMap());
    this.store = spy(fakeStore);
    doThrow(new CacheAccessException("")).when(this.store)
        .bulkComputeIfAbsent(getAnyStringSet(), getAnyIterableFunction());

    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(TEST_ENTRIES);
    this.loaderWriter = spy(fakeLoaderWriter);

    final Ehcache<String, String> ehcache = this.getEhcache(this.loaderWriter);

    final Map<String, String> actual = ehcache.getAll(Collections.<String>emptySet());
    final Map<String, String> expected = Collections.emptyMap();
    assertThat(actual, equalTo(expected));

    final InOrder ordered = inOrder(this.loaderWriter, this.spiedResilienceStrategy);
    verify(this.store).bulkComputeIfAbsent(eq(Collections.<String>emptySet()), getAnyIterableFunction());
    ordered.verify(this.loaderWriter).loadAll(Collections.<String>emptySet());
    ordered.verify(this.spiedResilienceStrategy)
        .getAllFailure(eq(Collections.<String>emptySet()), eq(expected), any(CacheAccessException.class));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.GetOutcome.class));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoadingOutcome.class));
  }

  /**
   * Tests {@link Ehcache#getAll(Set)} for
   * <ul>
   *    <li>empty request key set</li>
   *    <li>{@link Store#bulkComputeIfAbsent} throws before accessing loader</li>
   *    <li>with a {@code CacheLoaderWriter} (loader-provided entries not relevant)</li>
   *    <li>all {@link CacheLoaderWriter#loadAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testGetAllEmptyRequestCacheAccessExceptionBeforeLoaderAllFail() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.<String, String>emptyMap());
    this.store = spy(fakeStore);
    doThrow(new CacheAccessException("")).when(this.store)
        .bulkComputeIfAbsent(getAnyStringSet(), getAnyIterableFunction());

    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(TEST_ENTRIES);
    this.loaderWriter = spy(fakeLoaderWriter);
    doThrow(new Exception("loadAll failed")).when(this.loaderWriter).loadAll(getAnyStringSet());

    final Ehcache<String, String> ehcache = this.getEhcache(this.loaderWriter);

    final Map<String, String> actual = ehcache.getAll(Collections.<String>emptySet());
    final Map<String, String> expected = Collections.emptyMap();
    assertThat(actual, equalTo(expected));

    final InOrder ordered = inOrder(this.loaderWriter, this.spiedResilienceStrategy);
    verify(this.store).bulkComputeIfAbsent(eq(Collections.<String>emptySet()), getAnyIterableFunction());
    ordered.verify(this.loaderWriter).loadAll(Collections.<String>emptySet());
    ordered.verify(this.spiedResilienceStrategy)
        .getAllFailure(eq(Collections.<String>emptySet()), eq(expected), any(CacheAccessException.class));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.GetOutcome.class));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoadingOutcome.class));
  }

  /**
   * Tests {@link Ehcache#getAll(Set)} for
   * <ul>
   *    <li>non-empty request key set</li>
   *    <li>no {@link Store} entries match</li>
   *    <li>no {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testGetAllStoreNoMatchNoLoader() throws Exception {
    final FakeStore fakeStore = new FakeStore(getEntryMap(KEY_SET_B));
    this.store = spy(fakeStore);

    final Ehcache<String, String> ehcache = this.getEhcache(null);

    final Map<String, String> actual = ehcache.getAll(KEY_SET_A);
    assertThat(actual, equalTo(getNullEntryMap(KEY_SET_A)));

    verify(this.store).bulkComputeIfAbsent(eq(KEY_SET_A), getAnyIterableFunction());
    assertThat(fakeStore.getEntryMap(), equalTo(getEntryMap(KEY_SET_B)));
    verifyZeroInteractions(this.spiedResilienceStrategy);

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.GetOutcome.class));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoadingOutcome.class));
  }

  /**
   * Tests {@link Ehcache#getAll(Set)} for
   * <ul>
   *    <li>non-empty request key set</li>
   *    <li>no {@link Store} entries match</li>
   *    <li>{@link Store#bulkComputeIfAbsent} throws before accessing loader</li>
   *    <li>no {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testGetAllStoreNoMatchCacheAccessExceptionBeforeNoLoader() throws Exception {
    final FakeStore fakeStore = new FakeStore(getEntryMap(KEY_SET_B));
    this.store = spy(fakeStore);
    doThrow(new CacheAccessException("")).when(this.store)
        .bulkComputeIfAbsent(getAnyStringSet(), getAnyIterableFunction());

    final Ehcache<String, String> ehcache = this.getEhcache(null);

    final Map<String, String> actual = ehcache.getAll(KEY_SET_A);
    assertThat(actual, equalTo(getNullEntryMap(KEY_SET_A)));

    verify(this.store).bulkComputeIfAbsent(eq(KEY_SET_A), getAnyIterableFunction());
    // ResilienceStrategy invoked: no assertion for Store content
    verify(this.spiedResilienceStrategy)
        .getAllFailure(eq(KEY_SET_A), any(CacheAccessException.class));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.GetOutcome.class));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoadingOutcome.class));
  }

  /**
   * Tests {@link Ehcache#getAll(Set)} for
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

    final Ehcache<String, String> ehcache = this.getEhcache(this.loaderWriter);

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
  }

  /**
   * Tests {@link Ehcache#getAll(Set)} for
   * <ul>
   *    <li>non-empty request key set</li>
   *    <li>no {@link Store} entries match</li>
   *    <li>{@link Store#bulkComputeIfAbsent} throws before accessing loader</li>
   *    <li>with a {@code CacheLoaderWriter} (loader-provided entries not relevant)</li>
   *    <li>all {@link CacheLoaderWriter#loadAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testGetAllStoreNoMatchCacheAccessExceptionBeforeLoaderAllFail() throws Exception {
    final FakeStore fakeStore = new FakeStore(getEntryMap(KEY_SET_B));
    this.store = spy(fakeStore);
    doThrow(new CacheAccessException("")).when(this.store)
        .bulkComputeIfAbsent(getAnyStringSet(), getAnyIterableFunction());

    final FakeCacheLoaderWriter fakeLoader = new FakeCacheLoaderWriter(TEST_ENTRIES);
    this.loaderWriter = spy(fakeLoader);
    doThrow(new Exception("loadAll failed")).when(this.loaderWriter).loadAll(getAnyStringSet());

    final Ehcache<String, String> ehcache = this.getEhcache(this.loaderWriter);

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
        .getAllFailure(eq(KEY_SET_A), any(CacheAccessException.class), this.bulkExceptionCaptor.capture());
    assertThat(this.bulkExceptionCaptor.getValue().getSuccesses().keySet(), empty());
    assertThat(this.bulkExceptionCaptor.getValue().getFailures().keySet(), Matchers.<Set<?>>equalTo(KEY_SET_A));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.GetOutcome.class));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoadingOutcome.class));
  }

  /**
   * Tests {@link Ehcache#getAll(Set)} for
   * <ul>
   *    <li>non-empty request key set</li>
   *    <li>no {@link Store} entries match</li>
   *    <li>{@link Store#bulkComputeIfAbsent} throws after accessing loader</li>
   *    <li>with a {@code CacheLoaderWriter} (loader-provided entries not relevant)</li>
   *    <li>all {@link CacheLoaderWriter#loadAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testGetAllStoreNoMatchCacheAccessExceptionAfterLoaderAllFail() throws Exception {
    final FakeStore fakeStore = new FakeStore(getEntryMap(KEY_SET_B), Collections.singleton("keyA3"));
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeLoader = new FakeCacheLoaderWriter(TEST_ENTRIES);
    this.loaderWriter = spy(fakeLoader);
    doThrow(new Exception("loadAll failed")).when(this.loaderWriter).loadAll(getAnyStringSet());

    final Ehcache<String, String> ehcache = this.getEhcache(this.loaderWriter);

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
        .getAllFailure(eq(KEY_SET_A), any(CacheAccessException.class), this.bulkExceptionCaptor.capture());
    assertThat(this.bulkExceptionCaptor.getValue().getSuccesses().keySet(), empty());
    assertThat(this.bulkExceptionCaptor.getValue().getFailures().keySet(), Matchers.<Set<?>>equalTo(KEY_SET_A));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.GetOutcome.class));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoadingOutcome.class));
  }

  /**
   * Tests {@link Ehcache#getAll(Set)} for
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

    final Ehcache<String, String> ehcache = this.getEhcache(this.loaderWriter);

    final Map<String, String> actual = ehcache.getAll(KEY_SET_A);
    assertThat(actual, equalTo(getNullEntryMap(KEY_SET_A)));

    verify(this.store).bulkComputeIfAbsent(eq(KEY_SET_A), getAnyIterableFunction());
    assertThat(fakeStore.getEntryMap(), equalTo(getEntryMap(KEY_SET_B)));
    verify(this.loaderWriter, atLeast(1)).loadAll(this.loadAllCaptor.capture());
    assertThat(this.getLoadAllArgs(), equalTo(KEY_SET_A));
    verifyZeroInteractions(this.spiedResilienceStrategy);

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.GetOutcome.class));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoadingOutcome.class));
  }

  /**
   * Tests {@link Ehcache#getAll(java.util.Set)} for
   * <ul>
   *    <li>non-empty request key set</li>
   *    <li>no {@link org.ehcache.spi.cache.Store} entries match</li>
   *    <li>{@link org.ehcache.spi.cache.Store#bulkComputeIfAbsent} throws before accessing loader</li>
   *    <li>no {@link org.ehcache.spi.loader.CacheLoaderWriter} entries match</li>
   *    <li>no {@link org.ehcache.spi.loader.CacheLoaderWriter#loadAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testGetAllStoreNoMatchCacheAccessExceptionBeforeLoaderNoMatchNoneFail() throws Exception {
    final FakeStore fakeStore = new FakeStore(getEntryMap(KEY_SET_B));
    this.store = spy(fakeStore);
    doThrow(new CacheAccessException("")).when(this.store)
        .bulkComputeIfAbsent(getAnyStringSet(), getAnyIterableFunction());

    final FakeCacheLoaderWriter fakeLoader = new FakeCacheLoaderWriter(getEntryMap(KEY_SET_C));
    this.loaderWriter = spy(fakeLoader);

    final Ehcache<String, String> ehcache = this.getEhcache(this.loaderWriter);

    final Map<String, String> actual = ehcache.getAll(KEY_SET_A);
    final Map<String, String> expected = getNullEntryMap(KEY_SET_A);
    assertThat(actual, equalTo(expected));

    final InOrder ordered = inOrder(this.loaderWriter, this.spiedResilienceStrategy);
    verify(this.store).bulkComputeIfAbsent(eq(KEY_SET_A), getAnyIterableFunction());
    // ResilienceStrategy invoked: no assertion for Store content
    ordered.verify(this.loaderWriter, atLeast(1)).loadAll(this.loadAllCaptor.capture());
    assertThat(this.getLoadAllArgs(), equalTo(KEY_SET_A));
    ordered.verify(this.spiedResilienceStrategy)
        .getAllFailure(eq(KEY_SET_A), eq(expected), any(CacheAccessException.class));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.GetOutcome.class));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoadingOutcome.class));
  }

  /**
   * Tests {@link Ehcache#getAll(Set)} for
   * <ul>
   *    <li>non-empty request key set</li>
   *    <li>no {@link Store} entries match</li>
   *    <li>{@link Store#bulkComputeIfAbsent} throws after accessing loader</li>
   *    <li>no {@link CacheLoaderWriter} entries match</li>
   *    <li>no {@link CacheLoaderWriter#loadAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testGetAllStoreNoMatchCacheAccessExceptionAfterLoaderNoMatchNoneFail() throws Exception {
    final FakeStore fakeStore = new FakeStore(getEntryMap(KEY_SET_B), Collections.singleton("keyA3"));
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeLoader = new FakeCacheLoaderWriter(getEntryMap(KEY_SET_C));
    this.loaderWriter = spy(fakeLoader);

    final Ehcache<String, String> ehcache = this.getEhcache(this.loaderWriter);

    final Map<String, String> actual = ehcache.getAll(KEY_SET_A);
    final Map<String, String> expected = getNullEntryMap(KEY_SET_A);
    assertThat(actual, equalTo(expected));

    final InOrder ordered = inOrder(this.loaderWriter, this.spiedResilienceStrategy);
    verify(this.store).bulkComputeIfAbsent(eq(KEY_SET_A), getAnyIterableFunction());
    // ResilienceStrategy invoked: no assertion for Store content
    ordered.verify(this.loaderWriter, atLeast(1)).loadAll(this.loadAllCaptor.capture());
    assertThat(this.getLoadAllArgs(), equalTo(KEY_SET_A));
    ordered.verify(this.spiedResilienceStrategy)
        .getAllFailure(eq(KEY_SET_A), eq(expected), any(CacheAccessException.class));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.GetOutcome.class));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoadingOutcome.class));
  }

  /**
   * Tests {@link Ehcache#getAll(Set)} for
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

    final Ehcache<String, String> ehcache = this.getEhcache(this.loaderWriter);

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
  }

  /**
   * Tests {@link Ehcache#getAll(Set)} for
   * <ul>
   *    <li>non-empty request key set</li>
   *    <li>no {@link Store} entries match</li>
   *    <li>{@link Store#bulkComputeIfAbsent} throws before accessing loader</li>
   *    <li>no {@link CacheLoaderWriter} entries match</li>
   *    <li>some {@link CacheLoaderWriter#loadAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testGetAllStoreNoMatchCacheAccessExceptionBeforeLoaderNoMatchSomeFail() throws Exception {
    final FakeStore fakeStore = new FakeStore(getEntryMap(KEY_SET_B));
    this.store = spy(fakeStore);
    doThrow(new CacheAccessException("")).when(this.store)
        .bulkComputeIfAbsent(getAnyStringSet(), getAnyIterableFunction());

    final FakeCacheLoaderWriter fakeLoader = new FakeCacheLoaderWriter(getEntryMap(KEY_SET_C), KEY_SET_F);
    this.loaderWriter = spy(fakeLoader);

    final Ehcache<String, String> ehcache = this.getEhcache(this.loaderWriter);

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
        .getAllFailure(eq(fetchKeys), any(CacheAccessException.class), this.bulkExceptionCaptor.capture());
    assertThat(this.bulkExceptionCaptor.getValue().getSuccesses().keySet(), empty());
    assertThat(this.bulkExceptionCaptor.getValue().getFailures().keySet(), Matchers.<Set<?>>equalTo(fetchKeys));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.GetOutcome.class));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoadingOutcome.class));
  }

  /**
   * Tests {@link Ehcache#getAll(Set)} for
   * <ul>
   *    <li>non-empty request key set</li>
   *    <li>no {@link Store} entries match</li>
   *    <li>{@link Store#bulkComputeIfAbsent} throws after accessing loader</li>
   *    <li>no {@link CacheLoaderWriter} entries match</li>
   *    <li>some {@link CacheLoaderWriter#loadAll(Iterable)} calls fail</li>
   * </ul>
   */
  // TODO: Basis for cache miss handling issue #226
  @Test
  public void testGetAllStoreNoMatchCacheAccessExceptionAfterLoaderNoMatchSomeFail() throws Exception {
    final FakeStore fakeStore = new FakeStore(getEntryMap(KEY_SET_B), Collections.singleton("keyA3"));
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeLoader = new FakeCacheLoaderWriter(getEntryMap(KEY_SET_C), KEY_SET_F);
    this.loaderWriter = spy(fakeLoader);

    final Ehcache<String, String> ehcache = this.getEhcache(this.loaderWriter);

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
        .getAllFailure(eq(fetchKeys), any(CacheAccessException.class), this.bulkExceptionCaptor.capture());
    final Set<String> successKeys = copyWithout(copyUntil(fetchKeys, "keyA3"), KEY_SET_F);
    final Set<String> failKeys = copyWithout(fetchKeys, successKeys);
    assertThat(this.bulkExceptionCaptor.getValue().getSuccesses(),
        Matchers.<Map<?, ?>>equalTo(getNullEntryMap(successKeys)));
    assertThat(this.bulkExceptionCaptor.getValue().getFailures().keySet(), Matchers.<Set<?>>equalTo(failKeys));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.GetOutcome.class));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoadingOutcome.class));
  }

  /**
   * Tests {@link Ehcache#getAll(Set)} for
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

    final Ehcache<String, String> ehcache = this.getEhcache(this.loaderWriter);

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
  }

  /**
   * Tests {@link Ehcache#getAll(Set)} for
   * <ul>
   *    <li>non-empty request key set</li>
   *    <li>no {@link Store} entries match</li>
   *    <li>{@link Store#bulkComputeIfAbsent} throws before accessing loader</li>
   *    <li>some {@link CacheLoaderWriter} entries match</li>
   *    <li>no {@link CacheLoaderWriter#loadAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testGetAllStoreNoMatchCacheAccessExceptionBeforeLoaderSomeMatchNoneFail() throws Exception {
    final FakeStore fakeStore = new FakeStore(getEntryMap(KEY_SET_B));
    this.store = spy(fakeStore);
    doThrow(new CacheAccessException("")).when(this.store)
        .bulkComputeIfAbsent(getAnyStringSet(), getAnyIterableFunction());

    final FakeCacheLoaderWriter fakeLoader = new FakeCacheLoaderWriter(getEntryMap(KEY_SET_B, KEY_SET_C));
    this.loaderWriter = spy(fakeLoader);

    final Ehcache<String, String> ehcache = this.getEhcache(this.loaderWriter);

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
        .getAllFailure(eq(fetchKeys), eq(expected), any(CacheAccessException.class));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.GetOutcome.class));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoadingOutcome.class));
  }

  /**
   * Tests {@link Ehcache#getAll(Set)} for
   * <ul>
   *    <li>non-empty request key set</li>
   *    <li>no {@link Store} entries match</li>
   *    <li>{@link Store#bulkComputeIfAbsent} throws after accessing loader</li>
   *    <li>some {@link CacheLoaderWriter} entries match</li>
   *    <li>no {@link CacheLoaderWriter#loadAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testGetAllStoreNoMatchCacheAccessExceptionAfterLoaderSomeMatchNoneFail() throws Exception {
    final FakeStore fakeStore = new FakeStore(getEntryMap(KEY_SET_B), Collections.singleton("keyA3"));
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeLoader = new FakeCacheLoaderWriter(getEntryMap(KEY_SET_B, KEY_SET_C));
    this.loaderWriter = spy(fakeLoader);

    final Ehcache<String, String> ehcache = this.getEhcache(this.loaderWriter);

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
        .getAllFailure(eq(fetchKeys), eq(expected), any(CacheAccessException.class));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.GetOutcome.class));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoadingOutcome.class));
  }

  /**
   * Tests {@link Ehcache#getAll(Set)} for
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

    final Ehcache<String, String> ehcache = this.getEhcache(this.loaderWriter);

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
  }

  /**
   * Tests {@link Ehcache#getAll(Set)} for
   * <ul>
   *    <li>non-empty request key set</li>
   *    <li>no {@link Store} entries match</li>
   *    <li>{@link Store#bulkComputeIfAbsent} throws before accessing loader</li>
   *    <li>some {@link CacheLoaderWriter} entries match</li>
   *    <li>non-matching {@link CacheLoaderWriter#loadAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testGetAllStoreNoMatchCacheAccessExceptionBeforeLoaderSomeMatchDisjointFail() throws Exception {
    final FakeStore fakeStore = new FakeStore(getEntryMap(KEY_SET_B));
    this.store = spy(fakeStore);
    doThrow(new CacheAccessException("")).when(this.store)
        .bulkComputeIfAbsent(getAnyStringSet(), getAnyIterableFunction());

    final FakeCacheLoaderWriter fakeLoader = new FakeCacheLoaderWriter(getEntryMap(KEY_SET_C), KEY_SET_B);
    this.loaderWriter = spy(fakeLoader);

    final Ehcache<String, String> ehcache = this.getEhcache(this.loaderWriter);

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
        .getAllFailure(eq(fetchKeys), eq(expected), any(CacheAccessException.class));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.GetOutcome.class));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoadingOutcome.class));
  }

  /**
   * Tests {@link Ehcache#getAll(Set)} for
   * <ul>
   *    <li>non-empty request key set</li>
   *    <li>no {@link Store} entries match</li>
   *    <li>{@link Store#bulkComputeIfAbsent} throws after accessing loader</li>
   *    <li>some {@link CacheLoaderWriter} entries match</li>
   *    <li>non-matching {@link CacheLoaderWriter#loadAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testGetAllStoreNoMatchCacheAccessExceptionAfterLoaderSomeMatchDisjointFail() throws Exception {
    final FakeStore fakeStore = new FakeStore(getEntryMap(KEY_SET_B), Collections.singleton("keyA3"));
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeLoader = new FakeCacheLoaderWriter(getEntryMap(KEY_SET_C), KEY_SET_B);
    this.loaderWriter = spy(fakeLoader);

    final Ehcache<String, String> ehcache = this.getEhcache(this.loaderWriter);

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
        .getAllFailure(eq(fetchKeys), eq(expected), any(CacheAccessException.class));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.GetOutcome.class));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoadingOutcome.class));
  }

  /**
   * Tests {@link Ehcache#getAll(Set)} for
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

    final Ehcache<String, String> ehcache = this.getEhcache(this.loaderWriter);

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
  }

  /**
   * Tests {@link Ehcache#getAll(Set)} for
   * <ul>
   *    <li>non-empty request key set</li> 
   *    <li>no {@link Store} entries match</li>
   *    <li>{@link Store#bulkComputeIfAbsent} throws before accessing loader</li>
   *    <li>all {@link CacheLoaderWriter} entries match</li>
   *    <li>no {@link CacheLoaderWriter#loadAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testGetAllStoreNoMatchCacheAccessExceptionBeforeLoaderAllMatchNoneFail() throws Exception {
    final FakeStore fakeStore = new FakeStore(getEntryMap(KEY_SET_B));
    this.store = spy(fakeStore);
    doThrow(new CacheAccessException("")).when(this.store)
        .bulkComputeIfAbsent(getAnyStringSet(), getAnyIterableFunction());

    final FakeCacheLoaderWriter fakeLoader = new FakeCacheLoaderWriter(getEntryMap(KEY_SET_A, KEY_SET_C));
    this.loaderWriter = spy(fakeLoader);

    final Ehcache<String, String> ehcache = this.getEhcache(this.loaderWriter);

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
        .getAllFailure(eq(fetchKeys), eq(expected), any(CacheAccessException.class));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.GetOutcome.class));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoadingOutcome.class));
  }

  /**
   * Tests {@link Ehcache#getAll(Set)} for
   * <ul>
   *    <li>non-empty request key set</li> 
   *    <li>no {@link Store} entries match</li>
   *    <li>{@link Store#bulkComputeIfAbsent} throws after accessing loader</li>
   *    <li>all {@link CacheLoaderWriter} entries match</li>
   *    <li>no {@link CacheLoaderWriter#loadAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testGetAllStoreNoMatchCacheAccessExceptionAfterLoaderAllMatchNoneFail() throws Exception {
    final FakeStore fakeStore = new FakeStore(getEntryMap(KEY_SET_B), Collections.singleton("keyA3"));
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeLoader = new FakeCacheLoaderWriter(getEntryMap(KEY_SET_A, KEY_SET_C));
    this.loaderWriter = spy(fakeLoader);

    final Ehcache<String, String> ehcache = this.getEhcache(this.loaderWriter);

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
        .getAllFailure(eq(fetchKeys), eq(expected), any(CacheAccessException.class));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.GetOutcome.class));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoadingOutcome.class));
  }

  /**
   * Tests {@link Ehcache#getAll(Set)} for
   * <ul>
   *    <li>non-empty request key set</li> 
   *    <li>some {@link Store} entries match</li>
   *    <li>no {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testGetAllStoreSomeMatchNoLoader() throws Exception {
    final FakeStore fakeStore = new FakeStore(getEntryMap(KEY_SET_A, KEY_SET_B));
    this.store = spy(fakeStore);

    final Ehcache<String, String> ehcache = this.getEhcache(null);

    final Set<String> fetchKeys = fanIn(KEY_SET_A, KEY_SET_C);
    final Map<String, String> actual = ehcache.getAll(fetchKeys);

    assertThat(actual, equalTo(union(getEntryMap(KEY_SET_A), getNullEntryMap(KEY_SET_C))));

    verify(this.store).bulkComputeIfAbsent(eq(fetchKeys), getAnyIterableFunction());
    assertThat(fakeStore.getEntryMap(), equalTo(getEntryMap(KEY_SET_A, KEY_SET_B)));
    verifyZeroInteractions(this.spiedResilienceStrategy);

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.GetOutcome.class));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoadingOutcome.class));
  }

  /**
   * Tests {@link Ehcache#getAll(Set)} for
   * <ul>
   *    <li>non-empty request key set</li> 
   *    <li>some {@link Store} entries match</li>
   *    <li>{@link Store#bulkComputeIfAbsent} throws before accessing loader</li>
   *    <li>no {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testGetAllStoreSomeMatchCacheAccessExceptionBeforeNoLoader() throws Exception {
    final FakeStore fakeStore = new FakeStore(getEntryMap(KEY_SET_A, KEY_SET_B));
    this.store = spy(fakeStore);
    doThrow(new CacheAccessException("")).when(this.store)
        .bulkComputeIfAbsent(getAnyStringSet(), getAnyIterableFunction());

    final Ehcache<String, String> ehcache = this.getEhcache(null);

    final Set<String> fetchKeys = fanIn(KEY_SET_A, KEY_SET_C);
    final Map<String, String> actual = ehcache.getAll(fetchKeys);

    assertThat(actual, equalTo(getNullEntryMap(fetchKeys)));

    verify(this.store).bulkComputeIfAbsent(eq(fetchKeys), getAnyIterableFunction());
    // ResilienceStrategy invoked: no assertion for Store content
    verify(this.spiedResilienceStrategy).getAllFailure(eq(fetchKeys), any(CacheAccessException.class));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.GetOutcome.class));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoadingOutcome.class));
  }

  /**
   * Tests {@link Ehcache#getAll(Set)} for
   * <ul>
   *    <li>non-empty request key set</li> 
   *    <li>some {@link Store} entries match</li>
   *    <li>with a {@code CacheLoaderWriter} (loader-provided entries not relevant)</li>
   *    <li>all {@link CacheLoaderWriter#loadAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Ignore("BulkCacheLoadingException.getSuccesses() does not include store hits  Issue #225")
  @Test
  public void testGetAllStoreSomeMatchLoaderAllFail() throws Exception {
    final FakeStore fakeStore = new FakeStore(getEntryMap(KEY_SET_A, KEY_SET_B));
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeLoader = new FakeCacheLoaderWriter(TEST_ENTRIES);
    this.loaderWriter = spy(fakeLoader);
    doThrow(new Exception("loadAll failed")).when(this.loaderWriter).loadAll(getAnyStringSet());

    final Ehcache<String, String> ehcache = this.getEhcache(this.loaderWriter);

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
  }

  /**
   * Tests {@link Ehcache#getAll(Set)} for
   * <ul>
   *    <li>non-empty request key set</li> 
   *    <li>some {@link Store} entries match</li>
   *    <li>{@link Store#bulkComputeIfAbsent} throws before accessing loader</li>
   *    <li>with a {@code CacheLoaderWriter} (loader-provided entries not relevant)</li>
   *    <li>all {@link CacheLoaderWriter#loadAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testGetAllStoreSomeMatchCacheAccessExceptionBeforeLoaderAllFail() throws Exception {
    final FakeStore fakeStore = new FakeStore(getEntryMap(KEY_SET_A, KEY_SET_B));
    this.store = spy(fakeStore);
    doThrow(new CacheAccessException("")).when(this.store)
        .bulkComputeIfAbsent(getAnyStringSet(), getAnyIterableFunction());

    final FakeCacheLoaderWriter fakeLoader = new FakeCacheLoaderWriter(TEST_ENTRIES);
    this.loaderWriter = spy(fakeLoader);
    doThrow(new Exception("loadAll failed")).when(this.loaderWriter).loadAll(getAnyStringSet());

    final Ehcache<String, String> ehcache = this.getEhcache(this.loaderWriter);

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
        .getAllFailure(eq(fetchKeys), any(CacheAccessException.class), this.bulkExceptionCaptor.capture());
    assertThat(this.bulkExceptionCaptor.getValue().getSuccesses().keySet(), empty());
    assertThat(this.bulkExceptionCaptor.getValue().getFailures().keySet(), Matchers.<Set<?>>equalTo(fetchKeys));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.GetOutcome.class));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoadingOutcome.class));
  }

  /**
   * Tests {@link Ehcache#getAll(Set)} for
   * <ul>
   *    <li>non-empty request key set</li> 
   *    <li>some {@link Store} entries match</li>
   *    <li>{@link Store#bulkComputeIfAbsent} throws after accessing loader</li>
   *    <li>with a {@code CacheLoaderWriter} (loader-provided entries not relevant)</li>
   *    <li>all {@link CacheLoaderWriter#loadAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Ignore("BulkCacheLoadingException.getSuccesses() does not include store hits  Issue #226, Issue #227")
  @Test
  public void testGetAllStoreSomeMatchCacheAccessExceptionAfterLoaderAllFail() throws Exception {
    final FakeStore fakeStore = new FakeStore(getEntryMap(KEY_SET_A, KEY_SET_B), Collections.singleton("keyA3"));
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeLoader = new FakeCacheLoaderWriter(TEST_ENTRIES);
    this.loaderWriter = spy(fakeLoader);
    doThrow(new Exception("loadAll failed")).when(this.loaderWriter).loadAll(getAnyStringSet());

    final Ehcache<String, String> ehcache = this.getEhcache(this.loaderWriter);

    final Set<String> fetchKeys = fanIn(KEY_SET_A, KEY_SET_C);
    try {
      ehcache.getAll(fetchKeys);
      fail();
    } catch (BulkCacheLoadingException e) {
      // Expected
    }

    // TODO: Determine proper result after Issue resolution
    final InOrder ordered = inOrder(this.loaderWriter, this.spiedResilienceStrategy);
    verify(this.store).bulkComputeIfAbsent(eq(fetchKeys), getAnyIterableFunction());
    // ResilienceStrategy invoked: no assertion for Store content
    ordered.verify(this.loaderWriter, atLeast(1)).loadAll(this.loadAllCaptor.capture());
    assertThat(this.getLoadAllArgs(), equalTo(fetchKeys));

    ordered.verify(this.spiedResilienceStrategy)
        .getAllFailure(eq(fetchKeys), any(CacheAccessException.class), this.bulkExceptionCaptor.capture());
    final Set<String> successKeys = copyUntil(fetchKeys, "keyA3");
    final Set<String> valueKeys = copyOnly(successKeys, KEY_SET_A);
    final Set<String> nullKeys = copyWithout(successKeys, valueKeys);
    final Set<String> failKeys = copyWithout(fetchKeys, successKeys);
    assertThat(this.bulkExceptionCaptor.getValue().getSuccesses(),
        Matchers.<Map<?, ?>>equalTo(union(getEntryMap(successKeys), getNullEntryMap(nullKeys))));
    assertThat(this.bulkExceptionCaptor.getValue().getFailures().keySet(), Matchers.<Set<?>>equalTo(failKeys));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.GetOutcome.class));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoadingOutcome.class));
  }

  /**
   * Tests {@link Ehcache#getAll(Set)} for
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

    final Ehcache<String, String> ehcache = this.getEhcache(this.loaderWriter);

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
  }

  /**
   * Tests {@link Ehcache#getAll(Set)} for
   * <ul>
   *    <li>non-empty request key set</li> 
   *    <li>some {@link Store} entries match</li>
   *    <li>{@link Store#bulkComputeIfAbsent} throws before accessing loader</li>
   *    <li>no {@link CacheLoaderWriter} entries match</li>
   *    <li>no {@link CacheLoaderWriter#loadAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testGetAllStoreSomeMatchCacheAccessExceptionBeforeLoaderNoMatchNoneFail() throws Exception {
    final FakeStore fakeStore = new FakeStore(getEntryMap(KEY_SET_A, KEY_SET_B));
    this.store = spy(fakeStore);
    doThrow(new CacheAccessException("")).when(this.store)
        .bulkComputeIfAbsent(getAnyStringSet(), getAnyIterableFunction());

    final FakeCacheLoaderWriter fakeLoader = new FakeCacheLoaderWriter(getEntryMap(KEY_SET_F));
    this.loaderWriter = spy(fakeLoader);

    final Ehcache<String, String> ehcache = this.getEhcache(this.loaderWriter);

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
        .getAllFailure(eq(fetchKeys), eq(expected), any(CacheAccessException.class));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.GetOutcome.class));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoadingOutcome.class));
  }

  /**
   * Tests {@link Ehcache#getAll(Set)} for
   * <ul>
   *    <li>non-empty request key set</li> 
   *    <li>some {@link Store} entries match</li>
   *    <li>{@link Store#bulkComputeIfAbsent} throws after accessing loader</li>
   *    <li>no {@link CacheLoaderWriter} entries match</li>
   *    <li>no {@link CacheLoaderWriter#loadAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Ignore("ResilienceStrategy.getAllFailure(Iterable, Map, CacheAccessException) Map argument does not include store hits  Issue #227")
  @Test
  public void testGetAllStoreSomeMatchCacheAccessExceptionAfterLoaderNoMatchNoneFail() throws Exception {
    final FakeStore fakeStore = new FakeStore(getEntryMap(KEY_SET_A, KEY_SET_B), Collections.singleton("keyA3"));
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeLoader = new FakeCacheLoaderWriter(getEntryMap(KEY_SET_F));
    this.loaderWriter = spy(fakeLoader);

    final Ehcache<String, String> ehcache = this.getEhcache(this.loaderWriter);

    final Set<String> fetchKeys = fanIn(KEY_SET_A, KEY_SET_C);
    final Map<String, String> actual = ehcache.getAll(fetchKeys);

    assertThat(actual, equalTo(getNullEntryMap(fetchKeys)));

    // TODO: Determine proper result after Issue resolution
    final InOrder ordered = inOrder(this.loaderWriter, this.spiedResilienceStrategy);
    verify(this.store).bulkComputeIfAbsent(eq(fetchKeys), getAnyIterableFunction());
    // ResilienceStrategy invoked: no assertion for Store content
    ordered.verify(this.loaderWriter, atLeast(1)).loadAll(this.loadAllCaptor.capture());
    assertThat(this.getLoadAllArgs(), equalTo(fetchKeys));

    ordered.verify(this.spiedResilienceStrategy)
        .getAllFailure(eq(fetchKeys), this.getAllFailureMapCaptor.capture(), any(CacheAccessException.class));
    final Set<String> successKeys = copyUntil(fetchKeys, "keyA3");
    final Set<String> valueKeys = copyOnly(successKeys, KEY_SET_A);
    final Set<String> nullKeys = copyWithout(successKeys, valueKeys);
    final Set<String> failKeys = copyWithout(fetchKeys, successKeys);
    assertThat(this.getAllFailureMapCaptor.getValue(),
        equalTo(union(getEntryMap(valueKeys), getNullEntryMap(nullKeys, failKeys))));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.GetOutcome.class));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoadingOutcome.class));
  }

  /**
   * Tests {@link Ehcache#getAll(Set)} for
   * <ul>
   *    <li>non-empty request key set</li> 
   *    <li>some {@link Store} entries match</li>
   *    <li>no {@link CacheLoaderWriter} entries match</li>
   *    <li>some {@link CacheLoaderWriter#loadAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Ignore("BulkCacheLoadingException.getSuccesses() does not include store hits  Issue #225")
  @Test
  public void testGetAllStoreSomeMatchLoaderNoMatchSomeFail() throws Exception {
    final FakeStore fakeStore = new FakeStore(getEntryMap(KEY_SET_A, KEY_SET_B));
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeLoader = new FakeCacheLoaderWriter(getEntryMap(KEY_SET_F), KEY_SET_D);
    this.loaderWriter = spy(fakeLoader);

    final Ehcache<String, String> ehcache = this.getEhcache(this.loaderWriter);

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
  }

  /**
   * Tests {@link Ehcache#getAll(Set)} for
   * <ul>
   *    <li>non-empty request key set</li> 
   *    <li>some {@link Store} entries match</li>
   *    <li>{@link Store#bulkComputeIfAbsent} throws before accessing loader</li>
   *    <li>no {@link CacheLoaderWriter} entries match</li>
   *    <li>some {@link CacheLoaderWriter#loadAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testGetAllStoreSomeMatchCacheAccessExceptionBeforeLoaderNoMatchSomeFail() throws Exception {
    final FakeStore fakeStore = new FakeStore(getEntryMap(KEY_SET_A, KEY_SET_B));
    this.store = spy(fakeStore);
    doThrow(new CacheAccessException("")).when(this.store)
        .bulkComputeIfAbsent(getAnyStringSet(), getAnyIterableFunction());

    final FakeCacheLoaderWriter fakeLoader = new FakeCacheLoaderWriter(getEntryMap(KEY_SET_F), KEY_SET_D);
    this.loaderWriter = spy(fakeLoader);

    final Ehcache<String, String> ehcache = this.getEhcache(this.loaderWriter);

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
        .getAllFailure(eq(fetchKeys), any(CacheAccessException.class), this.bulkExceptionCaptor.capture());
    assertThat(this.bulkExceptionCaptor.getValue().getSuccesses().keySet(), empty());
    assertThat(this.bulkExceptionCaptor.getValue().getFailures().keySet(), Matchers.<Set<?>>equalTo(fetchKeys));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.GetOutcome.class));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoadingOutcome.class));
  }

  /**
   * Tests {@link Ehcache#getAll(Set)} for
   * <ul>
   *    <li>non-empty request key set</li> 
   *    <li>some {@link Store} entries match</li>
   *    <li>{@link Store#bulkComputeIfAbsent} throws after accessing loader</li>
   *    <li>no {@link CacheLoaderWriter} entries match</li>
   *    <li>some {@link CacheLoaderWriter#loadAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Ignore("BulkCacheLoadingException.getSuccesses holds cache-miss keys before CacheAccessException but not cache-hit keys  Issue #227")
  @Test
  public void testGetAllStoreSomeMatchCacheAccessExceptionAfterLoaderNoMatchSomeFail() throws Exception {
    final FakeStore fakeStore = new FakeStore(getEntryMap(KEY_SET_A, KEY_SET_B), Collections.singleton("keyA3"));
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeLoader = new FakeCacheLoaderWriter(getEntryMap(KEY_SET_F), KEY_SET_D);
    this.loaderWriter = spy(fakeLoader);

    final Ehcache<String, String> ehcache = this.getEhcache(this.loaderWriter);

    final Set<String> fetchKeys = fanIn(KEY_SET_A, KEY_SET_C, KEY_SET_D);
    try {
      ehcache.getAll(fetchKeys);
      fail();
    } catch (BulkCacheLoadingException e) {
      // Expected
    }

    // TODO: Determine proper result after Issue resolution
    final InOrder ordered = inOrder(this.loaderWriter, this.spiedResilienceStrategy);
    verify(this.store).bulkComputeIfAbsent(eq(fetchKeys), getAnyIterableFunction());
    // ResilienceStrategy invoked: no assertion for Store content
    ordered.verify(this.loaderWriter, atLeast(1)).loadAll(this.loadAllCaptor.capture());
    assertThat(this.getLoadAllArgs(), equalTo(fetchKeys));

    ordered.verify(this.spiedResilienceStrategy)
        .getAllFailure(eq(fetchKeys), any(CacheAccessException.class), this.bulkExceptionCaptor.capture());
    final Set<String> successKeys = copyWithout(copyUntil(fetchKeys, "keyA3"), KEY_SET_D);
    final Set<String> failKeys = copyWithout(fetchKeys, successKeys);
    assertThat(this.bulkExceptionCaptor.getValue().getSuccesses(),
        Matchers.<Map<?, ?>>equalTo(union(getEntryMap(copyOnly(successKeys, KEY_SET_A)),
            getNullEntryMap(copyWithout(successKeys, KEY_SET_A)))));
    assertThat(this.bulkExceptionCaptor.getValue().getFailures().keySet(), Matchers.<Set<?>>equalTo(failKeys));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.GetOutcome.class));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoadingOutcome.class));
  }

  /**
   * Tests {@link Ehcache#getAll(Set)} for
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

    final Ehcache<String, String> ehcache = this.getEhcache(this.loaderWriter);

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
  }

  /**
   * Tests {@link Ehcache#getAll(Set)} for
   * <ul>
   *    <li>non-empty request key set</li> 
   *    <li>some {@link Store} entries match</li>
   *    <li>{@link Store#bulkComputeIfAbsent} throws before accessing loader</li>
   *    <li>some {@link CacheLoaderWriter} entries match</li>
   *    <li>no {@link CacheLoaderWriter#loadAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testGetAllStoreSomeMatchCacheAccessExceptionBeforeLoaderSomeMatchNoneFail() throws Exception {
    final FakeStore fakeStore = new FakeStore(getEntryMap(KEY_SET_A, KEY_SET_B));
    this.store = spy(fakeStore);
    doThrow(new CacheAccessException("")).when(this.store)
        .bulkComputeIfAbsent(getAnyStringSet(), getAnyIterableFunction());

    final FakeCacheLoaderWriter fakeLoader = new FakeCacheLoaderWriter(getEntryMap(KEY_SET_C, KEY_SET_F));
    this.loaderWriter = spy(fakeLoader);

    final Ehcache<String, String> ehcache = this.getEhcache(this.loaderWriter);

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
        .getAllFailure(eq(fetchKeys), eq(expected), any(CacheAccessException.class));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.GetOutcome.class));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoadingOutcome.class));
  }

  /**
   * Tests {@link Ehcache#getAll(Set)} for
   * <ul>
   *    <li>non-empty request key set</li> 
   *    <li>some {@link Store} entries match</li>
   *    <li>{@link Store#bulkComputeIfAbsent} throws after accessing loader</li>
   *    <li>some {@link CacheLoaderWriter} entries match</li>
   *    <li>no {@link CacheLoaderWriter#loadAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Ignore("ResilienceStrategy.getAllFailure(Iterable, Map, CacheAccessException) Map argument does not include store hits  Issue #227")
  @Test
  public void testGetAllStoreSomeMatchCacheAccessExceptionAfterLoaderSomeMatchNoneFail() throws Exception {
    final FakeStore fakeStore = new FakeStore(getEntryMap(KEY_SET_A, KEY_SET_B), Collections.singleton("keyA3"));
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeLoader = new FakeCacheLoaderWriter(getEntryMap(KEY_SET_C, KEY_SET_F));
    this.loaderWriter = spy(fakeLoader);

    final Ehcache<String, String> ehcache = this.getEhcache(this.loaderWriter);

    final Set<String> fetchKeys = fanIn(KEY_SET_A, KEY_SET_C, KEY_SET_D);
    final Map<String, String> actual = ehcache.getAll(fetchKeys);

    assertThat(actual, equalTo(union(getNullEntryMap(KEY_SET_A, KEY_SET_D), getEntryMap(KEY_SET_C))));

    // TODO: Determine proper result after Issue resolution
    final InOrder ordered = inOrder(this.loaderWriter, this.spiedResilienceStrategy);
    verify(this.store).bulkComputeIfAbsent(eq(fetchKeys), getAnyIterableFunction());
    // ResilienceStrategy invoked: no assertion for Store content
    ordered.verify(this.loaderWriter, atLeast(1)).loadAll(this.loadAllCaptor.capture());
    assertThat(this.getLoadAllArgs(), equalTo(fetchKeys));

    ordered.verify(this.spiedResilienceStrategy)
        .getAllFailure(eq(fetchKeys), this.getAllFailureMapCaptor.capture(), any(CacheAccessException.class));
    final Set<String> successKeys = copyUntil(fetchKeys, "keyA3");
    final Set<String> setA_cacheHits = copyOnly(KEY_SET_A, successKeys);
    final Set<String> setA_cacheMisses = copyWithout(KEY_SET_A, successKeys);
    assertThat(this.getAllFailureMapCaptor.getValue(),
        equalTo(union(getEntryMap(KEY_SET_C, setA_cacheHits), getNullEntryMap(KEY_SET_D, setA_cacheMisses))));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.GetOutcome.class));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoadingOutcome.class));
  }

  /**
   * Tests {@link Ehcache#getAll(Set)} for
   * <ul>
   *    <li>non-empty request key set</li> 
   *    <li>some {@link Store} entries match</li>
   *    <li>some {@link CacheLoaderWriter} entries match</li>
   *    <li>non-matching {@link CacheLoaderWriter#loadAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Ignore("BulkCacheLoadingException.getSuccesses() does not include store hits  Issue #225")
  @Test
  public void testGetAllStoreSomeMatchLoaderSomeMatchDisjointFail() throws Exception {
    final FakeStore fakeStore = new FakeStore(getEntryMap(KEY_SET_A, KEY_SET_B));
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeLoader = new FakeCacheLoaderWriter(getEntryMap(KEY_SET_C, KEY_SET_E), KEY_SET_F);
    this.loaderWriter = spy(fakeLoader);

    final Ehcache<String, String> ehcache = this.getEhcache(this.loaderWriter);

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
  }

  /**
   * Tests {@link Ehcache#getAll(Set)} for
   * <ul>
   *    <li>non-empty request key set</li> 
   *    <li>some {@link Store} entries match</li>
   *    <li>{@link Store#bulkComputeIfAbsent} throws before accessing loader</li>
   *    <li>some {@link CacheLoaderWriter} entries match</li>
   *    <li>non-matching {@link CacheLoaderWriter#loadAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testGetAllStoreSomeMatchCacheAccessExceptionBeforeLoaderSomeMatchDisjointFail() throws Exception {
    final FakeStore fakeStore = new FakeStore(getEntryMap(KEY_SET_A, KEY_SET_B));
    this.store = spy(fakeStore);
    doThrow(new CacheAccessException("")).when(this.store)
        .bulkComputeIfAbsent(getAnyStringSet(), getAnyIterableFunction());

    final FakeCacheLoaderWriter fakeLoader = new FakeCacheLoaderWriter(getEntryMap(KEY_SET_C, KEY_SET_E), KEY_SET_F);
    this.loaderWriter = spy(fakeLoader);

    final Ehcache<String, String> ehcache = this.getEhcache(this.loaderWriter);

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
        .getAllFailure(eq(fetchKeys), any(CacheAccessException.class), this.bulkExceptionCaptor.capture());
    assertThat(this.bulkExceptionCaptor.getValue().getSuccesses().keySet(), empty());
    assertThat(this.bulkExceptionCaptor.getValue().getFailures().keySet(), Matchers.<Set<?>>equalTo(fetchKeys));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.GetOutcome.class));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoadingOutcome.class));
  }

  /**
   * Tests {@link Ehcache#getAll(Set)} for
   * <ul>
   *    <li>non-empty request key set</li> 
   *    <li>some {@link Store} entries match</li>
   *    <li>{@link Store#bulkComputeIfAbsent} throws after accessing loader</li>
   *    <li>some {@link CacheLoaderWriter} entries match</li>
   *    <li>non-matching {@link CacheLoaderWriter#loadAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Ignore("BulkCacheLoadingException.getSuccesses holds cache-miss keys before CacheAccessException but not cache-hit keys  Issue #226, Issue #227")
  @Test
  public void testGetAllStoreSomeMatchCacheAccessExceptionAfterLoaderSomeMatchDisjointFail() throws Exception {
    final FakeStore fakeStore = new FakeStore(getEntryMap(KEY_SET_A, KEY_SET_B), Collections.singleton("keyA3"));
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeLoader = new FakeCacheLoaderWriter(getEntryMap(KEY_SET_C, KEY_SET_E), KEY_SET_F);
    this.loaderWriter = spy(fakeLoader);

    final Ehcache<String, String> ehcache = this.getEhcache(this.loaderWriter);

    final Set<String> fetchKeys = fanIn(KEY_SET_A, KEY_SET_C, KEY_SET_D, KEY_SET_F);
    try {
      ehcache.getAll(fetchKeys);
      fail();
    } catch (BulkCacheLoadingException e) {
      // Expected
    }

    // TODO: Determine proper result after Issue resolution
    final InOrder ordered = inOrder(this.loaderWriter, this.spiedResilienceStrategy);
    verify(this.store).bulkComputeIfAbsent(eq(fetchKeys), getAnyIterableFunction());
    // ResilienceStrategy invoked: no assertion for Store content
    ordered.verify(this.loaderWriter, atLeast(1)).loadAll(this.loadAllCaptor.capture());
    assertThat(this.getLoadAllArgs(), equalTo(fetchKeys));

    ordered.verify(this.spiedResilienceStrategy)
        .getAllFailure(eq(fetchKeys), any(CacheAccessException.class), this.bulkExceptionCaptor.capture());
    final Set<String> successKeys = copyWithout(copyUntil(fetchKeys, "keyA3"), KEY_SET_F);
    final Set<String> setA_cacheHits = copyOnly(KEY_SET_A, successKeys);
    final Set<String> failKeys = copyWithout(fetchKeys, successKeys);
    assertThat(this.bulkExceptionCaptor.getValue().getSuccesses(),
        Matchers.<Map<?,?>>equalTo(union(getEntryMap(setA_cacheHits),
            getEntryMap(copyOnly(KEY_SET_C, successKeys)),
            getNullEntryMap(copyOnly(KEY_SET_D, successKeys)))));
    assertThat(this.bulkExceptionCaptor.getValue().getFailures().keySet(), Matchers.<Set<?>>equalTo(failKeys));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.GetOutcome.class));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoadingOutcome.class));
  }

  /**
   * Tests {@link Ehcache#getAll(Set)} for
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

    final Ehcache<String, String> ehcache = this.getEhcache(this.loaderWriter);

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
  }

  /**
   * Tests {@link Ehcache#getAll(Set)} for
   * <ul>
   *    <li>non-empty request key set</li> 
   *    <li>some {@link Store} entries match</li>
   *    <li>{@link Store#bulkComputeIfAbsent} throws before accessing loader</li>
   *    <li>all {@link CacheLoaderWriter} entries match</li>
   *    <li>no {@link CacheLoaderWriter#loadAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testGetAllStoreSomeMatchCacheAccessExceptionBeforeLoaderAllMatchNoneFail() throws Exception {
    final FakeStore fakeStore = new FakeStore(getEntryMap(KEY_SET_A, KEY_SET_B));
    this.store = spy(fakeStore);
    doThrow(new CacheAccessException("")).when(this.store)
        .bulkComputeIfAbsent(getAnyStringSet(), getAnyIterableFunction());

    final FakeCacheLoaderWriter fakeLoader = new FakeCacheLoaderWriter(getEntryMap(KEY_SET_A, KEY_SET_C, KEY_SET_D));
    this.loaderWriter = spy(fakeLoader);

    final Ehcache<String, String> ehcache = this.getEhcache(this.loaderWriter);

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
        .getAllFailure(eq(fetchKeys), eq(expected), any(CacheAccessException.class));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.GetOutcome.class));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoadingOutcome.class));
  }

  /**
   * Tests {@link Ehcache#getAll(Set)} for
   * <ul>
   *    <li>non-empty request key set</li> 
   *    <li>some {@link Store} entries match</li>
   *    <li>{@link Store#bulkComputeIfAbsent} throws after accessing loader</li>
   *    <li>all {@link CacheLoaderWriter} entries match</li>
   *    <li>no {@link CacheLoaderWriter#loadAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testGetAllStoreSomeMatchCacheAccessExceptionAfterLoaderAllMatchNoneFail() throws Exception {
    final FakeStore fakeStore = new FakeStore(getEntryMap(KEY_SET_A, KEY_SET_B), Collections.singleton("keyA3"));
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeLoader = new FakeCacheLoaderWriter(getEntryMap(KEY_SET_A, KEY_SET_C, KEY_SET_D));
    this.loaderWriter = spy(fakeLoader);

    final Ehcache<String, String> ehcache = this.getEhcache(this.loaderWriter);

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
        .getAllFailure(eq(fetchKeys), eq(expected), any(CacheAccessException.class));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.GetOutcome.class));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoadingOutcome.class));
  }

  /**
   * Tests {@link Ehcache#getAll(Set)} for
   * <ul>
   *    <li>non-empty request key set</li> 
   *    <li>all {@link Store} entries match</li>
   *    <li>no {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testGetAllStoreAllMatchNoLoader() throws Exception {
    final FakeStore fakeStore = new FakeStore(getEntryMap(KEY_SET_A, KEY_SET_B));
    this.store = spy(fakeStore);

    final Ehcache<String, String> ehcache = this.getEhcache(null);

    final Set<String> fetchKeys = fanIn(KEY_SET_A, KEY_SET_B);
    final Map<String, String> actual = ehcache.getAll(fetchKeys);

    assertThat(actual, equalTo(getEntryMap(fetchKeys)));

    verify(this.store).bulkComputeIfAbsent(eq(fetchKeys), getAnyIterableFunction());
    assertThat(fakeStore.getEntryMap(), equalTo(getEntryMap(KEY_SET_A, KEY_SET_B)));
    verifyZeroInteractions(this.spiedResilienceStrategy);

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.GetOutcome.class));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoadingOutcome.class));
  }

  /**
   * Tests {@link Ehcache#getAll(Set)} for
   * <ul>
   *    <li>non-empty request key set</li> 
   *    <li>all {@link Store} entries match</li>
   *    <li>{@link Store#bulkComputeIfAbsent} throws before accessing loader</li>
   *    <li>no {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testGetAllStoreAllMatchCacheAccessExceptionBeforeNoLoader() throws Exception {
    final FakeStore fakeStore = new FakeStore(getEntryMap(KEY_SET_A, KEY_SET_B));
    this.store = spy(fakeStore);
    doThrow(new CacheAccessException("")).when(this.store)
        .bulkComputeIfAbsent(getAnyStringSet(), getAnyIterableFunction());

    final Ehcache<String, String> ehcache = this.getEhcache(null);

    final Set<String> fetchKeys = fanIn(KEY_SET_A, KEY_SET_B);
    final Map<String, String> actual = ehcache.getAll(fetchKeys);

    assertThat(actual, equalTo(getNullEntryMap(fetchKeys)));

    verify(this.store).bulkComputeIfAbsent(eq(fetchKeys), getAnyIterableFunction());
    // ResilienceStrategy invoked: no assertion for Store content
    verify(this.spiedResilienceStrategy).getAllFailure(eq(fetchKeys), any(CacheAccessException.class));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.GetOutcome.class));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoadingOutcome.class));
  }

  /**
   * Tests {@link Ehcache#getAll(Set)} for
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

    final Ehcache<String, String> ehcache = this.getEhcache(this.loaderWriter);

    final Set<String> fetchKeys = fanIn(KEY_SET_A, KEY_SET_B);
    final Map<String, String> actual = ehcache.getAll(fetchKeys);

    assertThat(actual, equalTo(getEntryMap(fetchKeys)));

    verify(this.store).bulkComputeIfAbsent(eq(fetchKeys), getAnyIterableFunction());
    assertThat(fakeStore.getEntryMap(), equalTo(getEntryMap(KEY_SET_A, KEY_SET_B)));
    verify(this.loaderWriter, never()).loadAll(getAnyStringSet());
    verifyZeroInteractions(this.spiedResilienceStrategy);

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.GetOutcome.class));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoadingOutcome.class));
  }

  /**
   * Tests {@link Ehcache#getAll(Set)} for
   * <ul>
   *    <li>non-empty request key set</li> 
   *    <li>all {@link Store} entries match</li>
   *    <li>{@link Store#bulkComputeIfAbsent} throws before accessing loader</li>
   *    <li>with a {@code CacheLoaderWriter} (loader-provided entries not relevant)</li>
   *    <li>all {@link CacheLoaderWriter#loadAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testGetAllStoreAllMatchCacheAccessExceptionBeforeLoaderAllFail() throws Exception {
    final FakeStore fakeStore = new FakeStore(getEntryMap(KEY_SET_A, KEY_SET_B));
    this.store = spy(fakeStore);
    doThrow(new CacheAccessException("")).when(this.store)
        .bulkComputeIfAbsent(getAnyStringSet(), getAnyIterableFunction());

    final FakeCacheLoaderWriter fakeLoader = new FakeCacheLoaderWriter(TEST_ENTRIES);
    this.loaderWriter = spy(fakeLoader);
    doThrow(new Exception("loadAll failed")).when(this.loaderWriter).loadAll(getAnyStringSet());

    final Ehcache<String, String> ehcache = this.getEhcache(this.loaderWriter);

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
        .getAllFailure(eq(fetchKeys), any(CacheAccessException.class), this.bulkExceptionCaptor.capture());
    assertThat(this.bulkExceptionCaptor.getValue().getSuccesses().keySet(), empty());
    assertThat(this.bulkExceptionCaptor.getValue().getFailures().keySet(), Matchers.<Set<?>>equalTo(fetchKeys));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.GetOutcome.class));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoadingOutcome.class));
  }

  /**
   * Tests {@link Ehcache#getAll(Set)} for
   * <ul>
   *    <li>non-empty request key set</li> 
   *    <li>all {@link Store} entries match</li>
   *    <li>{@link Store#bulkComputeIfAbsent} throws after accessing loader</li>
   *    <li>with a {@code CacheLoaderWriter} (loader-provided entries not relevant)</li>
   *    <li>all {@link CacheLoaderWriter#loadAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testGetAllStoreAllMatchCacheAccessExceptionAfterLoaderAllFail() throws Exception {
    final FakeStore fakeStore = new FakeStore(getEntryMap(KEY_SET_A, KEY_SET_B), Collections.singleton("keyA3"));
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeLoader = new FakeCacheLoaderWriter(TEST_ENTRIES);
    this.loaderWriter = spy(fakeLoader);
    doThrow(new Exception("loadAll failed")).when(this.loaderWriter).loadAll(getAnyStringSet());

    final Ehcache<String, String> ehcache = this.getEhcache(this.loaderWriter);

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
        .getAllFailure(eq(fetchKeys), any(CacheAccessException.class), this.bulkExceptionCaptor.capture());
    assertThat(this.bulkExceptionCaptor.getValue().getSuccesses().keySet(), empty());
    assertThat(this.bulkExceptionCaptor.getValue().getFailures().keySet(), Matchers.<Set<?>>equalTo(fetchKeys));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.GetOutcome.class));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoadingOutcome.class));
  }

  /**
   * Tests {@link Ehcache#getAll(Set)} for
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

    final Ehcache<String, String> ehcache = this.getEhcache(this.loaderWriter);

    final Set<String> fetchKeys = fanIn(KEY_SET_A, KEY_SET_B);
    final Map<String, String> actual = ehcache.getAll(fetchKeys);

    assertThat(actual, equalTo(getEntryMap(fetchKeys)));

    verify(this.store).bulkComputeIfAbsent(eq(fetchKeys), getAnyIterableFunction());
    assertThat(fakeStore.getEntryMap(), equalTo(getEntryMap(KEY_SET_A, KEY_SET_B)));
    verify(this.loaderWriter, never()).loadAll(getAnyStringSet());
    verifyZeroInteractions(this.spiedResilienceStrategy);

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.GetOutcome.class));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoadingOutcome.class));
  }

  /**
   * Tests {@link Ehcache#getAll(Set)} for
   * <ul>
   *    <li>non-empty request key set</li> 
   *    <li>all {@link Store} entries match</li>
   *    <li>{@link Store#bulkComputeIfAbsent} throws before accessing loader</li>
   *    <li>no {@link CacheLoaderWriter} entries match</li>
   *    <li>no {@link CacheLoaderWriter#loadAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testGetAllStoreAllMatchCacheAccessExceptionBeforeLoaderNoMatchNoneFail() throws Exception {
    final FakeStore fakeStore = new FakeStore(getEntryMap(KEY_SET_A, KEY_SET_B));
    this.store = spy(fakeStore);
    doThrow(new CacheAccessException("")).when(this.store)
        .bulkComputeIfAbsent(getAnyStringSet(), getAnyIterableFunction());

    final FakeCacheLoaderWriter fakeLoader = new FakeCacheLoaderWriter(getEntryMap(KEY_SET_C));
    this.loaderWriter = spy(fakeLoader);

    final Ehcache<String, String> ehcache = this.getEhcache(this.loaderWriter);

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
        .getAllFailure(eq(fetchKeys), eq(expected), any(CacheAccessException.class));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.GetOutcome.class));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoadingOutcome.class));
  }

  /**
   * Tests {@link Ehcache#getAll(Set)} for
   * <ul>
   *    <li>non-empty request key set</li> 
   *    <li>all {@link Store} entries match</li>
   *    <li>{@link Store#bulkComputeIfAbsent} throws after accessing loader</li>
   *    <li>no {@link CacheLoaderWriter} entries match</li>
   *    <li>no {@link CacheLoaderWriter#loadAll(Iterable)} calls fail</li>
   * </ul>
   */
  // TODO: Reconcile against Issue #226 & Issue #227
  @Test
  public void testGetAllStoreAllMatchCacheAccessExceptionAfterLoaderNoMatchNoneFail() throws Exception {
    final FakeStore fakeStore = new FakeStore(getEntryMap(KEY_SET_A, KEY_SET_B), Collections.singleton("keyA3"));
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeLoader = new FakeCacheLoaderWriter(getEntryMap(KEY_SET_C));
    this.loaderWriter = spy(fakeLoader);

    final Ehcache<String, String> ehcache = this.getEhcache(this.loaderWriter);

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
        .getAllFailure(eq(fetchKeys), eq(expected), any(CacheAccessException.class));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.GetOutcome.class));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoadingOutcome.class));
  }

  /**
   * Tests {@link Ehcache#getAll(Set)} for
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

    final Ehcache<String, String> ehcache = this.getEhcache(this.loaderWriter);

    final Set<String> fetchKeys = fanIn(KEY_SET_A, KEY_SET_B);
    final Map<String, String> actual = ehcache.getAll(fetchKeys);

    assertThat(actual, equalTo(getEntryMap(fetchKeys)));

    verify(this.store).bulkComputeIfAbsent(eq(fetchKeys), getAnyIterableFunction());
    assertThat(fakeStore.getEntryMap(), equalTo(getEntryMap(KEY_SET_A, KEY_SET_B)));
    verify(this.loaderWriter, never()).loadAll(getAnyStringSet());
    verifyZeroInteractions(this.spiedResilienceStrategy);

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.GetOutcome.class));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoadingOutcome.class));
  }

  /**
   * Tests {@link Ehcache#getAll(Set)} for
   * <ul>
   *    <li>non-empty request key set</li> 
   *    <li>all {@link Store} entries match</li>
   *    <li>{@link Store#bulkComputeIfAbsent} throws before accessing loader</li>
   *    <li>no {@link CacheLoaderWriter} entries match</li>
   *    <li>some {@link CacheLoaderWriter#loadAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testGetAllStoreAllMatchCacheAccessExceptionBeforeLoaderNoMatchSomeFail() throws Exception {
    final FakeStore fakeStore = new FakeStore(getEntryMap(KEY_SET_A, KEY_SET_B));
    this.store = spy(fakeStore);
    doThrow(new CacheAccessException("")).when(this.store)
        .bulkComputeIfAbsent(getAnyStringSet(), getAnyIterableFunction());

    final FakeCacheLoaderWriter fakeLoader = new FakeCacheLoaderWriter(getEntryMap(KEY_SET_C, KEY_SET_F), KEY_SET_B);
    this.loaderWriter = spy(fakeLoader);

    final Ehcache<String, String> ehcache = this.getEhcache(this.loaderWriter);

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
        .getAllFailure(eq(fetchKeys), any(CacheAccessException.class), this.bulkExceptionCaptor.capture());
    assertThat(this.bulkExceptionCaptor.getValue().getSuccesses().keySet(), empty());
    assertThat(this.bulkExceptionCaptor.getValue().getFailures().keySet(), Matchers.<Set<?>>equalTo(fetchKeys));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.GetOutcome.class));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoadingOutcome.class));
  }

  /**
   * Tests {@link Ehcache#getAll(Set)} for
   * <ul>
   *    <li>non-empty request key set</li> 
   *    <li>all {@link Store} entries match</li>
   *    <li>{@link Store#bulkComputeIfAbsent} throws after accessing loader</li>
   *    <li>no {@link CacheLoaderWriter} entries match</li>
   *    <li>some {@link CacheLoaderWriter#loadAll(Iterable)} calls fail</li>
   * </ul>
   */
  // TODO: Reconcile against Issue #226 & Issue #227
  @Test
  public void testGetAllStoreAllMatchCacheAccessExceptionAfterLoaderNoMatchSomeFail() throws Exception {
    final FakeStore fakeStore = new FakeStore(getEntryMap(KEY_SET_A, KEY_SET_B), Collections.singleton("keyA3"));
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeLoader = new FakeCacheLoaderWriter(getEntryMap(KEY_SET_C, KEY_SET_F), KEY_SET_B);
    this.loaderWriter = spy(fakeLoader);

    final Ehcache<String, String> ehcache = this.getEhcache(this.loaderWriter);

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
        .getAllFailure(eq(fetchKeys), any(CacheAccessException.class), this.bulkExceptionCaptor.capture());
    assertThat(this.bulkExceptionCaptor.getValue().getSuccesses().keySet(), empty());
    assertThat(this.bulkExceptionCaptor.getValue().getFailures().keySet(), Matchers.<Set<?>>equalTo(fetchKeys));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.GetOutcome.class));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoadingOutcome.class));
  }

  /**
   * Tests {@link Ehcache#getAll(Set)} for
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

    final Ehcache<String, String> ehcache = this.getEhcache(this.loaderWriter);

    final Set<String> fetchKeys = fanIn(KEY_SET_A, KEY_SET_B);
    final Map<String, String> actual = ehcache.getAll(fetchKeys);

    assertThat(actual, equalTo(getEntryMap(fetchKeys)));

    verify(this.store).bulkComputeIfAbsent(eq(fetchKeys), getAnyIterableFunction());
    assertThat(fakeStore.getEntryMap(), equalTo(getEntryMap(KEY_SET_A, KEY_SET_B)));
    verify(this.loaderWriter, never()).loadAll(getAnyStringSet());
    verifyZeroInteractions(this.spiedResilienceStrategy);

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.GetOutcome.class));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoadingOutcome.class));
  }

  /**
   * Tests {@link Ehcache#getAll(Set)} for
   * <ul>
   *    <li>non-empty request key set</li> 
   *    <li>all {@link Store} entries match</li>
   *    <li>{@link Store#bulkComputeIfAbsent} throws before accessing loader</li>
   *    <li>some {@link CacheLoaderWriter} entries match</li>
   *    <li>no {@link CacheLoaderWriter#loadAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testGetAllStoreAllMatchCacheAccessExceptionBeforeLoaderSomeMatchNoneFail() throws Exception {
    final FakeStore fakeStore = new FakeStore(getEntryMap(KEY_SET_A, KEY_SET_B));
    this.store = spy(fakeStore);
    doThrow(new CacheAccessException("")).when(this.store)
        .bulkComputeIfAbsent(getAnyStringSet(), getAnyIterableFunction());

    final FakeCacheLoaderWriter fakeLoader = new FakeCacheLoaderWriter(getEntryMap(KEY_SET_B, KEY_SET_C));
    this.loaderWriter = spy(fakeLoader);

    final Ehcache<String, String> ehcache = this.getEhcache(this.loaderWriter);

    final Set<String> fetchKeys = fanIn(KEY_SET_A, KEY_SET_B);
    final Map<String, String> actual = ehcache.getAll(fetchKeys);

    final Map<String, String> expected = union(getNullEntryMap(KEY_SET_A), getEntryMap(KEY_SET_B));
    assertThat(actual, equalTo(expected));

    final InOrder ordered = inOrder(this.loaderWriter, this.spiedResilienceStrategy);
    verify(this.store).bulkComputeIfAbsent(eq(fetchKeys), getAnyIterableFunction());
    // ResilienceStrategy invoked: no assertion for Store content
    ordered.verify(this.loaderWriter, atLeast(1)).loadAll(fetchKeys);
    ordered.verify(this.spiedResilienceStrategy)
        .getAllFailure(eq(fetchKeys), eq(expected), any(CacheAccessException.class));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.GetOutcome.class));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoadingOutcome.class));
  }

  /**
   * Tests {@link Ehcache#getAll(Set)} for
   * <ul>
   *    <li>non-empty request key set</li> 
   *    <li>all {@link Store} entries match</li>
   *    <li>{@link Store#bulkComputeIfAbsent} throws after accessing loader</li>
   *    <li>some {@link CacheLoaderWriter} entries match</li>
   *    <li>no {@link CacheLoaderWriter#loadAll(Iterable)} calls fail</li>
   * </ul>
   */
  // TODO: Reconcile against Issue #226 & Issue #227
  @Test
  public void testGetAllStoreAllMatchCacheAccessExceptionAfterLoaderSomeMatchNoneFail() throws Exception {
    final FakeStore fakeStore = new FakeStore(getEntryMap(KEY_SET_A, KEY_SET_B), Collections.singleton("keyA3"));
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeLoader = new FakeCacheLoaderWriter(getEntryMap(KEY_SET_B, KEY_SET_C));
    this.loaderWriter = spy(fakeLoader);

    final Ehcache<String, String> ehcache = this.getEhcache(this.loaderWriter);

    final Set<String> fetchKeys = fanIn(KEY_SET_A, KEY_SET_B);
    final Map<String, String> actual = ehcache.getAll(fetchKeys);

    final Map<String, String> expected = union(getNullEntryMap(KEY_SET_A), getEntryMap(KEY_SET_B));
    assertThat(actual, equalTo(expected));

    final InOrder ordered = inOrder(this.loaderWriter, this.spiedResilienceStrategy);
    verify(this.store).bulkComputeIfAbsent(eq(fetchKeys), getAnyIterableFunction());
    // ResilienceStrategy invoked: no assertion for Store content
    ordered.verify(this.loaderWriter, atLeast(1)).loadAll(fetchKeys);
    ordered.verify(this.spiedResilienceStrategy)
        .getAllFailure(eq(fetchKeys), eq(expected), any(CacheAccessException.class));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.GetOutcome.class));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoadingOutcome.class));
  }

  /**
   * Tests {@link Ehcache#getAll(Set)} for
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

    final Ehcache<String, String> ehcache = this.getEhcache(this.loaderWriter);

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
  }

  /**
   * Tests {@link Ehcache#getAll(Set)} for
   * <ul>
   *    <li>non-empty request key set</li> 
   *    <li>all {@link Store} entries match</li>
   *    <li>{@link Store#bulkComputeIfAbsent} throws before accessing loader</li>
   *    <li>some {@link CacheLoaderWriter} entries match</li>
   *    <li>non-matching {@link CacheLoaderWriter#loadAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testGetAllStoreAllMatchCacheAccessExceptionBeforeLoaderSomeMatchDisjointFail() throws Exception {
    final FakeStore fakeStore = new FakeStore(getEntryMap(KEY_SET_A, KEY_SET_B));
    this.store = spy(fakeStore);
    doThrow(new CacheAccessException("")).when(this.store)
        .bulkComputeIfAbsent(getAnyStringSet(), getAnyIterableFunction());

    final FakeCacheLoaderWriter fakeLoader = new FakeCacheLoaderWriter(getEntryMap(KEY_SET_B, KEY_SET_C), KEY_SET_A);
    this.loaderWriter = spy(fakeLoader);

    final Ehcache<String, String> ehcache = this.getEhcache(this.loaderWriter);

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
        .getAllFailure(eq(fetchKeys), any(CacheAccessException.class), this.bulkExceptionCaptor.capture());
    assertThat(this.bulkExceptionCaptor.getValue().getSuccesses(), Matchers.<Map<?,?>>equalTo(Collections.emptyMap()));
    assertThat(this.bulkExceptionCaptor.getValue().getFailures().keySet(), Matchers.<Set<?>>equalTo(fetchKeys));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.GetOutcome.class));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoadingOutcome.class));
  }

  /**
   * Tests {@link Ehcache#getAll(Set)} for
   * <ul>
   *    <li>non-empty request key set</li> 
   *    <li>all {@link Store} entries match</li>
   *    <li>{@link Store#bulkComputeIfAbsent} throws after accessing loader</li>
   *    <li>some {@link CacheLoaderWriter} entries match</li>
   *    <li>non-matching {@link CacheLoaderWriter#loadAll(Iterable)} calls fail</li>
   * </ul>
   */
  // TODO: Reconcile against Issue #226 & Issue #227
  @Test
  public void testGetAllStoreAllMatchCacheAccessExceptionAfterLoaderSomeMatchDisjointFail() throws Exception {
    final FakeStore fakeStore = new FakeStore(getEntryMap(KEY_SET_A, KEY_SET_B), Collections.singleton("keyA3"));
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeLoader = new FakeCacheLoaderWriter(getEntryMap(KEY_SET_B, KEY_SET_C), KEY_SET_A);
    this.loaderWriter = spy(fakeLoader);

    final Ehcache<String, String> ehcache = this.getEhcache(this.loaderWriter);

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
        .getAllFailure(eq(fetchKeys), any(CacheAccessException.class), this.bulkExceptionCaptor.capture());
    assertThat(this.bulkExceptionCaptor.getValue().getSuccesses(), Matchers.<Map<?,?>>equalTo(Collections.emptyMap()));
    assertThat(this.bulkExceptionCaptor.getValue().getFailures().keySet(), Matchers.<Set<?>>equalTo(fetchKeys));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.GetOutcome.class));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoadingOutcome.class));
  }

  /**
   * Tests {@link Ehcache#getAll(Set)} for
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

    final Ehcache<String, String> ehcache = this.getEhcache(this.loaderWriter);

    final Set<String> fetchKeys = fanIn(KEY_SET_A, KEY_SET_B);
    final Map<String, String> actual = ehcache.getAll(fetchKeys);

    assertThat(actual, equalTo(getEntryMap(fetchKeys)));

    verify(this.store).bulkComputeIfAbsent(eq(fetchKeys), getAnyIterableFunction());
    assertThat(fakeStore.getEntryMap(), equalTo(getEntryMap(KEY_SET_A, KEY_SET_B)));
    verify(this.loaderWriter, never()).loadAll(getAnyStringSet());
    verifyZeroInteractions(this.spiedResilienceStrategy);

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.GetOutcome.class));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoadingOutcome.class));
  }

  /**
   * Tests {@link Ehcache#getAll(Set)} for
   * <ul>
   *    <li>non-empty request key set</li> 
   *    <li>all {@link Store} entries match</li>
   *    <li>{@link Store#bulkComputeIfAbsent} throws before accessing loader</li>
   *    <li>all {@link CacheLoaderWriter} entries match</li>
   *    <li>no {@link CacheLoaderWriter#loadAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testGetAllStoreAllMatchCacheAccessExceptionBeforeLoaderAllMatchNoneFail() throws Exception {
    final FakeStore fakeStore = new FakeStore(getEntryMap(KEY_SET_A, KEY_SET_B));
    this.store = spy(fakeStore);
    doThrow(new CacheAccessException("")).when(this.store)
        .bulkComputeIfAbsent(getAnyStringSet(), getAnyIterableFunction());

    final FakeCacheLoaderWriter fakeLoader = new FakeCacheLoaderWriter(getEntryMap(KEY_SET_A, KEY_SET_B));
    this.loaderWriter = spy(fakeLoader);

    final Ehcache<String, String> ehcache = this.getEhcache(this.loaderWriter);

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
        .getAllFailure(eq(fetchKeys), eq(expected), any(CacheAccessException.class));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.GetOutcome.class));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoadingOutcome.class));
  }

  /**
   * Tests {@link Ehcache#getAll(Set)} for
   * <ul>
   *    <li>non-empty request key set</li> 
   *    <li>all {@link Store} entries match</li>
   *    <li>{@link Store#bulkComputeIfAbsent} throws after accessing loader</li>
   *    <li>all {@link CacheLoaderWriter} entries match</li>
   *    <li>no {@link CacheLoaderWriter#loadAll(Iterable)} calls fail</li>
   * </ul>
   */
  // TODO: Reconcile against Issue #227
  @Test
  public void testGetAllStoreAllMatchCacheAccessExceptionAfterLoaderAllMatchNoneFail() throws Exception {
    final FakeStore fakeStore = new FakeStore(getEntryMap(KEY_SET_A, KEY_SET_B), Collections.singleton("keyA3"));
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeLoader = new FakeCacheLoaderWriter(getEntryMap(KEY_SET_A, KEY_SET_B));
    this.loaderWriter = spy(fakeLoader);

    final Ehcache<String, String> ehcache = this.getEhcache(this.loaderWriter);

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
        .getAllFailure(eq(fetchKeys), eq(expected), any(CacheAccessException.class));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.GetOutcome.class));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoadingOutcome.class));
  }

  /**
   * Gets an initialized {@link Ehcache Ehcache} instance using the
   * {@link CacheLoaderWriter} provided.
   *
   * @param cacheLoaderWriter the {@code CacheLoaderWriter} to use; may be {@code null}
   *
   * @return a new {@code Ehcache} instance
   */
  private Ehcache<String, String> getEhcache(final CacheLoaderWriter<String, String> cacheLoaderWriter) {
    final Ehcache<String, String> ehcache = new Ehcache<String, String>(CACHE_CONFIGURATION, this.store, cacheLoaderWriter, LoggerFactory.getLogger(Ehcache.class + "-" + "EhcacheBasicGetAllTest"));
    ehcache.init();
    assertThat("cache not initialized", ehcache.getStatus(), is(Status.AVAILABLE));
    this.spiedResilienceStrategy = this.setResilienceStrategySpy(ehcache);
    return ehcache;
  }

  /**
   * Returns a Mockito {@code any} Matcher for {@code java.util.Set<String>}.
   *
   * @return a Mockito {@code any} matcher for {@code Set<String>}.
   */
  @SuppressWarnings("unchecked")
  private static Set<? extends String> getAnyStringSet() {
    return any(Set.class);   // unchecked
  }

  /**
   * Returns a Mockito {@code any} Matcher for {@link org.ehcache.function.Function} over {@code java.lang.Iterable}.
   *
   * @return a Mockito {@code any} matcher for {@code Function}.
   */
  @SuppressWarnings("unchecked")
  private static Function<Iterable<? extends String>, Iterable<? extends Map.Entry<? extends String, ? extends String>>> getAnyIterableFunction() {
    return any(Function.class);   // unchecked
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
