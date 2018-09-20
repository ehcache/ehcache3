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

import org.ehcache.Status;
import org.ehcache.core.spi.store.Store;
import org.ehcache.core.statistics.CacheOperationOutcomes;
import org.ehcache.core.statistics.BulkOps;
import org.ehcache.spi.resilience.StoreAccessException;
import org.hamcrest.Matchers;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.EnumSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Function;

import static org.ehcache.core.EhcacheBasicBulkUtil.KEY_SET_A;
import static org.ehcache.core.EhcacheBasicBulkUtil.KEY_SET_B;
import static org.ehcache.core.EhcacheBasicBulkUtil.KEY_SET_C;
import static org.ehcache.core.EhcacheBasicBulkUtil.fanIn;
import static org.ehcache.core.EhcacheBasicBulkUtil.getEntryMap;
import static org.ehcache.core.EhcacheBasicBulkUtil.getNullEntryMap;
import static org.ehcache.core.EhcacheBasicBulkUtil.union;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

/**
 * Provides testing of basic GET_ALL operations on an {@code Ehcache}.
 *
 * @author Clifford W. Johnson
 */
@SuppressWarnings("ThrowableResultOfMethodCallIgnored")
public class EhcacheBasicGetAllTest extends EhcacheBasicCrudBase {

  @Test
  public void testGetAllNull() throws Exception {
    final Ehcache<String, String> ehcache = this.getEhcache();
    try {
      ehcache.getAll(null);
      fail();
    } catch (NullPointerException e) {
      // Expected
    }
  }

  @Test
  public void testGetAllNullKey() throws Exception {
    final Set<String> keys = new LinkedHashSet<>();
    for (final String key : KEY_SET_A) {
      keys.add(key);
      if ("keyA2".equals(key)) {
        keys.add(null);     // Add a null element
      }
    }
    final Ehcache<String, String> ehcache = this.getEhcache();
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
   *    <li>no {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testGetAllEmptyRequestNoLoader() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.<String, String>emptyMap());
    this.store = spy(fakeStore);

    final Ehcache<String, String> ehcache = this.getEhcache();

    final Map<String, String> actual = ehcache.getAll(Collections.<String>emptySet());
    assertThat(actual, is(notNullValue()));
    assertThat(actual.isEmpty(), is(true));

    verify(this.store, never()).bulkComputeIfAbsent(eq(Collections.<String>emptySet()), getAnyIterableFunction());
    verify(this.resilienceStrategy, never()).getAllFailure(eq(Collections.<String>emptySet()), any(StoreAccessException.class));

    validateStatsNoneof(ehcache);
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetAllOutcome.SUCCESS));
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

    final Ehcache<String, String> ehcache = this.getEhcache();

    final Map<String, String> actual = ehcache.getAll(KEY_SET_A);
    assertThat(actual, equalTo(getNullEntryMap(KEY_SET_A)));

    verify(this.store).bulkComputeIfAbsent(eq(KEY_SET_A), getAnyIterableFunction());
    assertThat(fakeStore.getEntryMap(), equalTo(getEntryMap(KEY_SET_B)));
    verifyZeroInteractions(this.resilienceStrategy);

    validateStatsNoneof(ehcache);
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetAllOutcome.SUCCESS));
    validateBulkCounters(ehcache, 0, KEY_SET_A.size());
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
  public void testGetAllStoreAllMatchStoreAccessExceptionBeforeNoLoader() throws Exception {
    final FakeStore fakeStore = new FakeStore(getEntryMap(KEY_SET_A, KEY_SET_B));
    this.store = spy(fakeStore);
    doThrow(new StoreAccessException("")).when(this.store)
        .bulkComputeIfAbsent(getAnyStringSet(), getAnyIterableFunction());

    final Ehcache<String, String> ehcache = this.getEhcache();

    final Set<String> fetchKeys = fanIn(KEY_SET_A, KEY_SET_B);
    ehcache.getAll(fetchKeys);

    verify(this.store).bulkComputeIfAbsent(eq(fetchKeys), getAnyIterableFunction());
    // ResilienceStrategy invoked: no assertion for Store content
    verify(this.resilienceStrategy).getAllFailure(eq(fetchKeys), any(StoreAccessException.class));

    validateStatsNoneof(ehcache);
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetAllOutcome.FAILURE));
    validateBulkCounters(ehcache, 0, 0);
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

    final Ehcache<String, String> ehcache = this.getEhcache();

    final Set<String> fetchKeys = fanIn(KEY_SET_A, KEY_SET_B);
    final Map<String, String> actual = ehcache.getAll(fetchKeys);

    assertThat(actual, equalTo(getEntryMap(fetchKeys)));

    verify(this.store).bulkComputeIfAbsent(eq(fetchKeys), getAnyIterableFunction());
    assertThat(fakeStore.getEntryMap(), equalTo(getEntryMap(KEY_SET_A, KEY_SET_B)));
    verifyZeroInteractions(this.resilienceStrategy);

    validateStatsNoneof(ehcache);
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetAllOutcome.SUCCESS));
    validateBulkCounters(ehcache, fetchKeys.size(), 0);
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
  public void testGetAllStoreNoMatchStoreAccessExceptionBeforeNoLoader() throws Exception {
    final FakeStore fakeStore = new FakeStore(getEntryMap(KEY_SET_B));
    this.store = spy(fakeStore);
    doThrow(new StoreAccessException("")).when(this.store)
        .bulkComputeIfAbsent(getAnyStringSet(), getAnyIterableFunction());

    final Ehcache<String, String> ehcache = this.getEhcache();

    ehcache.getAll(KEY_SET_A);

    verify(this.store).bulkComputeIfAbsent(eq(KEY_SET_A), getAnyIterableFunction());
    // ResilienceStrategy invoked: no assertion for Store content
    verify(this.resilienceStrategy).getAllFailure(eq(KEY_SET_A), any(StoreAccessException.class));

    validateStatsNoneof(ehcache);
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetAllOutcome.FAILURE));
    validateBulkCounters(ehcache, 0, 0);
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

    final Ehcache<String, String> ehcache = this.getEhcache();

    final Set<String> fetchKeys = fanIn(KEY_SET_A, KEY_SET_C);
    final Map<String, String> actual = ehcache.getAll(fetchKeys);

    assertThat(actual, equalTo(union(getEntryMap(KEY_SET_A), getNullEntryMap(KEY_SET_C))));

    verify(this.store).bulkComputeIfAbsent(eq(fetchKeys), getAnyIterableFunction());
    assertThat(fakeStore.getEntryMap(), equalTo(getEntryMap(KEY_SET_A, KEY_SET_B)));
    verifyZeroInteractions(this.resilienceStrategy);

    validateStatsNoneof(ehcache);
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetAllOutcome.SUCCESS));
    validateBulkCounters(ehcache, KEY_SET_A.size(), KEY_SET_C.size());
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
  public void testGetAllStoreSomeMatchStoreAccessExceptionBeforeNoLoader() throws Exception {
    final FakeStore fakeStore = new FakeStore(getEntryMap(KEY_SET_A, KEY_SET_B));
    this.store = spy(fakeStore);
    doThrow(new StoreAccessException("")).when(this.store)
        .bulkComputeIfAbsent(getAnyStringSet(), getAnyIterableFunction());

    final Ehcache<String, String> ehcache = this.getEhcache();

    final Set<String> fetchKeys = fanIn(KEY_SET_A, KEY_SET_C);
    ehcache.getAll(fetchKeys);

    verify(this.store).bulkComputeIfAbsent(eq(fetchKeys), getAnyIterableFunction());
    // ResilienceStrategy invoked: no assertion for Store content
    verify(this.resilienceStrategy).getAllFailure(eq(fetchKeys), any(StoreAccessException.class));

    validateStatsNoneof(ehcache);
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetAllOutcome.FAILURE));
    validateBulkCounters(ehcache, 0, 0);
  }

  private void validateStatsNoneof(Ehcache<String, String> cache) {
    validateStats(cache, EnumSet.noneOf(CacheOperationOutcomes.GetOutcome.class));
  }

  /**
   * Gets an initialized {@link Ehcache Ehcache} instance
   *
   * @return a new {@code Ehcache} instance
   */
  @SuppressWarnings("unchecked")
  private Ehcache<String, String> getEhcache() {
    final Ehcache<String, String> ehcache = new Ehcache<>(CACHE_CONFIGURATION, this.store, resilienceStrategy, cacheEventDispatcher, LoggerFactory
      .getLogger(Ehcache.class + "-" + "EhcacheBasicGetAllTest"));
    ehcache.init();
    assertThat("cache not initialized", ehcache.getStatus(), Matchers.is(Status.AVAILABLE));
    return ehcache;
  }

  static void validateBulkCounters(InternalCache<?, ?> ehcache, int expectedHitCount, int expectedMissCount) {
    LongAdder hitAdder = ehcache.getBulkMethodEntries().get(BulkOps.GET_ALL_HITS);
    LongAdder missAdder = ehcache.getBulkMethodEntries().get(BulkOps.GET_ALL_MISS);
    int hitCount = hitAdder == null ? 0 : hitAdder.intValue();
    int missCount = missAdder == null ? 0 : missAdder.intValue();
    assertThat(hitCount, is(expectedHitCount));
    assertThat(missCount, is(expectedMissCount));
  }

  /**
   * Returns a Mockito {@code any} Matcher for {@code java.util.Set<String>}.
   *
   * @return a Mockito {@code any} matcher for {@code Set<String>}.
   */
  @SuppressWarnings("unchecked")
  static Set<? extends String> getAnyStringSet() {
    return any(Set.class);   // unchecked
  }

  /**
   * Returns a Mockito {@code any} Matcher for {@link Function} over {@code java.lang.Iterable}.
   *
   * @return a Mockito {@code any} matcher for {@code Function}.
   */
  @SuppressWarnings("unchecked")
  static Function<Iterable<? extends String>, Iterable<? extends Map.Entry<? extends String, ? extends String>>> getAnyIterableFunction() {
    return any(Function.class);   // unchecked
  }
}
