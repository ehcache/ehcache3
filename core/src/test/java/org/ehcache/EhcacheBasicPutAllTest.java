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

import org.ehcache.exceptions.BulkCacheWritingException;
import org.ehcache.exceptions.CacheAccessException;
import org.ehcache.function.Function;
import org.ehcache.spi.cache.Store;
import org.ehcache.statistics.CacheOperationOutcomes;
import org.hamcrest.Matchers;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Formatter;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import static org.ehcache.EhcacheBasicBulkUtil.*;

import org.ehcache.spi.loaderwriter.CacheLoaderWriter;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isIn;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

/**
 * Provides testing of basic PUT_ALL operations on an {@code Ehcache}.
 * <p/>
 * In an effort compromise, this class intentionally omits test cases in which
 * the {@code Store} is pre-populated with no entries, pre-populated only with
 * entries having keys not in the {@code putAll} request map, and pre-populated
 * with entries for all keys in the {@code putAll} request map.  This reduces
 * the potential test cases by about 70% without, hopefully, compromising code
 * coverage.
 * <p/>
 * Since the processing in {@link Ehcache#putAll} relies on non-deterministically ordered Maps in several stages
 * of processing, the result of {@code putAll} when handling failures is *not* deterministic -- changes in
 * iteration order of the {@code putAll} request map can change the results of the {@code putAll} operation under
 * error scenarios.  The test assertions attempt to confirm results in aggregate over successes and failures and
 * do not specify specific success and failures for each test.
 *
 * @author Clifford W. Johnson
 */
@SuppressWarnings("ThrowableResultOfMethodCallIgnored")
public class EhcacheBasicPutAllTest extends EhcacheBasicCrudBase {

  @Mock
  protected CacheLoaderWriter<String, String> cacheLoaderWriter;

  /**
   * A Mockito {@code ArgumentCaptor} for the {@code Set} argument to the
   * {@link Store#bulkCompute(Set, Function, org.ehcache.function.NullaryFunction)
   *    Store.bulkCompute(Set, Function, NullaryFunction} method.
   */
  @Captor
  private ArgumentCaptor<Set<String>> bulkComputeSetCaptor;

  /**
   * A Mockito {@code ArgumentCaptor} for the
   * {@link org.ehcache.exceptions.BulkCacheWritingException BulkCacheWritingException}
   * provided to the
   * {@link org.ehcache.resilience.ResilienceStrategy#putAllFailure(Map, CacheAccessException, BulkCacheWritingException)}
   *    ResilienceStrategy.putAllFailure(Iterable, CacheAccessException, BulkCacheWritingException)} method.
   */
  @Captor
  private ArgumentCaptor<BulkCacheWritingException> bulkExceptionCaptor;

  @Test
  public void testPutAllNull() throws Exception {
    final Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    final FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);

    final Ehcache<String, String> ehcache = this.getEhcache(null);
    try {
      ehcache.putAll(null);
      fail();
    } catch (NullPointerException e) {
      // Expected
    }

    assertThat(fakeStore.getEntryMap(), equalTo(originalStoreContent));
  }

  @Test
  public void testPutAllNullKey() throws Exception {
    final Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    final FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);

    final Map<String, String> entries = new LinkedHashMap<String, String>();
    for (final Map.Entry<String, String> entry : getEntryMap(KEY_SET_A).entrySet()) {
      final String key = entry.getKey();
      entries.put(key, entry.getValue());
      if ("keyA2".equals(key)) {
        entries.put(null, "nullKey");
      }
    }
    final Ehcache<String, String> ehcache = this.getEhcache(null);
    try {
      ehcache.putAll(entries);
      fail();
    } catch (NullPointerException e) {
      // Expected
    }

    assertThat(fakeStore.getEntryMap(), equalTo(originalStoreContent));
  }

  @Test
  public void testPutAllNullValue() throws Exception {
    final Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    final FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);

    final Map<String, String> entries = new LinkedHashMap<String, String>();
    for (final Map.Entry<String, String> entry : getEntryMap(KEY_SET_A).entrySet()) {
      final String key = entry.getKey();
      entries.put(key, entry.getValue());
      if ("keyA2".equals(key)) {
        entries.put("keyA2a", null);
      }
    }
    final Ehcache<String, String> ehcache = this.getEhcache(null);
    try {
      ehcache.putAll(entries);
      fail();
    } catch (NullPointerException e) {
      // Expected
    }

    assertThat(fakeStore.getEntryMap(), equalTo(originalStoreContent));
  }

  /**
   * Tests {@link Ehcache#putAll(Map)} for
   * <ul>
   *    <li>empty request map</li>
   *    <li>populated {@code Store} (keys not relevant)</li>
   *    <li>no {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testPutAllEmptyRequestNoWriter() throws Exception {
    final Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    final FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);

    final Ehcache<String, String> ehcache = this.getEhcache(null);
    ehcache.putAll(Collections.<String, String>emptyMap());

    verify(this.store).bulkCompute(eq(Collections.<String>emptySet()), getAnyEntryIterableFunction());
    assertThat(fakeStore.getEntryMap(), equalTo(originalStoreContent));
    verifyZeroInteractions(this.spiedResilienceStrategy);

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.PutOutcome.class));
  }

  /**
   * Tests {@link Ehcache#putAll(Map)} for
   * <ul>
   *    <li>empty request map</li>
   *    <li>populated {@code Store} (keys not relevant)</li>
   *    <li>{@link Store#bulkCompute} throws before accessing writer</li>
   *    <li>no {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testPutAllEmptyRequestCacheAccessExceptionBeforeNoWriter() throws Exception {
    final Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    final FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);
    doThrow(new CacheAccessException("")).when(this.store)
        .bulkCompute(getAnyStringSet(), getAnyEntryIterableFunction());

    final Ehcache<String, String> ehcache = this.getEhcache(null);
    ehcache.putAll(Collections.<String, String>emptyMap());

    verify(this.store).bulkCompute(eq(Collections.<String>emptySet()), getAnyEntryIterableFunction());
    // ResilienceStrategy invoked; no assertions about Store content
    verify(this.spiedResilienceStrategy)
        .putAllFailure(eq(Collections.<String, String>emptyMap()), any(CacheAccessException.class));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.PutOutcome.class));
  }

  /**
   * Tests {@link Ehcache#putAll(Map)} for
   * <ul>
   *    <li>empty request map</li>
   *    <li>populated {@code Store} (keys not relevant)</li>
   *    <li>populated {@code CacheLoaderWriter} (keys not relevant)</li>
   * </ul>
   */
  @Test
  public void testPutAllEmptyRequestWriterNoneFail() throws Exception {
    final Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    final FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalStoreContent);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);

    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);
    ehcache.putAll(Collections.<String, String>emptyMap());

    verify(this.store).bulkCompute(eq(Collections.<String>emptySet()), getAnyEntryIterableFunction());
    assertThat(fakeStore.getEntryMap(), equalTo(originalStoreContent));
    assertThat(fakeLoaderWriter.getEntryMap(), equalTo(originalStoreContent));
    verifyZeroInteractions(this.spiedResilienceStrategy);

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.PutOutcome.class));
  }

  /**
   * Tests {@link Ehcache#putAll(Map)} for
   * <ul>
   *    <li>empty request map</li>
   *    <li>populated {@code Store} (keys not relevant)</li>
   *    <li>{@link Store#bulkCompute} throws before accessing writer</li>
   *    <li>populated {@code CacheLoaderWriter} (keys not relevant)</li>
   * </ul>
   */
  @Test
  public void testPutAllEmptyRequestCacheAccessExceptionBeforeWriterNoneFail() throws Exception {
    final Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    final FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);
    doThrow(new CacheAccessException("")).when(this.store)
        .bulkCompute(getAnyStringSet(), getAnyEntryIterableFunction());

    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalStoreContent);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);

    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);
    ehcache.putAll(Collections.<String, String>emptyMap());

    verify(this.store).bulkCompute(eq(Collections.<String>emptySet()), getAnyEntryIterableFunction());
    // ResilienceStrategy invoked; no assertions about Store content
    assertThat(fakeLoaderWriter.getEntryMap(), equalTo(originalStoreContent));
    verify(this.spiedResilienceStrategy)
        .putAllFailure(eq(Collections.<String, String>emptyMap()), any(CacheAccessException.class));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.PutOutcome.class));
  }

  /**
   * Tests {@link Ehcache#putAll(Map)} for
   * <ul>
   *    <li>empty request map</li>
   *    <li>populated {@code Store} (keys not relevant)</li>
   *    <li>populated {@code CacheLoaderWriter} (keys not relevant)</li>
   *    <li>all {@link CacheLoaderWriter#writeAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testPutAllEmptyRequestWriterAllFail() throws Exception {
    final Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    final FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalStoreContent);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);
    doThrow(new Exception("writeAll failed")).when(this.cacheLoaderWriter).writeAll(getAnyEntryIterable());

    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);
    ehcache.putAll(Collections.<String, String>emptyMap());

    verify(this.store).bulkCompute(eq(Collections.<String>emptySet()), getAnyEntryIterableFunction());
    assertThat(fakeStore.getEntryMap(), equalTo(originalStoreContent));
    verifyZeroInteractions(this.spiedResilienceStrategy);

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.PutOutcome.class));
  }

  /**
   * Tests {@link Ehcache#putAll(Map)} for
   * <ul>
   *    <li>empty request map</li>
   *    <li>populated {@code Store} (keys not relevant)</li>
   *    <li>{@link Store#bulkCompute} throws before accessing writer</li>
   *    <li>populated {@code CacheLoaderWriter} (keys not relevant)</li>
   *    <li>all {@link CacheLoaderWriter#writeAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testPutAllEmptyRequestCacheAccessExceptionBeforeWriterAllFail() throws Exception {
    final Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    final FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);
    doThrow(new CacheAccessException("")).when(this.store)
        .bulkCompute(getAnyStringSet(), getAnyEntryIterableFunction());

    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalStoreContent);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);
    doThrow(new Exception("writeAll failed")).when(this.cacheLoaderWriter).writeAll(getAnyEntryIterable());

    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);
    ehcache.putAll(Collections.<String, String>emptyMap());

    verify(this.store).bulkCompute(eq(Collections.<String>emptySet()), getAnyEntryIterableFunction());
    // ResilienceStrategy invoked; no assertions about Store content
    verify(this.spiedResilienceStrategy)
        .putAllFailure(eq(Collections.<String, String>emptyMap()), any(CacheAccessException.class));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.PutOutcome.class));
  }

  /**
   * Tests {@link Ehcache#putAll(Map)} for
   * <ul>
   *    <li>non-empty request map</li>
   *    <li>populated {@code Store} - some keys overlap request</li>
   *    <li>no {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testPutAllStoreSomeOverlapNoWriter() throws Exception {
    final Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    final FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);

    final Ehcache<String, String> ehcache = this.getEhcache(null);

    final Map<String, String> contentUpdates = getAltEntryMap("new_", fanIn(KEY_SET_A, KEY_SET_C));
    ehcache.putAll(contentUpdates);

    verify(this.store, atLeast(1)).bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), equalTo(contentUpdates.keySet()));
    assertThat(fakeStore.getEntryMap(), equalTo(union(originalStoreContent, contentUpdates)));
    verifyZeroInteractions(this.spiedResilienceStrategy);

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.PutOutcome.class));
  }

  /**
   * Tests {@link Ehcache#putAll(Map)} for
   * <ul>
   *    <li>non-empty request map</li>
   *    <li>populated {@code Store} - some keys overlap request</li>
   *    <li>{@link Store#bulkCompute} throws before accessing writer</li>
   *    <li>no {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testPutAllStoreSomeOverlapCacheAccessExceptionBeforeNoWriter() throws Exception {
    final Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    final FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);
    doThrow(new CacheAccessException("")).when(this.store)
        .bulkCompute(getAnyStringSet(), getAnyEntryIterableFunction());

    final Ehcache<String, String> ehcache = this.getEhcache(null);

    final Map<String, String> contentUpdates = getAltEntryMap("new_", fanIn(KEY_SET_A, KEY_SET_C));
    ehcache.putAll(contentUpdates);

    final InOrder ordered = inOrder(this.store, this.spiedResilienceStrategy);
    ordered.verify(this.store, atLeast(1)).bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), everyItem(isIn(contentUpdates.keySet())));
    // ResilienceStrategy invoked; no assertions about Store content
    ordered.verify(this.spiedResilienceStrategy)
        .putAllFailure(eq(contentUpdates), any(CacheAccessException.class));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.PutOutcome.class));
  }

  /**
   * Tests {@link Ehcache#putAll(Map)} for
   * <ul>
   *    <li>non-empty request map</li>
   *    <li>populated {@code Store} - some keys overlap request</li>
   *    <li>{@link Store#bulkCompute} throws after accessing writer</li>
   *    <li>no {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testPutAllStoreSomeOverlapCacheAccessExceptionAfterNoWriter() throws Exception {
    final Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    final FakeStore fakeStore = new FakeStore(originalStoreContent, Collections.singleton("keyA3"));
    this.store = spy(fakeStore);

    final Ehcache<String, String> ehcache = this.getEhcache(null);

    final Map<String, String> contentUpdates = getAltEntryMap("new_", fanIn(KEY_SET_A, KEY_SET_C));
    ehcache.putAll(contentUpdates);

    final InOrder ordered = inOrder(this.store, this.spiedResilienceStrategy);
    ordered.verify(this.store, atLeast(1)).bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), everyItem(isIn(contentUpdates.keySet())));
    // ResilienceStrategy invoked; no assertions about Store content
    ordered.verify(this.spiedResilienceStrategy)
        .putAllFailure(eq(contentUpdates), any(CacheAccessException.class));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.PutOutcome.class));
  }

  /**
   * Tests {@link Ehcache#putAll(Map)} for
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

    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    final Map<String, String> contentUpdates = getAltEntryMap("new_", fanIn(KEY_SET_A, KEY_SET_C));
    ehcache.putAll(contentUpdates);

    verify(this.store, atLeast(1)).bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), equalTo(contentUpdates.keySet()));
    assertThat(fakeStore.getEntryMap(), equalTo(union(originalStoreContent, contentUpdates)));
    assertThat(fakeLoaderWriter.getEntryMap(), equalTo(union(originalWriterContent, contentUpdates)));
    verifyZeroInteractions(this.spiedResilienceStrategy);

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.PutOutcome.class));
  }

  /**
   * Tests {@link Ehcache#putAll(Map)} for
   * <ul>
   *    <li>non-empty request map</li>
   *    <li>populated {@code Store} - some keys overlap request</li>
   *    <li>{@link Store#bulkCompute} throws before accessing writer</li>
   *    <li>populated {@code CacheLoaderWriter} - some keys overlap</li>
   *    <li>no {@link CacheLoaderWriter#writeAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testPutAllStoreSomeOverlapCacheAccessExceptionBeforeWriterNoOverlapNoneFail() throws Exception {
    final Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    final FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);
    doThrow(new CacheAccessException("")).when(this.store)
        .bulkCompute(getAnyStringSet(), getAnyEntryIterableFunction());

    final Map<String, String> originalWriterContent = getEntryMap(KEY_SET_A, KEY_SET_B, KEY_SET_D);
    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);

    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    final Map<String, String> contentUpdates = getAltEntryMap("new_", fanIn(KEY_SET_A, KEY_SET_C));
    ehcache.putAll(contentUpdates);

    final InOrder ordered = inOrder(this.store, this.cacheLoaderWriter, this.spiedResilienceStrategy);
    ordered.verify(this.store, atLeast(1)).bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), everyItem(isIn(contentUpdates.keySet())));
    // ResilienceStrategy invoked; no assertions about Store content
    ordered.verify(this.cacheLoaderWriter, atLeast(1)).writeAll(getAnyEntryIterable());
    assertThat(fakeLoaderWriter.getEntryMap(), equalTo(union(originalWriterContent, contentUpdates)));
    ordered.verify(this.spiedResilienceStrategy)
        .putAllFailure(eq(contentUpdates), any(CacheAccessException.class));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.PutOutcome.class));
  }

  /**
   * Tests {@link Ehcache#putAll(Map)} for
   * <ul>
   *    <li>non-empty request map</li>
   *    <li>populated {@code Store} - some keys overlap request</li>
   *    <li>{@link Store#bulkCompute} throws after accessing writer</li>
   *    <li>populated {@code CacheLoaderWriter} - some keys overlap</li>
   *    <li>no {@link CacheLoaderWriter#writeAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testPutAllStoreSomeOverlapCacheAccessExceptionAfterWriterNoOverlapNoneFail() throws Exception {
    final Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    final FakeStore fakeStore = new FakeStore(originalStoreContent, Collections.singleton("keyA3"));
    this.store = spy(fakeStore);

    final Map<String, String> originalWriterContent = getEntryMap(KEY_SET_A, KEY_SET_B, KEY_SET_D);
    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);

    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

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
        .putAllFailure(eq(contentUpdates), any(CacheAccessException.class));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.PutOutcome.class));
  }

  /**
   * Tests {@link Ehcache#putAll(Map)} for
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

    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

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
  }

  /**
   * Tests {@link Ehcache#putAll(Map)} for
   * <ul>
   *    <li>non-empty request map</li>
   *    <li>populated {@code Store} - some keys overlap request</li>
   *    <li>{@link Store#bulkCompute} throws before accessing writer</li>
   *    <li>populated {@code CacheLoaderWriter} - some keys overlap</li>
   *    <li>some {@link CacheLoaderWriter#writeAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testPutAllStoreSomeOverlapCacheAccessExceptionBeforeWriterNoOverlapSomeFail() throws Exception {
    final Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    final FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);
    doThrow(new CacheAccessException("")).when(this.store)
        .bulkCompute(getAnyStringSet(), getAnyEntryIterableFunction());

    final Map<String, String> originalWriterContent = getEntryMap(KEY_SET_A, KEY_SET_B, KEY_SET_D);
    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent, KEY_SET_C);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);

    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

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
        .putAllFailure(eq(contentUpdates), any(CacheAccessException.class), this.bulkExceptionCaptor.capture());
    assertThat(this.bulkExceptionCaptor.getValue().getSuccesses(), Matchers.<Set<?>>equalTo(expectedSuccesses.keySet()));
    assertThat(this.bulkExceptionCaptor.getValue().getFailures().keySet(), Matchers.<Set<?>>equalTo(expectedFailures));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.PutOutcome.class));
  }

  /**
   * Tests {@link Ehcache#putAll(Map)} for
   * <ul>
   *    <li>non-empty request map</li>
   *    <li>populated {@code Store} - some keys overlap request</li>
   *    <li>{@link Store#bulkCompute} throws after accessing writer</li>
   *    <li>populated {@code CacheLoaderWriter} - some keys overlap</li>
   *    <li>some {@link CacheLoaderWriter#writeAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testPutAllStoreSomeOverlapCacheAccessExceptionAfterWriterNoOverlapSomeFail() throws Exception {
    final Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    final FakeStore fakeStore = new FakeStore(originalStoreContent, Collections.singleton("keyA3"));
    this.store = spy(fakeStore);

    final Map<String, String> originalWriterContent = getEntryMap(KEY_SET_A, KEY_SET_B, KEY_SET_D);
    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent, KEY_SET_C);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);

    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

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
        .putAllFailure(eq(contentUpdates), any(CacheAccessException.class), this.bulkExceptionCaptor.capture());
    assertThat(this.bulkExceptionCaptor.getValue().getSuccesses(), Matchers.<Set<?>>equalTo(expectedSuccesses.keySet()));
    assertThat(this.bulkExceptionCaptor.getValue().getFailures().keySet(), Matchers.<Set<?>>equalTo(expectedFailures));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.PutOutcome.class));
  }

  /**
   * Tests {@link Ehcache#putAll(Map)} for
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

    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

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
  }

  /**
   * Tests {@link Ehcache#putAll(Map)} for
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
  public void testPutAllStoreSomeOverlapCacheAccessExceptionBeforeWriterNoOverlapSomeFailWithAbort() throws Exception {
    final Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    final FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);
    doThrow(new CacheAccessException("")).when(this.store)
        .bulkCompute(getAnyStringSet(), getAnyEntryIterableFunction());

    final Map<String, String> originalWriterContent = getEntryMap(KEY_SET_A, KEY_SET_B, KEY_SET_D);
    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent, KEY_SET_C);
    fakeLoaderWriter.setCompleteFailureKey("keyC4");
    this.cacheLoaderWriter = spy(fakeLoaderWriter);

    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

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
        .putAllFailure(eq(contentUpdates), any(CacheAccessException.class), this.bulkExceptionCaptor.capture());

    @SuppressWarnings("unchecked")
    final Set<String> bcweSuccesses = (Set<String>)this.bulkExceptionCaptor.getValue().getSuccesses();
    @SuppressWarnings("unchecked")
    final Map<String, Exception> bcweFailures = (Map<String, Exception>)this.bulkExceptionCaptor.getValue().getFailures();

    assertThat(union(bcweSuccesses, bcweFailures.keySet()), equalTo(contentUpdates.keySet()));
    assertThat(Collections.disjoint(bcweSuccesses, bcweFailures.keySet()), is(true));
    assertThat(bcweSuccesses, everyItem(isIn(expectedSuccesses.keySet())));
    assertThat(expectedFailures, everyItem(isIn(bcweFailures.keySet())));
    // TODO: Confirm correctness - BulkCacheWritingException miscategorizes success as failure  (Issue #238)
    assertThat(copyWithout(fakeLoaderWriter.getEntryMap(), bcweFailures.keySet()),
        equalTo(copyWithout(union(originalWriterContent, copyOnly(contentUpdates, bcweSuccesses)), bcweFailures.keySet())));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.PutOutcome.class));

    this.dumpResults(fakeStore, originalStoreContent, fakeLoaderWriter, originalWriterContent, contentUpdates, expectedFailures,
        expectedSuccesses, bcweSuccesses, bcweFailures);
  }

  /**
   * Tests {@link Ehcache#putAll(Map)} for
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
  public void testPutAllStoreSomeOverlapCacheAccessExceptionAfterWriterNoOverlapSomeFailWithAbort() throws Exception {
    final Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    final FakeStore fakeStore = new FakeStore(originalStoreContent, Collections.singleton("keyA3"));
    this.store = spy(fakeStore);

    final Map<String, String> originalWriterContent = getEntryMap(KEY_SET_A, KEY_SET_B, KEY_SET_D);
    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent, KEY_SET_C);
    fakeLoaderWriter.setCompleteFailureKey("keyC4");
    this.cacheLoaderWriter = spy(fakeLoaderWriter);

    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

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
        .putAllFailure(eq(contentUpdates), any(CacheAccessException.class), this.bulkExceptionCaptor.capture());

    @SuppressWarnings("unchecked")
    final Set<String> bcweSuccesses = (Set<String>)this.bulkExceptionCaptor.getValue().getSuccesses();
    @SuppressWarnings("unchecked")
    final Map<String, Exception> bcweFailures = (Map<String, Exception>)this.bulkExceptionCaptor.getValue().getFailures();

    assertThat(union(bcweSuccesses, bcweFailures.keySet()), equalTo(contentUpdates.keySet()));
    assertThat(Collections.disjoint(bcweSuccesses, bcweFailures.keySet()), is(true));
    assertThat(bcweSuccesses, everyItem(isIn(expectedSuccesses.keySet())));
    assertThat(expectedFailures, everyItem(isIn(bcweFailures.keySet())));
    // TODO: Confirm correctness - BulkCacheWritingException miscategorizes success as failure  (Issue #238)
    assertThat(copyWithout(fakeLoaderWriter.getEntryMap(), bcweFailures.keySet()),
        equalTo(copyWithout(union(originalWriterContent, copyOnly(contentUpdates, bcweSuccesses)), bcweFailures.keySet())));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.PutOutcome.class));

    this.dumpResults(fakeStore, originalStoreContent, fakeLoaderWriter, originalWriterContent, contentUpdates, expectedFailures,
        expectedSuccesses, bcweSuccesses, bcweFailures);
  }

  /**
   * Tests {@link Ehcache#putAll(Map)} for
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

    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

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
  }

  /**
   * Tests {@link Ehcache#putAll(Map)} for
   * <ul>
   *    <li>non-empty request map</li>
   *    <li>populated {@code Store} - some keys overlap request</li>
   *    <li>{@link Store#bulkCompute} throws before accessing writer</li>
   *    <li>populated {@code CacheLoaderWriter} - some keys overlap</li>
   *    <li>all {@link CacheLoaderWriter#writeAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testPutAllStoreSomeOverlapCacheAccessExceptionBeforeWriterNoOverlapAllFail() throws Exception {
    final Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    final FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);
    doThrow(new CacheAccessException("")).when(this.store)
        .bulkCompute(getAnyStringSet(), getAnyEntryIterableFunction());

    final Map<String, String> originalWriterContent = getEntryMap(KEY_SET_A, KEY_SET_B, KEY_SET_D);
    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);
    doThrow(new Exception("writeAll failed")).when(this.cacheLoaderWriter).writeAll(getAnyEntryIterable());

    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

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
        .putAllFailure(eq(contentUpdates), any(CacheAccessException.class), this.bulkExceptionCaptor.capture());

    assertThat(this.bulkExceptionCaptor.getValue().getSuccesses(), empty());
    assertThat(this.bulkExceptionCaptor.getValue().getFailures().keySet(),
        Matchers.<Set<?>>equalTo(contentUpdates.keySet()));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.PutOutcome.class));
  }

  /**
   * Tests {@link Ehcache#putAll(Map)} for
   * <ul>
   *    <li>non-empty request map</li>
   *    <li>populated {@code Store} - some keys overlap request</li>
   *    <li>{@link Store#bulkCompute} throws after accessing writer</li>
   *    <li>populated {@code CacheLoaderWriter} - some keys overlap</li>
   *    <li>all {@link CacheLoaderWriter#writeAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testPutAllStoreSomeOverlapCacheAccessExceptionAfterWriterNoOverlapAllFail() throws Exception {
    final Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    final FakeStore fakeStore = new FakeStore(originalStoreContent, Collections.singleton("keyA3"));
    this.store = spy(fakeStore);

    final Map<String, String> originalWriterContent = getEntryMap(KEY_SET_A, KEY_SET_B, KEY_SET_D);
    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);
    doThrow(new Exception("writeAll failed")).when(this.cacheLoaderWriter).writeAll(getAnyEntryIterable());

    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

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
        .putAllFailure(eq(contentUpdates), any(CacheAccessException.class), this.bulkExceptionCaptor.capture());

    assertThat(this.bulkExceptionCaptor.getValue().getSuccesses(), empty());
    assertThat(this.bulkExceptionCaptor.getValue().getFailures().keySet(),
        Matchers.<Set<?>>equalTo(contentUpdates.keySet()));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.PutOutcome.class));
  }

  /**
   * Tests {@link Ehcache#putAll(Map)} for
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

    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    final Map<String, String> contentUpdates = getAltEntryMap("new_", fanIn(KEY_SET_A, KEY_SET_C, KEY_SET_D));
    ehcache.putAll(contentUpdates);

    verify(this.store, atLeast(1)).bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), equalTo(contentUpdates.keySet()));
    assertThat(fakeStore.getEntryMap(), equalTo(union(originalStoreContent, contentUpdates)));
    verify(this.cacheLoaderWriter, atLeast(1)).writeAll(getAnyEntryIterable());
    assertThat(fakeLoaderWriter.getEntryMap(), equalTo(union(originalWriterContent, contentUpdates)));
    verifyZeroInteractions(this.spiedResilienceStrategy);

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.PutOutcome.class));
  }

  /**
   * Tests {@link Ehcache#putAll(Map)} for
   * <ul>
   *    <li>non-empty request map</li>
   *    <li>populated {@code Store} - some keys overlap request</li>
   *    <li>{@link Store#bulkCompute} throws before accessing writer</li>
   *    <li>populated {@code CacheLoaderWriter} - some keys overlap</li>
   *    <li>no {@link CacheLoaderWriter#writeAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testPutAllStoreSomeOverlapCacheAccessExceptionBeforeWriterSomeOverlapNoneFail() throws Exception {
    final Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    final FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);
    doThrow(new CacheAccessException("")).when(this.store)
        .bulkCompute(getAnyStringSet(), getAnyEntryIterableFunction());

    final Map<String, String> originalWriterContent = getEntryMap(KEY_SET_A, KEY_SET_B, KEY_SET_C);
    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);

    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

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
        .putAllFailure(eq(contentUpdates), any(CacheAccessException.class));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.PutOutcome.class));
  }

  /**
   * Tests {@link Ehcache#putAll(Map)} for
   * <ul>
   *    <li>non-empty request map</li>
   *    <li>populated {@code Store} - some keys overlap request</li>
   *    <li>{@link Store#bulkCompute} throws after accessing writer</li>
   *    <li>populated {@code CacheLoaderWriter} - some keys overlap</li>
   *    <li>no {@link CacheLoaderWriter#writeAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testPutAllStoreSomeOverlapCacheAccessExceptionAfterWriterSomeOverlapNoneFail() throws Exception {
    final Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    final FakeStore fakeStore = new FakeStore(originalStoreContent, Collections.singleton("keyA3"));
    this.store = spy(fakeStore);

    final Map<String, String> originalWriterContent = getEntryMap(KEY_SET_A, KEY_SET_B, KEY_SET_C);
    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);

    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

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
        .putAllFailure(eq(contentUpdates), any(CacheAccessException.class));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.PutOutcome.class));
  }

  /**
   * Tests {@link Ehcache#putAll(Map)} for
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

    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

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
  }

  /**
   * Tests {@link Ehcache#putAll(Map)} for
   * <ul>
   *    <li>non-empty request map</li>
   *    <li>populated {@code Store} - some keys overlap request</li>
   *    <li>{@link Store#bulkCompute} throws before accessing writer</li>
   *    <li>populated {@code CacheLoaderWriter} - some keys overlap</li>
   *    <li>some {@link CacheLoaderWriter#writeAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testPutAllStoreSomeOverlapCacheAccessExceptionBeforeWriterSomeOverlapSomeFail() throws Exception {
    final Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    final FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);
    doThrow(new CacheAccessException("")).when(this.store)
        .bulkCompute(getAnyStringSet(), getAnyEntryIterableFunction());

    final Map<String, String> originalWriterContent = getEntryMap(KEY_SET_A, KEY_SET_B, KEY_SET_C);
    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent, KEY_SET_D);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);

    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

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
        .putAllFailure(eq(contentUpdates), any(CacheAccessException.class), this.bulkExceptionCaptor.capture());

    assertThat(this.bulkExceptionCaptor.getValue().getSuccesses(), Matchers.<Set<?>>equalTo(expectedSuccesses.keySet()));
    assertThat(this.bulkExceptionCaptor.getValue().getFailures().keySet(), Matchers.<Set<?>>equalTo(expectedFailures));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.PutOutcome.class));
  }

  /**
   * Tests {@link Ehcache#putAll(Map)} for
   * <ul>
   *    <li>non-empty request map</li>
   *    <li>populated {@code Store} - some keys overlap request</li>
   *    <li>{@link Store#bulkCompute} throws after accessing writer</li>
   *    <li>populated {@code CacheLoaderWriter} - some keys overlap</li>
   *    <li>some {@link CacheLoaderWriter#writeAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testPutAllStoreSomeOverlapCacheAccessExceptionAfterWriterSomeOverlapSomeFail() throws Exception {
    final Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    final FakeStore fakeStore = new FakeStore(originalStoreContent, Collections.singleton("keyA3"));
    this.store = spy(fakeStore);

    final Map<String, String> originalWriterContent = getEntryMap(KEY_SET_A, KEY_SET_B, KEY_SET_C);
    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent, KEY_SET_D);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);

    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

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
        .putAllFailure(eq(contentUpdates), any(CacheAccessException.class), this.bulkExceptionCaptor.capture());

    assertThat(this.bulkExceptionCaptor.getValue().getSuccesses(), Matchers.<Set<?>>equalTo(expectedSuccesses.keySet()));
    assertThat(this.bulkExceptionCaptor.getValue().getFailures().keySet(), Matchers.<Set<?>>equalTo(expectedFailures));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.PutOutcome.class));
  }

  /**
   * Tests {@link Ehcache#putAll(Map)} for
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

    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

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
  }

  /**
   * Tests {@link Ehcache#putAll(Map)} for
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
  public void testPutAllStoreSomeOverlapCacheAccessExceptionBeforeWriterSomeOverlapSomeFailWithAbort() throws Exception {
    final Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    final FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);
    doThrow(new CacheAccessException("")).when(this.store)
        .bulkCompute(getAnyStringSet(), getAnyEntryIterableFunction());

    final Map<String, String> originalWriterContent = getEntryMap(KEY_SET_A, KEY_SET_B, KEY_SET_C);
    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent, KEY_SET_D);
    fakeLoaderWriter.setCompleteFailureKey("keyC4");
    this.cacheLoaderWriter = spy(fakeLoaderWriter);

    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

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
        .putAllFailure(eq(contentUpdates), any(CacheAccessException.class), this.bulkExceptionCaptor.capture());

    @SuppressWarnings("unchecked")
    final Set<String> bcweSuccesses = (Set<String>)this.bulkExceptionCaptor.getValue().getSuccesses();
    @SuppressWarnings("unchecked")
    final Map<String, Exception> bcweFailures = (Map<String, Exception>)this.bulkExceptionCaptor.getValue().getFailures();

    assertThat(union(bcweSuccesses, bcweFailures.keySet()), equalTo(contentUpdates.keySet()));
    assertThat(Collections.disjoint(bcweSuccesses, bcweFailures.keySet()), is(true));
    assertThat(bcweSuccesses, everyItem(isIn(expectedSuccesses.keySet())));
    assertThat(expectedFailures, everyItem(isIn(bcweFailures.keySet())));
    // TODO: Confirm correctness - BulkCacheWritingException miscategorizes success as failure  (Issue #238)
    assertThat(copyWithout(fakeLoaderWriter.getEntryMap(), bcweFailures.keySet()),
        equalTo(copyWithout(union(originalWriterContent, copyOnly(contentUpdates, bcweSuccesses)), bcweFailures.keySet())));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.PutOutcome.class));

    this.dumpResults(fakeStore, originalStoreContent, fakeLoaderWriter, originalWriterContent, contentUpdates, expectedFailures,
        expectedSuccesses, bcweSuccesses, bcweFailures);
  }

  /**
   * Tests {@link Ehcache#putAll(Map)} for
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
  public void testPutAllStoreSomeOverlapCacheAccessExceptionAfterWriterSomeOverlapSomeFailWithAbort() throws Exception {
    final Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    final FakeStore fakeStore = new FakeStore(originalStoreContent, Collections.singleton("keyA3"));
    this.store = spy(fakeStore);

    final Map<String, String> originalWriterContent = getEntryMap(KEY_SET_A, KEY_SET_B, KEY_SET_C);
    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent, KEY_SET_D);
    fakeLoaderWriter.setCompleteFailureKey("keyC4");
    this.cacheLoaderWriter = spy(fakeLoaderWriter);

    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

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
        .putAllFailure(eq(contentUpdates), any(CacheAccessException.class), this.bulkExceptionCaptor.capture());

    @SuppressWarnings("unchecked")
    final Set<String> bcweSuccesses = (Set<String>)this.bulkExceptionCaptor.getValue().getSuccesses();
    @SuppressWarnings("unchecked")
    final Map<String, Exception> bcweFailures = (Map<String, Exception>)this.bulkExceptionCaptor.getValue().getFailures();

    assertThat(union(bcweSuccesses, bcweFailures.keySet()), equalTo(contentUpdates.keySet()));
    assertThat(Collections.disjoint(bcweSuccesses, bcweFailures.keySet()), is(true));
    assertThat(bcweSuccesses, everyItem(isIn(expectedSuccesses.keySet())));
    assertThat(expectedFailures, everyItem(isIn(bcweFailures.keySet())));
    // TODO: Confirm correctness - BulkCacheWritingException miscategorizes success as failure  (Issue #238)
    assertThat(copyWithout(fakeLoaderWriter.getEntryMap(), bcweFailures.keySet()),
        equalTo(copyWithout(union(originalWriterContent, copyOnly(contentUpdates, bcweSuccesses)), bcweFailures.keySet())));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.PutOutcome.class));

    this.dumpResults(fakeStore, originalStoreContent, fakeLoaderWriter, originalWriterContent, contentUpdates, expectedFailures,
        expectedSuccesses, bcweSuccesses, bcweFailures);
  }

  /**
   * Tests {@link Ehcache#putAll(Map)} for
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

    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

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
  }

  /**
   * Tests {@link Ehcache#putAll(Map)} for
   * <ul>
   *    <li>non-empty request map</li>
   *    <li>populated {@code Store} - some keys overlap request</li>
   *    <li>{@link Store#bulkCompute} throws before accessing writer</li>
   *    <li>populated {@code CacheLoaderWriter} - some keys overlap</li>
   *    <li>all {@link CacheLoaderWriter#writeAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testPutAllStoreSomeOverlapCacheAccessExceptionBeforeWriterSomeOverlapAllFail() throws Exception {
    final Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    final FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);
    doThrow(new CacheAccessException("")).when(this.store)
        .bulkCompute(getAnyStringSet(), getAnyEntryIterableFunction());

    final Map<String, String> originalWriterContent = getEntryMap(KEY_SET_A, KEY_SET_B, KEY_SET_C);
    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);
    doThrow(new Exception("writeAll failed")).when(this.cacheLoaderWriter).writeAll(getAnyEntryIterable());

    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

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
        .putAllFailure(eq(contentUpdates), any(CacheAccessException.class), this.bulkExceptionCaptor.capture());

    assertThat(this.bulkExceptionCaptor.getValue().getSuccesses(), empty());
    assertThat(this.bulkExceptionCaptor.getValue().getFailures().keySet(),
        Matchers.<Set<?>>equalTo(contentUpdates.keySet()));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.PutOutcome.class));
  }

  /**
   * Tests {@link Ehcache#putAll(Map)} for
   * <ul>
   *    <li>non-empty request map</li>
   *    <li>populated {@code Store} - some keys overlap request</li>
   *    <li>{@link Store#bulkCompute} throws after accessing writer</li>
   *    <li>populated {@code CacheLoaderWriter} - some keys overlap</li>
   *    <li>all {@link CacheLoaderWriter#writeAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testPutAllStoreSomeOverlapCacheAccessExceptionAfterWriterSomeOverlapAllFail() throws Exception {
    final Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    final FakeStore fakeStore = new FakeStore(originalStoreContent, Collections.singleton("keyA3"));
    this.store = spy(fakeStore);

    final Map<String, String> originalWriterContent = getEntryMap(KEY_SET_A, KEY_SET_B, KEY_SET_C);
    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);
    doThrow(new Exception("writeAll failed")).when(this.cacheLoaderWriter).writeAll(getAnyEntryIterable());

    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

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
        .putAllFailure(eq(contentUpdates), any(CacheAccessException.class), this.bulkExceptionCaptor.capture());

    assertThat(this.bulkExceptionCaptor.getValue().getSuccesses(), empty());
    assertThat(this.bulkExceptionCaptor.getValue().getFailures().keySet(),
        Matchers.<Set<?>>equalTo(contentUpdates.keySet()));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.PutOutcome.class));
  }

  /**
   * Tests {@link Ehcache#putAll(Map)} for
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

    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    final Map<String, String> contentUpdates = getAltEntryMap("new_", fanIn(KEY_SET_B, KEY_SET_C));
    ehcache.putAll(contentUpdates);

    verify(this.store, atLeast(1)).bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), equalTo(contentUpdates.keySet()));
    assertThat(fakeStore.getEntryMap(), equalTo(union(originalStoreContent, contentUpdates)));
    verify(this.cacheLoaderWriter, atLeast(1)).writeAll(getAnyEntryIterable());
    assertThat(fakeLoaderWriter.getEntryMap(), equalTo(union(originalWriterContent, contentUpdates)));
    verifyZeroInteractions(this.spiedResilienceStrategy);

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.PutOutcome.class));
  }

  /**
   * Tests {@link Ehcache#putAll(Map)} for
   * <ul>
   *    <li>non-empty request map</li>
   *    <li>populated {@code Store} - some keys overlap request</li>
   *    <li>{@link Store#bulkCompute} throws before accessing writer</li>
   *    <li>populated {@code CacheLoaderWriter} - all keys overlap</li>
   *    <li>no {@link CacheLoaderWriter#writeAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testPutAllStoreSomeOverlapCacheAccessExceptionBeforeWriterFullOverlapNoneFail() throws Exception {
    final Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    final FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);
    doThrow(new CacheAccessException("")).when(this.store)
        .bulkCompute(getAnyStringSet(), getAnyEntryIterableFunction());

    final Map<String, String> originalWriterContent = getEntryMap(KEY_SET_A, KEY_SET_B, KEY_SET_C);
    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);

    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

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
        .putAllFailure(eq(contentUpdates), any(CacheAccessException.class));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.PutOutcome.class));
  }

  /**
   * Tests {@link Ehcache#putAll(Map)} for
   * <ul>
   *    <li>non-empty request map</li>
   *    <li>populated {@code Store} - some keys overlap request</li>
   *    <li>{@link Store#bulkCompute} throws after accessing writer</li>
   *    <li>populated {@code CacheLoaderWriter} - all keys overlap</li>
   *    <li>no {@link CacheLoaderWriter#writeAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testPutAllStoreSomeOverlapCacheAccessExceptionAfterWriterFullOverlapNoneFail() throws Exception {
    final Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    final FakeStore fakeStore = new FakeStore(originalStoreContent, Collections.singleton("keyB3"));
    this.store = spy(fakeStore);

    final Map<String, String> originalWriterContent = getEntryMap(KEY_SET_A, KEY_SET_B, KEY_SET_C);
    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);

    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

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
        .putAllFailure(eq(contentUpdates), any(CacheAccessException.class));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.PutOutcome.class));
  }

  /**
   * Tests {@link Ehcache#putAll(Map)} for
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

    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

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
  }

  /**
   * Tests {@link Ehcache#putAll(Map)} for
   * <ul>
   *    <li>non-empty request map</li>
   *    <li>populated {@code Store} - some keys overlap request</li>
   *    <li>{@link Store#bulkCompute} throws before accessing writer</li>
   *    <li>populated {@code CacheLoaderWriter} - all keys overlap</li>
   *    <li>some {@link CacheLoaderWriter#writeAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testPutAllStoreSomeOverlapCacheAccessExceptionBeforeWriterFullOverlapSomeFail() throws Exception {
    final Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    final FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);
    doThrow(new CacheAccessException("")).when(this.store)
        .bulkCompute(getAnyStringSet(), getAnyEntryIterableFunction());

    final Map<String, String> originalWriterContent = getEntryMap(KEY_SET_A, KEY_SET_B, KEY_SET_C, KEY_SET_D);
    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent, KEY_SET_D);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);

    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

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
        .putAllFailure(eq(contentUpdates), any(CacheAccessException.class), this.bulkExceptionCaptor.capture());

    assertThat(this.bulkExceptionCaptor.getValue().getSuccesses(), Matchers.<Set<?>>equalTo(expectedSuccesses.keySet()));
    assertThat(this.bulkExceptionCaptor.getValue().getFailures().keySet(), Matchers.<Set<?>>equalTo(expectedFailures));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.PutOutcome.class));
  }

  /**
   * Tests {@link Ehcache#putAll(Map)} for
   * <ul>
   *    <li>non-empty request map</li>
   *    <li>populated {@code Store} - some keys overlap request</li>
   *    <li>{@link Store#bulkCompute} throws after accessing writer</li>
   *    <li>populated {@code CacheLoaderWriter} - all keys overlap</li>
   *    <li>some {@link CacheLoaderWriter#writeAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testPutAllStoreSomeOverlapCacheAccessExceptionAfterWriterFullOverlapSomeFail() throws Exception {
    final Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    final FakeStore fakeStore = new FakeStore(originalStoreContent, Collections.singleton("keyB3"));
    this.store = spy(fakeStore);

    final Map<String, String> originalWriterContent = getEntryMap(KEY_SET_A, KEY_SET_B, KEY_SET_C, KEY_SET_D);
    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent, KEY_SET_D);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);

    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

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
        .putAllFailure(eq(contentUpdates), any(CacheAccessException.class), this.bulkExceptionCaptor.capture());

    assertThat(this.bulkExceptionCaptor.getValue().getSuccesses(), Matchers.<Set<?>>equalTo(expectedSuccesses.keySet()));
    assertThat(this.bulkExceptionCaptor.getValue().getFailures().keySet(), Matchers.<Set<?>>equalTo(expectedFailures));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.PutOutcome.class));
  }

  /**
   * Tests {@link Ehcache#putAll(Map)} for
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

    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

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
  }

  /**
   * Tests {@link Ehcache#putAll(Map)} for
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
  public void testPutAllStoreSomeOverlapCacheAccessExceptionBeforeWriterFullOverlapSomeFailWithAbort() throws Exception {
    final Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    final FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);
    doThrow(new CacheAccessException("")).when(this.store)
        .bulkCompute(getAnyStringSet(), getAnyEntryIterableFunction());

    final Map<String, String> originalWriterContent = getEntryMap(KEY_SET_A, KEY_SET_B, KEY_SET_C, KEY_SET_D);
    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent, KEY_SET_D);
    fakeLoaderWriter.setCompleteFailureKey("keyC4");
    this.cacheLoaderWriter = spy(fakeLoaderWriter);

    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

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
        .putAllFailure(eq(contentUpdates), any(CacheAccessException.class), this.bulkExceptionCaptor.capture());

    @SuppressWarnings("unchecked")
    final Set<String> bcweSuccesses = (Set<String>)this.bulkExceptionCaptor.getValue().getSuccesses();
    @SuppressWarnings("unchecked")
    final Map<String, Exception> bcweFailures = (Map<String, Exception>)this.bulkExceptionCaptor.getValue().getFailures();

    assertThat(union(bcweSuccesses, bcweFailures.keySet()), equalTo(contentUpdates.keySet()));
    assertThat(Collections.disjoint(bcweSuccesses, bcweFailures.keySet()), is(true));
    assertThat(bcweSuccesses, everyItem(isIn(expectedSuccesses.keySet())));
    assertThat(expectedFailures, everyItem(isIn(bcweFailures.keySet())));
    // TODO: Confirm correctness - BulkCacheWritingException miscategorizes success as failure  (Issue #238)
    assertThat(copyWithout(fakeLoaderWriter.getEntryMap(), bcweFailures.keySet()),
        equalTo(copyWithout(union(originalWriterContent, copyOnly(contentUpdates, bcweSuccesses)), bcweFailures.keySet())));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.PutOutcome.class));

    this.dumpResults(fakeStore, originalStoreContent, fakeLoaderWriter, originalWriterContent, contentUpdates, expectedFailures,
        expectedSuccesses, bcweSuccesses, bcweFailures);
  }

  /**
   * Tests {@link Ehcache#putAll(Map)} for
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
  public void testPutAllStoreSomeOverlapCacheAccessExceptionAfterWriterFullOverlapSomeFailWithAbort() throws Exception {
    final Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    final FakeStore fakeStore = new FakeStore(originalStoreContent, Collections.singleton("keyB3"));
    this.store = spy(fakeStore);

    final Map<String, String> originalWriterContent = getEntryMap(KEY_SET_A, KEY_SET_B, KEY_SET_C, KEY_SET_D);
    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent, KEY_SET_D);
    fakeLoaderWriter.setCompleteFailureKey("keyC4");
    this.cacheLoaderWriter = spy(fakeLoaderWriter);

    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

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
        .putAllFailure(eq(contentUpdates), any(CacheAccessException.class), this.bulkExceptionCaptor.capture());

    @SuppressWarnings("unchecked")
    final Set<String> bcweSuccesses = (Set<String>)this.bulkExceptionCaptor.getValue().getSuccesses();
    @SuppressWarnings("unchecked")
    final Map<String, Exception> bcweFailures = (Map<String, Exception>)this.bulkExceptionCaptor.getValue().getFailures();

    assertThat(union(bcweSuccesses, bcweFailures.keySet()), equalTo(contentUpdates.keySet()));
    assertThat(Collections.disjoint(bcweSuccesses, bcweFailures.keySet()), is(true));
    assertThat(bcweSuccesses, everyItem(isIn(expectedSuccesses.keySet())));
    assertThat(expectedFailures, everyItem(isIn(bcweFailures.keySet())));
    // TODO: Confirm correctness - BulkCacheWritingException miscategorizes success as failure  (Issue #238)
    assertThat(copyWithout(fakeLoaderWriter.getEntryMap(), bcweFailures.keySet()),
        equalTo(copyWithout(union(originalWriterContent, copyOnly(contentUpdates, bcweSuccesses)), bcweFailures.keySet())));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.PutOutcome.class));

    this.dumpResults(fakeStore, originalStoreContent, fakeLoaderWriter, originalWriterContent, contentUpdates, expectedFailures,
        expectedSuccesses, bcweSuccesses, bcweFailures);
  }

  /**
   * Tests {@link Ehcache#putAll(Map)} for
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

    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

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
  }

  /**
   * Tests {@link Ehcache#putAll(Map)} for
   * <ul>
   *    <li>non-empty request map</li>
   *    <li>populated {@code Store} - some keys overlap request</li>
   *    <li>{@link Store#bulkCompute} throws before accessing writer</li>
   *    <li>populated {@code CacheLoaderWriter} - all keys overlap</li>
   *    <li>all {@link CacheLoaderWriter#writeAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testPutAllStoreSomeOverlapCacheAccessExceptionBeforeWriterFullOverlapAllFail() throws Exception {
    final Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    final FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);
    doThrow(new CacheAccessException("")).when(this.store)
        .bulkCompute(getAnyStringSet(), getAnyEntryIterableFunction());

    final Map<String, String> originalWriterContent = getEntryMap(KEY_SET_A, KEY_SET_B, KEY_SET_C, KEY_SET_D);
    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);
    doThrow(new Exception("writeAll failed")).when(this.cacheLoaderWriter).writeAll(getAnyEntryIterable());

    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

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
        .putAllFailure(eq(contentUpdates), any(CacheAccessException.class), this.bulkExceptionCaptor.capture());

    assertThat(this.bulkExceptionCaptor.getValue().getSuccesses(), empty());
    assertThat(this.bulkExceptionCaptor.getValue().getFailures().keySet(),
        Matchers.<Set<?>>equalTo(contentUpdates.keySet()));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.PutOutcome.class));
  }

  /**
   * Tests {@link Ehcache#putAll(Map)} for
   * <ul>
   *    <li>non-empty request map</li>
   *    <li>populated {@code Store} - some keys overlap request</li>
   *    <li>{@link Store#bulkCompute} throws after accessing writer</li>
   *    <li>populated {@code CacheLoaderWriter} - all keys overlap</li>
   *    <li>all {@link CacheLoaderWriter#writeAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testPutAllStoreSomeOverlapCacheAccessExceptionAfterWriterFullOverlapAllFail() throws Exception {
    final Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    final FakeStore fakeStore = new FakeStore(originalStoreContent, Collections.singleton("keyB3"));
    this.store = spy(fakeStore);

    final Map<String, String> originalWriterContent = getEntryMap(KEY_SET_A, KEY_SET_B, KEY_SET_C, KEY_SET_D);
    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);
    doThrow(new Exception("writeAll failed")).when(this.cacheLoaderWriter).writeAll(getAnyEntryIterable());

    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

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
        .putAllFailure(eq(contentUpdates), any(CacheAccessException.class), this.bulkExceptionCaptor.capture());

    assertThat(this.bulkExceptionCaptor.getValue().getSuccesses(), empty());
    assertThat(this.bulkExceptionCaptor.getValue().getFailures().keySet(),
        Matchers.<Set<?>>equalTo(contentUpdates.keySet()));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.PutOutcome.class));
  }

  /**
   * Gets an initialized {@link Ehcache Ehcache} instance using the
   * {@link CacheLoaderWriter} provided.
   *
   * @param cacheLoaderWriter
   *    the {@code CacheLoaderWriter} to use; may be {@code null}
   *
   * @return a new {@code Ehcache} instance
   */
  private Ehcache<String, String> getEhcache(final CacheLoaderWriter<String, String> cacheLoaderWriter) {
    final Ehcache<String, String> ehcache = new Ehcache<String, String>(CACHE_CONFIGURATION, this.store, cacheLoaderWriter, LoggerFactory.getLogger(Ehcache.class + "-" + "EhcacheBasicPutAllTest"));
    ehcache.init();
    assertThat("cache not initialized", ehcache.getStatus(), is(Status.AVAILABLE));
    this.spiedResilienceStrategy = this.setResilienceStrategySpy(ehcache);
    return ehcache;
  }

  /**
   * Returns a Mockito {@code any} Matcher for {@code java.util.Set<String>}.
   *
   * @return a Mockito {@code any} matcher for {@code Set<String>}
   */
  @SuppressWarnings("unchecked")
  private static Set<? extends String> getAnyStringSet() {
    return any(Set.class);   // unchecked
  }

  /**
   * Returns a Mockito {@code any} Matcher for a {@link Function} over a {@code Map.Entry} {@code Iterable}.
   *
   * @return a Mockito {@code any} matcher for {@code Function}
   */
  @SuppressWarnings("unchecked")
  private static Function<Iterable<? extends Map.Entry<? extends String, ? extends String>>, Iterable<? extends Map.Entry<? extends String, ? extends String>>> getAnyEntryIterableFunction() {
    return any(Function.class);   // unchecked
  }

  /**
   * Returns a Mockito {@code any} Matcher for a {@code Map.Entry} {@code Iterable}.
   *
   * @return a Mockito {@code any} matcher for {@code Iterable}
   */
  @SuppressWarnings("unchecked")
  private static Iterable<? extends Map.Entry<? extends String, ? extends String>> getAnyEntryIterable() {
    return any(Iterable.class);
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

  /**
   * Indicates whether or not {@link #dumpResults} should emit output.
   */
  private static final boolean debugResults;
  static {
    debugResults = Boolean.parseBoolean(System.getProperty(EhcacheBasicPutAllTest.class.getName() + ".debug", "false"));
  }

  @Rule
  public TestName name = new TestName();

  /**
   * Writes a dump of test object details to {@code System.out} if, and only if, {@link #debugResults} is enabled.
   *
   * @param fakeStore the {@link org.ehcache.EhcacheBasicCrudBase.FakeStore FakeStore} instance used in the test
   * @param originalStoreContent  the original content provided to {@code fakeStore}
   * @param fakeLoaderWriter the {@link org.ehcache.EhcacheBasicCrudBase.FakeCacheLoaderWriter FakeCacheLoaderWriter} instances used in the test
   * @param originalWriterContent the original content provided to {@code fakeLoaderWriter}
   * @param contentUpdates the {@code Map} provided to the {@link org.ehcache.Ehcache#putAll(java.util.Map)} call in the test
   * @param expectedFailures the {@code Set} of failing keys expected for the test
   * @param expectedSuccesses the {@code Set} of successful keys expected for the test
   * @param bcweSuccesses the {@code Set} from {@link org.ehcache.exceptions.BulkCacheWritingException#getSuccesses()}
   * @param bcweFailures the {@code Map} from {@link org.ehcache.exceptions.BulkCacheWritingException#getFailures()}
   */
  private void dumpResults(
      final FakeStore fakeStore,
      final Map<String, String> originalStoreContent,
      final FakeCacheLoaderWriter fakeLoaderWriter,
      final Map<String, String> originalWriterContent,
      final Map<String, String> contentUpdates,
      final Set<String> expectedFailures,
      final Map<String, String> expectedSuccesses,
      final Set<String> bcweSuccesses,
      final Map<String, Exception> bcweFailures) {

    if (!debugResults) {
      return;
    }

    final StringBuilder sb = new StringBuilder(2048);
    final Formatter fmt = new Formatter(sb);
    fmt.format("Dumping results of %s:%n", this.name.getMethodName());

    fmt.format("    Content Update Entries: %s%n", sortMap(contentUpdates));
    fmt.format("    Original Store Entries : %s%n", sortMap(originalStoreContent));
    fmt.format("    Final Store Entries    : %s%n", sortMap(fakeStore.getEntryMap()));
    fmt.format("    Original Writer Entries: %s%n", sortMap(originalWriterContent));
    fmt.format("    Final Writer Entries   : %s%n", sortMap(fakeLoaderWriter.getEntryMap()));
    fmt.format("    Expected Successes: %s%n", sort(expectedSuccesses.keySet()));
    fmt.format("    Declared Successes: %s%n", sort(bcweSuccesses));
    fmt.format("    Expected Failures: %s%n", sort(expectedFailures));
    fmt.format("    Declared Failures: %s%n", sort(bcweFailures.keySet()));

    System.err.flush();
    System.out.append(sb);
    System.out.flush();
  }

  private static List<String> sort(final Collection<String> input) {
    final String[] sortArray = new String[input.size()];
    input.toArray(sortArray);
    Arrays.sort(sortArray);
    return Arrays.asList(sortArray);
  }

  private static SortedMap<String, String> sortMap(final Map<String, String> input) {
    return new TreeMap<String, String>(input);
  }
}
