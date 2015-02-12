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
import org.ehcache.function.NullaryFunction;
import org.ehcache.spi.cache.Store;
import org.ehcache.statistics.CacheOperationOutcomes;
import org.hamcrest.Matchers;
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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
 * Provides testing of basic REMOVE_ALL operations on an {@code Ehcache}.
 *
 * @author Clifford W. Johnson
 */
@SuppressWarnings("ThrowableResultOfMethodCallIgnored")
public class EhcacheBasicRemoveAllTest extends EhcacheBasicCrudBase {

  @Mock
  protected CacheLoaderWriter<String, String> cacheLoaderWriter;

  /**
   * A Mockito {@code ArgumentCaptor} for the {@code Set} argument to the
   * {@link org.ehcache.spi.cache.Store#bulkCompute(Set, Function, NullaryFunction)
   *    Store.bulkCompute(Set, Function, NullaryFunction} method.
   */
  @Captor
  private ArgumentCaptor<Set<String>> bulkComputeSetCaptor;

  /**
   * A Mockito {@code ArgumentCaptor} for the
   * {@link org.ehcache.exceptions.BulkCacheWritingException BulkCacheWritingException}
   * provided to the
   * {@link org.ehcache.resilience.ResilienceStrategy#removeAllFailure(Iterable, CacheAccessException, BulkCacheWritingException)}
   *    ResilienceStrategy.putAllFailure(Iterable, CacheAccessException, BulkCacheWritingException)} method.
   */
  @Captor
  private ArgumentCaptor<BulkCacheWritingException> bulkExceptionCaptor;


  @Test
  public void testRemoveAllNull() throws Exception {
    final Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    final FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);

    final Ehcache<String, String> ehcache = this.getEhcache(null);
    try {
      ehcache.removeAll(null);
      fail();
    } catch (NullPointerException e) {
      // Expected
    }

    assertThat(fakeStore.getEntryMap(), equalTo(originalStoreContent));
  }

  @Test
  public void testRemoveAllNullKey() throws Exception {
    final Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    final FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);

    final Set<String> keys = new LinkedHashSet<String>();
    for (final String key : KEY_SET_A) {
      keys.add(key);
      if ("keyA2".equals(key)) {
        keys.add(null);
      }
    }
    final Ehcache<String, String> ehcache = this.getEhcache(null);
    try {
      ehcache.removeAll(keys);
      fail();
    } catch (NullPointerException e) {
      // Expected
    }

    assertThat(fakeStore.getEntryMap(), equalTo(originalStoreContent));
  }

  /**
   * Tests {@link Ehcache#removeAll(Set)} for
   * <ul>
   *    <li>empty request set</li>
   *    <li>populated {@code Store} (keys not relevant)</li>
   *    <li>no {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testRemoveAllEmptyRequestNoWriter() throws Exception {
    final Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    final FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);

    final Ehcache<String, String> ehcache = this.getEhcache(null);
    ehcache.removeAll(Collections.<String>emptySet());

    verify(this.store).bulkCompute(eq(Collections.<String>emptySet()), getAnyEntryIterableFunction());
    assertThat(fakeStore.getEntryMap(), equalTo(originalStoreContent));
    verifyZeroInteractions(this.spiedResilienceStrategy);

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.RemoveOutcome.class));
  }

  /**
   * Tests {@link Ehcache#removeAll(Set)} for
   * <ul>
   *    <li>empty request set</li>
   *    <li>populated {@code Store} (keys not relevant)</li>
   *    <li>{@link Store#bulkCompute} throws before accessing writer</li>
   *    <li>no {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testRemoveAllEmptyRequestCacheAccessExceptionBeforeNoWriter() throws Exception {
    final Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    final FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);
    doThrow(new CacheAccessException("")).when(this.store)
        .bulkCompute(getAnyStringSet(), getAnyEntryIterableFunction());

    final Ehcache<String, String> ehcache = this.getEhcache(null);
    ehcache.removeAll(Collections.<String>emptySet());

    verify(this.store).bulkCompute(eq(Collections.<String>emptySet()), getAnyEntryIterableFunction());
    // ResilienceStrategy invoked; no assertions about Store content
    verify(this.spiedResilienceStrategy)
        .removeAllFailure(eq(Collections.<String>emptySet()), any(CacheAccessException.class));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.RemoveOutcome.class));
  }

  /**
   * Tests {@link Ehcache#removeAll(Set)} for
   * <ul>
   *    <li>empty request set</li>
   *    <li>populated {@code Store} (keys not relevant)</li>
   *    <li>populated {@code CacheLoaderWriter} (keys not relevant)</li>
   * </ul>
   */
  @Test
  public void testRemoveAllEmptyRequestWriterNoneFail() throws Exception {
    final Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    final FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalStoreContent);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);

    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);
    ehcache.removeAll(Collections.<String>emptySet());

    verify(this.store).bulkCompute(eq(Collections.<String>emptySet()), getAnyEntryIterableFunction());
    assertThat(fakeStore.getEntryMap(), equalTo(originalStoreContent));
    assertThat(fakeLoaderWriter.getEntryMap(), equalTo(originalStoreContent));
    verifyZeroInteractions(this.spiedResilienceStrategy);

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.RemoveOutcome.class));
  }

  /**
   * Tests {@link Ehcache#removeAll(Set)} for
   * <ul>
   *    <li>empty request set</li>
   *    <li>populated {@code Store} (keys not relevant)</li>
   *    <li>{@link Store#bulkCompute} throws before accessing writer</li>
   *    <li>populated {@code CacheLoaderWriter} (keys not relevant)</li>
   * </ul>
   */
  @Test
  public void testRemoveAllEmptyRequestCacheAccessExceptionBeforeWriterNoneFail() throws Exception {
    final Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    final FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);
    doThrow(new CacheAccessException("")).when(this.store)
        .bulkCompute(getAnyStringSet(), getAnyEntryIterableFunction());

    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalStoreContent);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);

    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);
    ehcache.removeAll(Collections.<String>emptySet());

    verify(this.store).bulkCompute(eq(Collections.<String>emptySet()), getAnyEntryIterableFunction());
    // ResilienceStrategy invoked; no assertions about Store content
    assertThat(fakeLoaderWriter.getEntryMap(), equalTo(originalStoreContent));
    verify(this.spiedResilienceStrategy)
        .removeAllFailure(eq(Collections.<String>emptySet()), any(CacheAccessException.class));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.RemoveOutcome.class));
  }

  /**
   * Tests {@link Ehcache#removeAll(Set)} for
   * <ul>
   *    <li>empty request set</li>
   *    <li>populated {@code Store} (keys not relevant)</li>
   *    <li>populated {@code CacheLoaderWriter} (keys not relevant)</li>
   *    <li>all {@link CacheLoaderWriter#deleteAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testRemoveAllEmptyRequestWriterAllFail() throws Exception {
    final Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    final FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalStoreContent);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);
    doThrow(new Exception("deleteAll failed")).when(this.cacheLoaderWriter).deleteAll(getAnyStringIterable());

    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);
    ehcache.removeAll(Collections.<String>emptySet());

    verify(this.store).bulkCompute(eq(Collections.<String>emptySet()), getAnyEntryIterableFunction());
    assertThat(fakeStore.getEntryMap(), equalTo(originalStoreContent));
    verifyZeroInteractions(this.spiedResilienceStrategy);

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.RemoveOutcome.class));
  }

  /**
   * Tests {@link Ehcache#removeAll(Set)} for
   * <ul>
   *    <li>empty request set</li>
   *    <li>populated {@code Store} (keys not relevant)</li>
   *    <li>{@link Store#bulkCompute} throws before accessing writer</li>
   *    <li>populated {@code CacheLoaderWriter} (keys not relevant)</li>
   *    <li>all {@link CacheLoaderWriter#deleteAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testRemoveAllEmptyRequestCacheAccessExceptionBeforeWriterAllFail() throws Exception {
    final Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    final FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);
    doThrow(new CacheAccessException("")).when(this.store)
        .bulkCompute(getAnyStringSet(), getAnyEntryIterableFunction());

    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalStoreContent);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);
    doThrow(new Exception("deleteAll failed")).when(this.cacheLoaderWriter).deleteAll(getAnyStringIterable());

    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);
    ehcache.removeAll(Collections.<String>emptySet());

    verify(this.store).bulkCompute(eq(Collections.<String>emptySet()), getAnyEntryIterableFunction());
    // ResilienceStrategy invoked; no assertions about Store content
    verify(this.spiedResilienceStrategy)
        .removeAllFailure(eq(Collections.<String>emptySet()), any(CacheAccessException.class));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.RemoveOutcome.class));
  }

  /**
   * Tests {@link Ehcache#removeAll(Set)} for
   * <ul>
   *    <li>non-empty request set</li>
   *    <li>populated {@code Store} - some keys overlap request</li>
   *    <li>no {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testRemoveAllStoreSomeOverlapNoWriter() throws Exception {
    final Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    final FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);

    final Ehcache<String, String> ehcache = this.getEhcache(null);

    final Set<String> contentUpdates = fanIn(KEY_SET_A, KEY_SET_C);
    ehcache.removeAll(contentUpdates);

    verify(this.store, atLeast(1)).bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), equalTo(contentUpdates));
    assertThat(fakeStore.getEntryMap(), equalTo(copyWithout(originalStoreContent, contentUpdates)));
    verifyZeroInteractions(this.spiedResilienceStrategy);

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.RemoveOutcome.class));
  }

  /**
   * Tests {@link Ehcache#removeAll(Set)} for
   * <ul>
   *    <li>non-empty request set</li>
   *    <li>populated {@code Store} - some keys overlap request</li>
   *    <li>{@link Store#bulkCompute} throws before accessing writer</li>
   *    <li>no {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testRemoveAllStoreSomeOverlapCacheAccessExceptionBeforeNoWriter() throws Exception {
    final Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    final FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);
    doThrow(new CacheAccessException("")).when(this.store)
        .bulkCompute(getAnyStringSet(), getAnyEntryIterableFunction());

    final Ehcache<String, String> ehcache = this.getEhcache(null);

    final Set<String> contentUpdates = fanIn(KEY_SET_A, KEY_SET_C);
    ehcache.removeAll(contentUpdates);

    final InOrder ordered = inOrder(this.store, this.spiedResilienceStrategy);
    ordered.verify(this.store, atLeast(1)).bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), everyItem(isIn(contentUpdates)));
    // ResilienceStrategy invoked; no assertions about Store content
    ordered.verify(this.spiedResilienceStrategy)
        .removeAllFailure(eq(contentUpdates), any(CacheAccessException.class));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.RemoveOutcome.class));
  }

  /**
   * Tests {@link Ehcache#removeAll(Set)} for
   * <ul>
   *    <li>non-empty request set</li>
   *    <li>populated {@code Store} - some keys overlap request</li>
   *    <li>{@link Store#bulkCompute} throws after accessing writer</li>
   *    <li>no {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testRemoveAllStoreSomeOverlapCacheAccessExceptionAfterNoWriter() throws Exception {
    final Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    final FakeStore fakeStore = new FakeStore(originalStoreContent, Collections.singleton("keyA3"));
    this.store = spy(fakeStore);

    final Ehcache<String, String> ehcache = this.getEhcache(null);

    final Set<String> contentUpdates = fanIn(KEY_SET_A, KEY_SET_C);
    ehcache.removeAll(contentUpdates);

    final InOrder ordered = inOrder(this.store, this.spiedResilienceStrategy);
    ordered.verify(this.store, atLeast(1)).bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), everyItem(isIn(contentUpdates)));
    // ResilienceStrategy invoked; no assertions about Store content
    ordered.verify(this.spiedResilienceStrategy)
        .removeAllFailure(eq(contentUpdates), any(CacheAccessException.class));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.RemoveOutcome.class));
  }

  /**
   * Tests {@link Ehcache#removeAll(Set)} for
   * <ul>
   *    <li>non-empty request set</li>
   *    <li>populated {@code Store} - some keys overlap request</li>
   *    <li>populated {@code CacheLoaderWriter} - no keys overlap</li>
   *    <li>no {@link CacheLoaderWriter#deleteAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testRemoveAllStoreSomeOverlapWriterNoOverlapNoneFail() throws Exception {
    final Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    final FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);

    final Map<String, String> originalWriterContent = getEntryMap(KEY_SET_B, KEY_SET_D);
    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);

    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    final Set<String> contentUpdates = fanIn(KEY_SET_A, KEY_SET_C);
    ehcache.removeAll(contentUpdates);

    verify(this.store, atLeast(1)).bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), equalTo(contentUpdates));
    assertThat(fakeStore.getEntryMap(), equalTo(copyWithout(originalStoreContent, contentUpdates)));
    assertThat(fakeLoaderWriter.getEntryMap(), equalTo(copyWithout(originalWriterContent, contentUpdates)));
    verifyZeroInteractions(this.spiedResilienceStrategy);

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.RemoveOutcome.class));
  }

  /**
   * Tests {@link Ehcache#removeAll(Set)} for
   * <ul>
   *    <li>non-empty request set</li>
   *    <li>populated {@code Store} - some keys overlap request</li>
   *    <li>{@link Store#bulkCompute} throws before accessing writer</li>
   *    <li>populated {@code CacheLoaderWriter} - no keys overlap</li>
   *    <li>no {@link CacheLoaderWriter#deleteAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testRemoveAllStoreSomeOverlapCacheAccessExceptionBeforeWriterNoOverlapNoneFail() throws Exception {
    final Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    final FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);
    doThrow(new CacheAccessException("")).when(this.store)
        .bulkCompute(getAnyStringSet(), getAnyEntryIterableFunction());

    final Map<String, String> originalWriterContent = getEntryMap(KEY_SET_B, KEY_SET_D);
    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);

    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    final Set<String> contentUpdates = fanIn(KEY_SET_A, KEY_SET_C);
    ehcache.removeAll(contentUpdates);

    final InOrder ordered = inOrder(this.store, this.cacheLoaderWriter, this.spiedResilienceStrategy);
    ordered.verify(this.store, atLeast(1)).bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), everyItem(isIn(contentUpdates)));
    // ResilienceStrategy invoked; no assertions about Store content
    ordered.verify(this.cacheLoaderWriter, atLeast(1)).deleteAll(getAnyStringIterable());
    assertThat(fakeLoaderWriter.getEntryMap(), equalTo(copyWithout(originalWriterContent, contentUpdates)));
    ordered.verify(this.spiedResilienceStrategy)
        .removeAllFailure(eq(contentUpdates), any(CacheAccessException.class));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.RemoveOutcome.class));
  }

  /**
   * Tests {@link Ehcache#removeAll(Set)} for
   * <ul>
   *    <li>non-empty request set</li>
   *    <li>populated {@code Store} - some keys overlap request</li>
   *    <li>{@link Store#bulkCompute} throws after accessing writer</li>
   *    <li>populated {@code CacheLoaderWriter} - no keys overlap</li>
   *    <li>no {@link CacheLoaderWriter#deleteAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testRemoveAllStoreSomeOverlapCacheAccessExceptionAfterWriterNoOverlapNoneFail() throws Exception {
    final Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    final FakeStore fakeStore = new FakeStore(originalStoreContent, Collections.singleton("keyA3"));
    this.store = spy(fakeStore);

    final Map<String, String> originalWriterContent = getEntryMap(KEY_SET_B, KEY_SET_D);
    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);

    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    final Set<String> contentUpdates = fanIn(KEY_SET_A, KEY_SET_C);
    ehcache.removeAll(contentUpdates);

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.RemoveOutcome.class));

    final InOrder ordered = inOrder(this.store, this.cacheLoaderWriter, this.spiedResilienceStrategy);
    ordered.verify(this.store, atLeast(1))
        .bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), everyItem(isIn(contentUpdates)));
    // ResilienceStrategy invoked; no assertions about Store content
    ordered.verify(this.cacheLoaderWriter, atLeast(1)).deleteAll(getAnyStringIterable());
    assertThat(fakeLoaderWriter.getEntryMap(), equalTo(copyWithout(originalWriterContent, contentUpdates)));
    ordered.verify(this.spiedResilienceStrategy)
        .removeAllFailure(eq(contentUpdates), any(CacheAccessException.class));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.RemoveOutcome.class));
  }

  /**
   * Tests {@link Ehcache#removeAll(Set)} for
   * <ul>
   *    <li>non-empty request set</li>
   *    <li>populated {@code Store} - some keys overlap request</li>
   *    <li>populated {@code CacheLoaderWriter} - no keys overlap</li>
   *    <li>some {@link CacheLoaderWriter#deleteAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testRemoveAllStoreSomeOverlapWriterNoOverlapSomeFail() throws Exception {
    final Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    final FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);

    final Map<String, String> originalWriterContent = getEntryMap(KEY_SET_B, KEY_SET_D);
    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent, KEY_SET_C);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);

    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    final Set<String> contentUpdates = fanIn(KEY_SET_A, KEY_SET_C);
    final Set<String> expectedFailures = KEY_SET_C;
    final Set<String> expectedSuccesses = copyWithout(contentUpdates, expectedFailures);
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
    verifyZeroInteractions(this.spiedResilienceStrategy);

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.RemoveOutcome.class));
  }

  /**
   * Tests {@link Ehcache#removeAll(Set)} for
   * <ul>
   *    <li>non-empty request set</li>
   *    <li>populated {@code Store} - some keys overlap request</li>
   *    <li>{@link Store#bulkCompute} throws before accessing writer</li>
   *    <li>populated {@code CacheLoaderWriter} - no keys overlap</li>
   *    <li>some {@link CacheLoaderWriter#deleteAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testRemoveAllStoreSomeOverlapCacheAccessExceptionBeforeWriterNoOverlapSomeFail() throws Exception {
    final Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    final FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);
    doThrow(new CacheAccessException("")).when(this.store)
        .bulkCompute(getAnyStringSet(), getAnyEntryIterableFunction());

    final Map<String, String> originalWriterContent = getEntryMap(KEY_SET_B, KEY_SET_D);
    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent, KEY_SET_C);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);

    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    final Set<String> contentUpdates = fanIn(KEY_SET_A, KEY_SET_C);
    final Set<String> expectedFailures = KEY_SET_C;
    final Set<String> expectedSuccesses = copyWithout(contentUpdates, expectedFailures);
    try {
      ehcache.removeAll(contentUpdates);
      fail();
    } catch (BulkCacheWritingException e) {
      // Expected
    }

    final InOrder ordered = inOrder(this.store, this.cacheLoaderWriter, this.spiedResilienceStrategy);
    ordered.verify(this.store, atLeast(1)).bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), everyItem(isIn(contentUpdates)));
    // ResilienceStrategy invoked; no assertions about Store content

    ordered.verify(this.cacheLoaderWriter, atLeast(1)).deleteAll(getAnyStringIterable());
    assertThat(fakeLoaderWriter.getEntryMap(), equalTo(copyWithout(originalWriterContent, expectedSuccesses)));

    ordered.verify(this.spiedResilienceStrategy)
        .removeAllFailure(eq(contentUpdates), any(CacheAccessException.class), this.bulkExceptionCaptor.capture());
    assertThat(this.bulkExceptionCaptor.getValue().getSuccesses(), Matchers.<Set<?>>equalTo(expectedSuccesses));
    assertThat(this.bulkExceptionCaptor.getValue().getFailures().keySet(), Matchers.<Set<?>>equalTo(expectedFailures));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.RemoveOutcome.class));
  }

  /**
   * Tests {@link Ehcache#removeAll(Set)} for
   * <ul>
   *    <li>non-empty request set</li>
   *    <li>populated {@code Store} - some keys overlap request</li>
   *    <li>{@link Store#bulkCompute} throws after accessing writer</li>
   *    <li>populated {@code CacheLoaderWriter} - no keys overlap</li>
   *    <li>some {@link CacheLoaderWriter#deleteAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testRemoveAllStoreSomeOverlapCacheAccessExceptionAfterWriterNoOverlapSomeFail() throws Exception {
    final Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    final FakeStore fakeStore = new FakeStore(originalStoreContent, Collections.singleton("keyA3"));
    this.store = spy(fakeStore);

    final Map<String, String> originalWriterContent = getEntryMap(KEY_SET_B, KEY_SET_D);
    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent, KEY_SET_C);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);

    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    final Set<String> contentUpdates = fanIn(KEY_SET_A, KEY_SET_C);
    final Set<String> expectedFailures = KEY_SET_C;
    final Set<String> expectedSuccesses = copyWithout(contentUpdates, expectedFailures);
    try {
      ehcache.removeAll(contentUpdates);
      fail();
    } catch (BulkCacheWritingException e) {
      // Expected
    }

    final InOrder ordered = inOrder(this.store, this.cacheLoaderWriter, this.spiedResilienceStrategy);
    ordered.verify(this.store, atLeast(1)).bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), everyItem(isIn(contentUpdates)));
    // ResilienceStrategy invoked; no assertions about Store content

    ordered.verify(this.cacheLoaderWriter, atLeast(1)).deleteAll(getAnyStringIterable());
    assertThat(fakeLoaderWriter.getEntryMap(), equalTo(copyWithout(originalWriterContent, expectedSuccesses)));

    ordered.verify(this.spiedResilienceStrategy)
        .removeAllFailure(eq(contentUpdates), any(CacheAccessException.class), this.bulkExceptionCaptor.capture());
    assertThat(this.bulkExceptionCaptor.getValue().getSuccesses(), Matchers.<Set<?>>equalTo(expectedSuccesses));
    assertThat(this.bulkExceptionCaptor.getValue().getFailures().keySet(), Matchers.<Set<?>>equalTo(expectedFailures));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.RemoveOutcome.class));
  }

  /**
   * Tests {@link Ehcache#removeAll(Set)} for
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
    final Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    final FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);

    final Map<String, String> originalWriterContent = getEntryMap(KEY_SET_B, KEY_SET_D);
    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent, KEY_SET_C);
    fakeLoaderWriter.setCompleteFailureKey("keyC4");
    this.cacheLoaderWriter = spy(fakeLoaderWriter);

    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    final Set<String> contentUpdates = fanIn(KEY_SET_A, KEY_SET_C);
    final Set<String> expectedFailures = KEY_SET_C;
    final Set<String> expectedSuccesses = copyWithout(contentUpdates, expectedFailures);
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
    verifyZeroInteractions(this.spiedResilienceStrategy);

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.RemoveOutcome.class));
  }

  /**
   * Tests {@link Ehcache#removeAll(Set)} for
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
  public void testRemoveAllStoreSomeOverlapCacheAccessExceptionBeforeWriterNoOverlapSomeFailWithAbort() throws Exception {
    final Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    final FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);
    doThrow(new CacheAccessException("")).when(this.store)
        .bulkCompute(getAnyStringSet(), getAnyEntryIterableFunction());

    final Map<String, String> originalWriterContent = getEntryMap(KEY_SET_B, KEY_SET_D);
    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent, KEY_SET_C);
    fakeLoaderWriter.setCompleteFailureKey("keyC4");
    this.cacheLoaderWriter = spy(fakeLoaderWriter);

    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    final Set<String> contentUpdates = fanIn(KEY_SET_A, KEY_SET_C);
    final Set<String> expectedFailures = KEY_SET_C;
    final Set<String> expectedSuccesses = copyWithout(contentUpdates, expectedFailures);
    try {
      ehcache.removeAll(contentUpdates);
      fail();
    } catch (BulkCacheWritingException e) {
      // Expected
    }

    final InOrder ordered = inOrder(this.store, this.cacheLoaderWriter, this.spiedResilienceStrategy);
    ordered.verify(this.store, atLeast(1)).bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), everyItem(isIn(contentUpdates)));
    // ResilienceStrategy invoked; no assertions about Store content
    ordered.verify(this.cacheLoaderWriter, atLeast(1)).deleteAll(getAnyStringIterable());
    ordered.verify(this.spiedResilienceStrategy)
        .removeAllFailure(eq(contentUpdates), any(CacheAccessException.class), this.bulkExceptionCaptor.capture());

    @SuppressWarnings("unchecked")
    final Set<String> bcweSuccesses = (Set<String>)this.bulkExceptionCaptor.getValue().getSuccesses();
    @SuppressWarnings("unchecked")
    final Map<String, Exception> bcweFailures = (Map<String, Exception>)this.bulkExceptionCaptor.getValue().getFailures();

    assertThat(union(bcweSuccesses, bcweFailures.keySet()), equalTo(contentUpdates));
    assertThat(Collections.disjoint(bcweSuccesses, bcweFailures.keySet()), is(true));
    assertThat(bcweSuccesses, everyItem(isIn(expectedSuccesses)));
    assertThat(expectedFailures, everyItem(isIn(bcweFailures.keySet())));
    assertThat(copyWithout(fakeLoaderWriter.getEntryMap(), bcweFailures.keySet()),
        equalTo(copyWithout(copyWithout(originalWriterContent, copyOnly(contentUpdates, bcweSuccesses)), bcweFailures.keySet())));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.RemoveOutcome.class));

    this.dumpResults(fakeStore, originalStoreContent, fakeLoaderWriter, originalWriterContent, contentUpdates, expectedFailures,
        expectedSuccesses, bcweSuccesses, bcweFailures);
  }

  /**
   * Tests {@link Ehcache#removeAll(Set)} for
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
  public void testRemoveAllStoreSomeOverlapCacheAccessExceptionAfterWriterNoOverlapSomeFailWithAbort() throws Exception {
    final Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    final FakeStore fakeStore = new FakeStore(originalStoreContent, Collections.singleton("keyA3"));
    this.store = spy(fakeStore);

    final Map<String, String> originalWriterContent = getEntryMap(KEY_SET_B, KEY_SET_D);
    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent, KEY_SET_C);
    fakeLoaderWriter.setCompleteFailureKey("keyC4");
    this.cacheLoaderWriter = spy(fakeLoaderWriter);

    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    final Set<String> contentUpdates = fanIn(KEY_SET_A, KEY_SET_C);
    final Set<String> expectedFailures = KEY_SET_C;
    final Set<String> expectedSuccesses = copyWithout(contentUpdates, expectedFailures);
    try {
      ehcache.removeAll(contentUpdates);
      fail();
    } catch (BulkCacheWritingException e) {
      // Expected
    }

    final InOrder ordered = inOrder(this.store, this.cacheLoaderWriter, this.spiedResilienceStrategy);
    ordered.verify(this.store, atLeast(1)).bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), everyItem(isIn(contentUpdates)));
    // ResilienceStrategy invoked; no assertions about Store content
    ordered.verify(this.cacheLoaderWriter, atLeast(1)).deleteAll(getAnyStringIterable());
    ordered.verify(this.spiedResilienceStrategy)
        .removeAllFailure(eq(contentUpdates), any(CacheAccessException.class), this.bulkExceptionCaptor.capture());

    @SuppressWarnings("unchecked")
    final Set<String> bcweSuccesses = (Set<String>)this.bulkExceptionCaptor.getValue().getSuccesses();
    @SuppressWarnings("unchecked")
    final Map<String, Exception> bcweFailures = (Map<String, Exception>)this.bulkExceptionCaptor.getValue().getFailures();

    assertThat(union(bcweSuccesses, bcweFailures.keySet()), equalTo(contentUpdates));
    assertThat(Collections.disjoint(bcweSuccesses, bcweFailures.keySet()), is(true));
    assertThat(bcweSuccesses, everyItem(isIn(expectedSuccesses)));
    assertThat(expectedFailures, everyItem(isIn(bcweFailures.keySet())));
    assertThat(copyWithout(fakeLoaderWriter.getEntryMap(), bcweFailures.keySet()),
        equalTo(copyWithout(copyWithout(originalWriterContent, copyOnly(contentUpdates, bcweSuccesses)), bcweFailures.keySet())));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.RemoveOutcome.class));

    this.dumpResults(fakeStore, originalStoreContent, fakeLoaderWriter, originalWriterContent, contentUpdates, expectedFailures,
        expectedSuccesses, bcweSuccesses, bcweFailures);
  }

  /**
   * Tests {@link Ehcache#removeAll(Set)} for
   * <ul>
   *    <li>non-empty request set</li>
   *    <li>populated {@code Store} - some keys overlap request</li>
   *    <li>populated {@code CacheLoaderWriter} - no keys overlap</li>
   *    <li>all {@link CacheLoaderWriter#deleteAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testRemoveAllStoreSomeOverlapWriterNoOverlapAllFail() throws Exception {
    final Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    final FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);

    final Map<String, String> originalWriterContent = getEntryMap(KEY_SET_B, KEY_SET_D);
    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);
    doThrow(new Exception("deleteAll failed")).when(this.cacheLoaderWriter).deleteAll(getAnyStringIterable());

    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    final Set<String> contentUpdates = fanIn(KEY_SET_A, KEY_SET_C);
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
    verifyZeroInteractions(this.spiedResilienceStrategy);

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.RemoveOutcome.class));
  }

  /**
   * Tests {@link Ehcache#removeAll(Set)} for
   * <ul>
   *    <li>non-empty request set</li>
   *    <li>populated {@code Store} - some keys overlap request</li>
   *    <li>{@link Store#bulkCompute} throws before accessing writer</li>
   *    <li>populated {@code CacheLoaderWriter} - no keys overlap</li>
   *    <li>all {@link CacheLoaderWriter#deleteAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testRemoveAllStoreSomeOverlapCacheAccessExceptionBeforeWriterNoOverlapAllFail() throws Exception {
    final Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    final FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);
    doThrow(new CacheAccessException("")).when(this.store)
        .bulkCompute(getAnyStringSet(), getAnyEntryIterableFunction());

    final Map<String, String> originalWriterContent = getEntryMap(KEY_SET_B, KEY_SET_D);
    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);
    doThrow(new Exception("deleteAll failed")).when(this.cacheLoaderWriter).deleteAll(getAnyStringIterable());

    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    final Set<String> contentUpdates = fanIn(KEY_SET_A, KEY_SET_C);
    try {
      ehcache.removeAll(contentUpdates);
      fail();
    } catch (BulkCacheWritingException e) {
      // Expected
    }

    final InOrder ordered = inOrder(this.store, this.cacheLoaderWriter, this.spiedResilienceStrategy);
    ordered.verify(this.store, atLeast(1)).bulkCompute(this.bulkComputeSetCaptor.capture(),
        getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), everyItem(isIn(contentUpdates)));
    // ResilienceStrategy invoked; no assertions about Store content
    ordered.verify(this.cacheLoaderWriter, atLeast(1)).deleteAll(getAnyStringIterable());
    ordered.verify(this.spiedResilienceStrategy)
        .removeAllFailure(eq(contentUpdates), any(CacheAccessException.class), this.bulkExceptionCaptor.capture());

    assertThat(this.bulkExceptionCaptor.getValue().getSuccesses(), empty());
    assertThat(this.bulkExceptionCaptor.getValue().getFailures().keySet(), Matchers.<Set<?>>equalTo(contentUpdates));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.RemoveOutcome.class));
  }

  /**
   * Tests {@link Ehcache#removeAll(Set)} for
   * <ul>
   *    <li>non-empty request set</li>
   *    <li>populated {@code Store} - some keys overlap request</li>
   *    <li>{@link Store#bulkCompute} throws after accessing writer</li>
   *    <li>populated {@code CacheLoaderWriter} - no keys overlap</li>
   *    <li>all {@link CacheLoaderWriter#deleteAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testRemoveAllStoreSomeOverlapCacheAccessExceptionAfterWriterNoOverlapAllFail() throws Exception {
    final Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    final FakeStore fakeStore = new FakeStore(originalStoreContent, Collections.singleton("keyA3"));
    this.store = spy(fakeStore);

    final Map<String, String> originalWriterContent = getEntryMap(KEY_SET_B, KEY_SET_D);
    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);
    doThrow(new Exception("deleteAll failed")).when(this.cacheLoaderWriter).deleteAll(getAnyStringIterable());

    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    final Set<String> contentUpdates = fanIn(KEY_SET_A, KEY_SET_C);
    try {
      ehcache.removeAll(contentUpdates);
      fail();
    } catch (BulkCacheWritingException e) {
      // Expected
    }

    final InOrder ordered = inOrder(this.store, this.cacheLoaderWriter, this.spiedResilienceStrategy);
    ordered.verify(this.store, atLeast(1)).bulkCompute(this.bulkComputeSetCaptor.capture(),
        getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), everyItem(isIn(contentUpdates)));
    // ResilienceStrategy invoked; no assertions about Store content
    ordered.verify(this.cacheLoaderWriter, atLeast(1)).deleteAll(getAnyStringIterable());
    ordered.verify(this.spiedResilienceStrategy)
        .removeAllFailure(eq(contentUpdates), any(CacheAccessException.class), this.bulkExceptionCaptor.capture());

    assertThat(this.bulkExceptionCaptor.getValue().getSuccesses(), empty());
    assertThat(this.bulkExceptionCaptor.getValue().getFailures().keySet(), Matchers.<Set<?>>equalTo(contentUpdates));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.RemoveOutcome.class));
  }

  /**
   * Tests {@link Ehcache#removeAll(Set)} for
   * <ul>
   *    <li>non-empty request set</li>
   *    <li>populated {@code Store} - some keys overlap request</li>
   *    <li>populated {@code CacheLoaderWriter} - some keys overlap</li>
   *    <li>no {@link CacheLoaderWriter#deleteAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testRemoveAllStoreSomeOverlapWriterSomeOverlapNoneFail() throws Exception {
    final Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    final FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);

    final Map<String, String> originalWriterContent = getEntryMap(KEY_SET_B, KEY_SET_C);
    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);

    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    final Set<String> contentUpdates = fanIn(KEY_SET_A, KEY_SET_C, KEY_SET_D);
    ehcache.removeAll(contentUpdates);

    verify(this.store, atLeast(1)).bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), equalTo(contentUpdates));
    assertThat(fakeStore.getEntryMap(), equalTo(copyWithout(originalStoreContent, contentUpdates)));
    verify(this.cacheLoaderWriter, atLeast(1)).deleteAll(getAnyStringIterable());
    assertThat(fakeLoaderWriter.getEntryMap(), equalTo(copyWithout(originalWriterContent, contentUpdates)));
    verifyZeroInteractions(this.spiedResilienceStrategy);

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.RemoveOutcome.class));
  }

  /**
   * Tests {@link Ehcache#removeAll(Set)} for
   * <ul>
   *    <li>non-empty request set</li>
   *    <li>populated {@code Store} - some keys overlap request</li>
   *    <li>{@link Store#bulkCompute} throws before accessing writer</li>
   *    <li>populated {@code CacheLoaderWriter} - some keys overlap</li>
   *    <li>no {@link CacheLoaderWriter#deleteAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testRemoveAllStoreSomeOverlapCacheAccessExceptionBeforeWriterSomeOverlapNoneFail() throws Exception {
    final Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    final FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);
    doThrow(new CacheAccessException("")).when(this.store)
        .bulkCompute(getAnyStringSet(), getAnyEntryIterableFunction());

    final Map<String, String> originalWriterContent = getEntryMap(KEY_SET_B, KEY_SET_C);
    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);

    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    final Set<String> contentUpdates = fanIn(KEY_SET_A, KEY_SET_C, KEY_SET_D);
    ehcache.removeAll(contentUpdates);

    final InOrder ordered = inOrder(this.store, this.cacheLoaderWriter, this.spiedResilienceStrategy);
    ordered.verify(this.store, atLeast(1))
        .bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), everyItem(isIn(contentUpdates)));
    // ResilienceStrategy invoked; no assertions about Store content
    ordered.verify(this.cacheLoaderWriter, atLeast(1)).deleteAll(getAnyStringIterable());
    assertThat(fakeLoaderWriter.getEntryMap(), equalTo(copyWithout(originalWriterContent, contentUpdates)));
    ordered.verify(this.spiedResilienceStrategy)
        .removeAllFailure(eq(contentUpdates), any(CacheAccessException.class));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.RemoveOutcome.class));
  }

  /**
   * Tests {@link Ehcache#removeAll(Set)} for
   * <ul>
   *    <li>non-empty request set</li>
   *    <li>populated {@code Store} - some keys overlap request</li>
   *    <li>{@link Store#bulkCompute} throws after accessing writer</li>
   *    <li>populated {@code CacheLoaderWriter} - some keys overlap</li>
   *    <li>no {@link CacheLoaderWriter#deleteAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testRemoveAllStoreSomeOverlapCacheAccessExceptionAfterWriterSomeOverlapNoneFail() throws Exception {
    final Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    final FakeStore fakeStore = new FakeStore(originalStoreContent, Collections.singleton("keyA3"));
    this.store = spy(fakeStore);

    final Map<String, String> originalWriterContent = getEntryMap(KEY_SET_B, KEY_SET_C);
    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);

    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    final Set<String> contentUpdates = fanIn(KEY_SET_A, KEY_SET_C, KEY_SET_D);
    ehcache.removeAll(contentUpdates);

    final InOrder ordered = inOrder(this.store, this.cacheLoaderWriter, this.spiedResilienceStrategy);
    ordered.verify(this.store, atLeast(1))
        .bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), everyItem(isIn(contentUpdates)));
    // ResilienceStrategy invoked; no assertions about Store content
    ordered.verify(this.cacheLoaderWriter, atLeast(1)).deleteAll(getAnyStringIterable());
    assertThat(fakeLoaderWriter.getEntryMap(), equalTo(copyWithout(originalWriterContent, contentUpdates)));
    ordered.verify(this.spiedResilienceStrategy)
        .removeAllFailure(eq(contentUpdates), any(CacheAccessException.class));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.RemoveOutcome.class));
  }

  /**
   * Tests {@link Ehcache#removeAll(Set)} for
   * <ul>
   *    <li>non-empty request set</li>
   *    <li>populated {@code Store} - some keys overlap request</li>
   *    <li>populated {@code CacheLoaderWriter} - some keys overlap</li>
   *    <li>some {@link CacheLoaderWriter#deleteAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testRemoveAllStoreSomeOverlapWriterSomeOverlapSomeFail() throws Exception {
    final Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    final FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);

    final Map<String, String> originalWriterContent = getEntryMap(KEY_SET_B, KEY_SET_C);
    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent, KEY_SET_D);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);

    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    final Set<String> contentUpdates = fanIn(KEY_SET_A, KEY_SET_C, KEY_SET_D);
    final Set<String> expectedFailures = KEY_SET_D;
    final Set<String> expectedSuccesses = copyWithout(contentUpdates, expectedFailures);
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
    verifyZeroInteractions(this.spiedResilienceStrategy);

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.RemoveOutcome.class));
  }

  /**
   * Tests {@link Ehcache#removeAll(Set)} for
   * <ul>
   *    <li>non-empty request set</li>
   *    <li>populated {@code Store} - some keys overlap request</li>
   *    <li>{@link Store#bulkCompute} throws before accessing writer</li>
   *    <li>populated {@code CacheLoaderWriter} - some keys overlap</li>
   *    <li>some {@link CacheLoaderWriter#deleteAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testRemoveAllStoreSomeOverlapCacheAccessExceptionBeforeWriterSomeOverlapSomeFail() throws Exception {
    final Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    final FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);
    doThrow(new CacheAccessException("")).when(this.store)
        .bulkCompute(getAnyStringSet(), getAnyEntryIterableFunction());

    final Map<String, String> originalWriterContent = getEntryMap(KEY_SET_B, KEY_SET_C);
    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent, KEY_SET_D);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);

    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    final Set<String> contentUpdates = fanIn(KEY_SET_A, KEY_SET_C, KEY_SET_D);
    final Set<String> expectedFailures = KEY_SET_D;
    final Set<String> expectedSuccesses = copyWithout(contentUpdates, expectedFailures);
    try {
      ehcache.removeAll(contentUpdates);
      fail();
    } catch (BulkCacheWritingException e) {
      // Expected
    }

    final InOrder ordered = inOrder(this.store, this.cacheLoaderWriter, this.spiedResilienceStrategy);
    ordered.verify(this.store, atLeast(1))
        .bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), everyItem(isIn(contentUpdates)));
    // ResilienceStrategy invoked; no assertions about Store content
    ordered.verify(this.cacheLoaderWriter, atLeast(1)).deleteAll(getAnyStringIterable());
    assertThat(fakeLoaderWriter.getEntryMap(), equalTo(copyWithout(originalWriterContent, expectedSuccesses)));
    ordered.verify(this.spiedResilienceStrategy)
        .removeAllFailure(eq(contentUpdates), any(CacheAccessException.class), this.bulkExceptionCaptor.capture());

    assertThat(this.bulkExceptionCaptor.getValue().getSuccesses(), Matchers.<Set<?>>equalTo(expectedSuccesses));
    assertThat(this.bulkExceptionCaptor.getValue().getFailures().keySet(), Matchers.<Set<?>>equalTo(expectedFailures));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.RemoveOutcome.class));
  }

  /**
   * Tests {@link Ehcache#removeAll(Set)} for
   * <ul>
   *    <li>non-empty request set</li>
   *    <li>populated {@code Store} - some keys overlap request</li>
   *    <li>{@link Store#bulkCompute} throws after accessing writer</li>
   *    <li>populated {@code CacheLoaderWriter} - some keys overlap</li>
   *    <li>some {@link CacheLoaderWriter#deleteAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testRemoveAllStoreSomeOverlapCacheAccessExceptionAfterWriterSomeOverlapSomeFail() throws Exception {
    final Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    final FakeStore fakeStore = new FakeStore(originalStoreContent, Collections.singleton("keyA3"));
    this.store = spy(fakeStore);

    final Map<String, String> originalWriterContent = getEntryMap(KEY_SET_B, KEY_SET_C);
    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent, KEY_SET_D);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);

    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    final Set<String> contentUpdates = fanIn(KEY_SET_A, KEY_SET_C, KEY_SET_D);
    final Set<String> expectedFailures = KEY_SET_D;
    final Set<String> expectedSuccesses = copyWithout(contentUpdates, expectedFailures);
    try {
      ehcache.removeAll(contentUpdates);
      fail();
    } catch (BulkCacheWritingException e) {
      // Expected
    }

    final InOrder ordered = inOrder(this.store, this.cacheLoaderWriter, this.spiedResilienceStrategy);
    ordered.verify(this.store, atLeast(1))
        .bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), everyItem(isIn(contentUpdates)));
    // ResilienceStrategy invoked; no assertions about Store content
    ordered.verify(this.cacheLoaderWriter, atLeast(1)).deleteAll(getAnyStringIterable());
    assertThat(fakeLoaderWriter.getEntryMap(), equalTo(copyWithout(originalWriterContent, expectedSuccesses)));
    ordered.verify(this.spiedResilienceStrategy)
        .removeAllFailure(eq(contentUpdates), any(CacheAccessException.class), this.bulkExceptionCaptor.capture());

    assertThat(this.bulkExceptionCaptor.getValue().getSuccesses(), Matchers.<Set<?>>equalTo(expectedSuccesses));
    assertThat(this.bulkExceptionCaptor.getValue().getFailures().keySet(), Matchers.<Set<?>>equalTo(expectedFailures));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.RemoveOutcome.class));
  }

  /**
   * Tests {@link Ehcache#removeAll(Set)} for
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
    final Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    final FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);

    final Map<String, String> originalWriterContent = getEntryMap(KEY_SET_B, KEY_SET_C);
    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent, KEY_SET_D);
    fakeLoaderWriter.setCompleteFailureKey("keyC4");
    this.cacheLoaderWriter = spy(fakeLoaderWriter);

    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    final Set<String> contentUpdates = fanIn(KEY_SET_A, KEY_SET_C, KEY_SET_D);
    final Set<String> expectedFailures = union(KEY_SET_D, Collections.singleton("keyC4"));
    final Set<String> expectedSuccesses = copyWithout(contentUpdates, expectedFailures);
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
    verifyZeroInteractions(this.spiedResilienceStrategy);

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.RemoveOutcome.class));
  }

  /**
   * Tests {@link Ehcache#removeAll(Set)} for
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
  public void testRemoveAllStoreSomeOverlapCacheAccessExceptionBeforeWriterSomeOverlapSomeFailWithAbort() throws Exception {
    final Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    final FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);
    doThrow(new CacheAccessException("")).when(this.store)
        .bulkCompute(getAnyStringSet(), getAnyEntryIterableFunction());

    final Map<String, String> originalWriterContent = getEntryMap(KEY_SET_B, KEY_SET_C);
    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent, KEY_SET_D);
    fakeLoaderWriter.setCompleteFailureKey("keyC4");
    this.cacheLoaderWriter = spy(fakeLoaderWriter);

    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    final Set<String> contentUpdates = fanIn(KEY_SET_A, KEY_SET_C, KEY_SET_D);
    final Set<String> expectedFailures = union(KEY_SET_D, Collections.singleton("keyC4"));
    final Set<String> expectedSuccesses = copyWithout(contentUpdates, expectedFailures);
    try {
      ehcache.removeAll(contentUpdates);
      fail();
    } catch (BulkCacheWritingException e) {
      // Expected
    }

    final InOrder ordered = inOrder(this.store, this.cacheLoaderWriter, this.spiedResilienceStrategy);
    ordered.verify(this.store, atLeast(1)).bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), everyItem(isIn(contentUpdates)));
    // ResilienceStrategy invoked; no assertions about Store content
    ordered.verify(this.cacheLoaderWriter, atLeast(1)).deleteAll(getAnyStringIterable());
    ordered.verify(this.spiedResilienceStrategy)
        .removeAllFailure(eq(contentUpdates), any(CacheAccessException.class), this.bulkExceptionCaptor.capture());

    @SuppressWarnings("unchecked")
    final Set<String> bcweSuccesses = (Set<String>)this.bulkExceptionCaptor.getValue().getSuccesses();
    @SuppressWarnings("unchecked")
    final Map<String, Exception> bcweFailures = (Map<String, Exception>)this.bulkExceptionCaptor.getValue().getFailures();

    assertThat(union(bcweSuccesses, bcweFailures.keySet()), equalTo(contentUpdates));
    assertThat(Collections.disjoint(bcweSuccesses, bcweFailures.keySet()), is(true));
    assertThat(bcweSuccesses, everyItem(isIn(expectedSuccesses)));
    assertThat(expectedFailures, everyItem(isIn(bcweFailures.keySet())));
    assertThat(copyWithout(fakeLoaderWriter.getEntryMap(), bcweFailures.keySet()),
        equalTo(copyWithout(copyWithout(originalWriterContent, copyOnly(contentUpdates, bcweSuccesses)),
            bcweFailures.keySet())));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.RemoveOutcome.class));

    this.dumpResults(fakeStore, originalStoreContent, fakeLoaderWriter, originalWriterContent, contentUpdates, expectedFailures,
        expectedSuccesses, bcweSuccesses, bcweFailures);
  }

  /**
   * Tests {@link Ehcache#removeAll(Set)} for
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
  public void testRemoveAllStoreSomeOverlapCacheAccessExceptionAfterWriterSomeOverlapSomeFailWithAbort() throws Exception {
    final Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    final FakeStore fakeStore = new FakeStore(originalStoreContent, Collections.singleton("keyA3"));
    this.store = spy(fakeStore);

    final Map<String, String> originalWriterContent = getEntryMap(KEY_SET_B, KEY_SET_C);
    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent, KEY_SET_D);
    fakeLoaderWriter.setCompleteFailureKey("keyC4");
    this.cacheLoaderWriter = spy(fakeLoaderWriter);

    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    final Set<String> contentUpdates = fanIn(KEY_SET_A, KEY_SET_C, KEY_SET_D);
    final Set<String> expectedFailures = union(KEY_SET_D, Collections.singleton("keyC4"));
    final Set<String> expectedSuccesses = copyWithout(contentUpdates, expectedFailures);
    try {
      ehcache.removeAll(contentUpdates);
      fail();
    } catch (BulkCacheWritingException e) {
      // Expected
    }

    final InOrder ordered = inOrder(this.store, this.cacheLoaderWriter, this.spiedResilienceStrategy);
    ordered.verify(this.store, atLeast(1)).bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), everyItem(isIn(contentUpdates)));
    // ResilienceStrategy invoked; no assertions about Store content
    ordered.verify(this.cacheLoaderWriter, atLeast(1)).deleteAll(getAnyStringIterable());
    ordered.verify(this.spiedResilienceStrategy)
        .removeAllFailure(eq(contentUpdates), any(CacheAccessException.class), this.bulkExceptionCaptor.capture());

    @SuppressWarnings("unchecked")
    final Set<String> bcweSuccesses = (Set<String>)this.bulkExceptionCaptor.getValue().getSuccesses();
    @SuppressWarnings("unchecked")
    final Map<String, Exception> bcweFailures = (Map<String, Exception>)this.bulkExceptionCaptor.getValue().getFailures();

    assertThat(union(bcweSuccesses, bcweFailures.keySet()), equalTo(contentUpdates));
    assertThat(Collections.disjoint(bcweSuccesses, bcweFailures.keySet()), is(true));
    assertThat(bcweSuccesses, everyItem(isIn(expectedSuccesses)));
    assertThat(expectedFailures, everyItem(isIn(bcweFailures.keySet())));
    assertThat(copyWithout(fakeLoaderWriter.getEntryMap(), bcweFailures.keySet()),
        equalTo(copyWithout(copyWithout(originalWriterContent, copyOnly(contentUpdates, bcweSuccesses)),
            bcweFailures.keySet())));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.RemoveOutcome.class));

    this.dumpResults(fakeStore, originalStoreContent, fakeLoaderWriter, originalWriterContent, contentUpdates, expectedFailures,
        expectedSuccesses, bcweSuccesses, bcweFailures);
  }

  /**
   * Tests {@link Ehcache#removeAll(Set)} for
   * <ul>
   *    <li>non-empty request set</li>
   *    <li>populated {@code Store} - some keys overlap request</li>
   *    <li>populated {@code CacheLoaderWriter} - some keys overlap</li>
   *    <li>all {@link CacheLoaderWriter#deleteAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testRemoveAllStoreSomeOverlapWriterSomeOverlapAllFail() throws Exception {
    final Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    final FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);

    final Map<String, String> originalWriterContent = getEntryMap(KEY_SET_B, KEY_SET_C);
    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);
    doThrow(new Exception("deleteAll failed")).when(this.cacheLoaderWriter).deleteAll(getAnyStringIterable());

    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    final Set<String> contentUpdates = fanIn(KEY_SET_A, KEY_SET_C, KEY_SET_D);
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
    verifyZeroInteractions(this.spiedResilienceStrategy);

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.RemoveOutcome.class));
  }

  /**
   * Tests {@link Ehcache#removeAll(Set)} for
   * <ul>
   *    <li>non-empty request set</li>
   *    <li>populated {@code Store} - some keys overlap request</li>
   *    <li>{@link Store#bulkCompute} throws before accessing writer</li>
   *    <li>populated {@code CacheLoaderWriter} - some keys overlap</li>
   *    <li>all {@link CacheLoaderWriter#deleteAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testRemoveAllStoreSomeOverlapCacheAccessExceptionBeforeWriterSomeOverlapAllFail() throws Exception {
    final Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    final FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);
    doThrow(new CacheAccessException("")).when(this.store)
        .bulkCompute(getAnyStringSet(), getAnyEntryIterableFunction());

    final Map<String, String> originalWriterContent = getEntryMap(KEY_SET_B, KEY_SET_C);
    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);
    doThrow(new Exception("deleteAll failed")).when(this.cacheLoaderWriter).deleteAll(getAnyStringIterable());

    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    final Set<String> contentUpdates = fanIn(KEY_SET_A, KEY_SET_C, KEY_SET_D);
    try {
      ehcache.removeAll(contentUpdates);
      fail();
    } catch (BulkCacheWritingException e) {
      // Expected
    }

    final InOrder ordered = inOrder(this.store, this.cacheLoaderWriter, this.spiedResilienceStrategy);
    ordered.verify(this.store, atLeast(1))
        .bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), everyItem(isIn(contentUpdates)));
    // ResilienceStrategy invoked; no assertions about Store content
    ordered.verify(this.cacheLoaderWriter, atLeast(1)).deleteAll(getAnyStringIterable());
    ordered.verify(this.spiedResilienceStrategy)
        .removeAllFailure(eq(contentUpdates), any(CacheAccessException.class), this.bulkExceptionCaptor.capture());

    assertThat(this.bulkExceptionCaptor.getValue().getSuccesses(), empty());
    assertThat(this.bulkExceptionCaptor.getValue().getFailures().keySet(), Matchers.<Set<?>>equalTo(contentUpdates));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.RemoveOutcome.class));
  }

  /**
   * Tests {@link Ehcache#removeAll(Set)} for
   * <ul>
   *    <li>non-empty request set</li>
   *    <li>populated {@code Store} - some keys overlap request</li>
   *    <li>{@link Store#bulkCompute} throws after accessing writer</li>
   *    <li>populated {@code CacheLoaderWriter} - some keys overlap</li>
   *    <li>all {@link CacheLoaderWriter#deleteAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testRemoveAllStoreSomeOverlapCacheAccessExceptionAfterWriterSomeOverlapAllFail() throws Exception {
    final Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    final FakeStore fakeStore = new FakeStore(originalStoreContent, Collections.singleton("keyA3"));
    this.store = spy(fakeStore);

    final Map<String, String> originalWriterContent = getEntryMap(KEY_SET_B, KEY_SET_C);
    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);
    doThrow(new Exception("deleteAll failed")).when(this.cacheLoaderWriter).deleteAll(getAnyStringIterable());

    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    final Set<String> contentUpdates = fanIn(KEY_SET_A, KEY_SET_C, KEY_SET_D);
    try {
      ehcache.removeAll(contentUpdates);
      fail();
    } catch (BulkCacheWritingException e) {
      // Expected
    }

    final InOrder ordered = inOrder(this.store, this.cacheLoaderWriter, this.spiedResilienceStrategy);
    ordered.verify(this.store, atLeast(1))
        .bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), everyItem(isIn(contentUpdates)));
    // ResilienceStrategy invoked; no assertions about Store content
    ordered.verify(this.cacheLoaderWriter, atLeast(1)).deleteAll(getAnyStringIterable());
    ordered.verify(this.spiedResilienceStrategy)
        .removeAllFailure(eq(contentUpdates), any(CacheAccessException.class), this.bulkExceptionCaptor.capture());

    assertThat(this.bulkExceptionCaptor.getValue().getSuccesses(), empty());
    assertThat(this.bulkExceptionCaptor.getValue().getFailures().keySet(), Matchers.<Set<?>>equalTo(contentUpdates));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.RemoveOutcome.class));
  }

  /**
   * Tests {@link Ehcache#removeAll(Set)} for
   * <ul>
   *    <li>non-empty request set</li>
   *    <li>populated {@code Store} - some keys overlap request</li>
   *    <li>populated {@code CacheLoaderWriter} - all keys overlap</li>
   *    <li>no {@link CacheLoaderWriter#deleteAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testRemoveAllStoreSomeOverlapWriterFullOverlapNoneFail() throws Exception {
    final Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    final FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);

    final Map<String, String> originalWriterContent = getEntryMap(KEY_SET_B, KEY_SET_C);
    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);

    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    final Set<String> contentUpdates = fanIn(KEY_SET_B, KEY_SET_C);
    ehcache.removeAll(contentUpdates);

    verify(this.store, atLeast(1)).bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), equalTo(contentUpdates));
    assertThat(fakeStore.getEntryMap(), equalTo(copyWithout(originalStoreContent, contentUpdates)));
    verify(this.cacheLoaderWriter, atLeast(1)).deleteAll(getAnyStringIterable());
    assertThat(fakeLoaderWriter.getEntryMap(), equalTo(copyWithout(originalWriterContent, contentUpdates)));
    verifyZeroInteractions(this.spiedResilienceStrategy);

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.RemoveOutcome.class));
  }

  /**
   * Tests {@link Ehcache#removeAll(Set)} for
   * <ul>
   *    <li>non-empty request set</li>
   *    <li>populated {@code Store} - some keys overlap request</li>
   *    <li>{@link Store#bulkCompute} throws before accessing writer</li>
   *    <li>populated {@code CacheLoaderWriter} - all keys overlap</li>
   *    <li>no {@link CacheLoaderWriter#deleteAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testRemoveAllStoreSomeOverlapCacheAccessExceptionBeforeWriterFullOverlapNoneFail() throws Exception {
    final Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    final FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);
    doThrow(new CacheAccessException("")).when(this.store)
        .bulkCompute(getAnyStringSet(), getAnyEntryIterableFunction());

    final Map<String, String> originalWriterContent = getEntryMap(KEY_SET_B, KEY_SET_C);
    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);

    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    final Set<String> contentUpdates = fanIn(KEY_SET_B, KEY_SET_C);
    ehcache.removeAll(contentUpdates);

    final InOrder ordered = inOrder(this.store, this.cacheLoaderWriter, this.spiedResilienceStrategy);
    ordered.verify(this.store, atLeast(1))
        .bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), everyItem(isIn(contentUpdates)));
    // ResilienceStrategy invoked; no assertions about Store content
    ordered.verify(this.cacheLoaderWriter, atLeast(1)).deleteAll(getAnyStringIterable());
    assertThat(fakeLoaderWriter.getEntryMap(), equalTo(copyWithout(originalWriterContent, contentUpdates)));
    ordered.verify(this.spiedResilienceStrategy)
        .removeAllFailure(eq(contentUpdates), any(CacheAccessException.class));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.RemoveOutcome.class));
  }

  /**
   * Tests {@link Ehcache#removeAll(Set)} for
   * <ul>
   *    <li>non-empty request set</li>
   *    <li>populated {@code Store} - some keys overlap request</li>
   *    <li>{@link Store#bulkCompute} throws after accessing writer</li>
   *    <li>populated {@code CacheLoaderWriter} - all keys overlap</li>
   *    <li>no {@link CacheLoaderWriter#deleteAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testRemoveAllStoreSomeOverlapCacheAccessExceptionAfterWriterFullOverlapNoneFail() throws Exception {
    final Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    final FakeStore fakeStore = new FakeStore(originalStoreContent, Collections.singleton("keyB3"));
    this.store = spy(fakeStore);

    final Map<String, String> originalWriterContent = getEntryMap(KEY_SET_B, KEY_SET_C);
    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);

    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    final Set<String> contentUpdates = fanIn(KEY_SET_B, KEY_SET_C);
    ehcache.removeAll(contentUpdates);

    final InOrder ordered = inOrder(this.store, this.cacheLoaderWriter, this.spiedResilienceStrategy);
    ordered.verify(this.store, atLeast(1))
        .bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), everyItem(isIn(contentUpdates)));
    // ResilienceStrategy invoked; no assertions about Store content
    ordered.verify(this.cacheLoaderWriter, atLeast(1)).deleteAll(getAnyStringIterable());
    assertThat(fakeLoaderWriter.getEntryMap(), equalTo(copyWithout(originalWriterContent, contentUpdates)));
    ordered.verify(this.spiedResilienceStrategy)
        .removeAllFailure(eq(contentUpdates), any(CacheAccessException.class));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.RemoveOutcome.class));
  }

  /**
   * Tests {@link Ehcache#removeAll(Set)} for
   * <ul>
   *    <li>non-empty request set</li>
   *    <li>populated {@code Store} - some keys overlap request</li>
   *    <li>populated {@code CacheLoaderWriter} - all keys overlap</li>
   *    <li>some {@link CacheLoaderWriter#deleteAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testRemoveAllStoreSomeOverlapWriterFullOverlapSomeFail() throws Exception {
    final Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    final FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);

    final Map<String, String> originalWriterContent = getEntryMap(KEY_SET_B, KEY_SET_C, KEY_SET_D);
    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent, KEY_SET_D);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);

    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    final Set<String> contentUpdates = fanIn(KEY_SET_B, KEY_SET_C, KEY_SET_D);
    final Set<String> expectedFailures = KEY_SET_D;
    final Set<String> expectedSuccesses = copyWithout(contentUpdates, expectedFailures);
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
    verifyZeroInteractions(this.spiedResilienceStrategy);

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.RemoveOutcome.class));
  }

  /**
   * Tests {@link Ehcache#removeAll(Set)} for
   * <ul>
   *    <li>non-empty request set</li>
   *    <li>populated {@code Store} - some keys overlap request</li>
   *    <li>{@link Store#bulkCompute} throws before accessing writer</li>
   *    <li>populated {@code CacheLoaderWriter} - all keys overlap</li>
   *    <li>some {@link CacheLoaderWriter#deleteAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testRemoveAllStoreSomeOverlapCacheAccessExceptionBeforeWriterFullOverlapSomeFail() throws Exception {
    final Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    final FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);
    doThrow(new CacheAccessException("")).when(this.store)
        .bulkCompute(getAnyStringSet(), getAnyEntryIterableFunction());

    final Map<String, String> originalWriterContent = getEntryMap(KEY_SET_B, KEY_SET_C, KEY_SET_D);
    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent, KEY_SET_D);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);

    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    final Set<String> contentUpdates = fanIn(KEY_SET_B, KEY_SET_C, KEY_SET_D);
    final Set<String> expectedFailures = KEY_SET_D;
    final Set<String> expectedSuccesses = copyWithout(contentUpdates, expectedFailures);
    try {
      ehcache.removeAll(contentUpdates);
      fail();
    } catch (BulkCacheWritingException e) {
      // Expected
    }

    final InOrder ordered = inOrder(this.store, this.cacheLoaderWriter, this.spiedResilienceStrategy);
    ordered.verify(this.store, atLeast(1))
        .bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), everyItem(isIn(contentUpdates)));
    // ResilienceStrategy invoked; no assertions about Store content
    ordered.verify(this.cacheLoaderWriter, atLeast(1)).deleteAll(getAnyStringIterable());
    assertThat(fakeLoaderWriter.getEntryMap(), equalTo(copyWithout(originalWriterContent, expectedSuccesses)));
    ordered.verify(this.spiedResilienceStrategy)
        .removeAllFailure(eq(contentUpdates), any(CacheAccessException.class), this.bulkExceptionCaptor.capture());

    assertThat(this.bulkExceptionCaptor.getValue().getSuccesses(), Matchers.<Set<?>>equalTo(expectedSuccesses));
    assertThat(this.bulkExceptionCaptor.getValue().getFailures().keySet(), Matchers.<Set<?>>equalTo(expectedFailures));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.RemoveOutcome.class));
  }

  /**
   * Tests {@link Ehcache#removeAll(Set)} for
   * <ul>
   *    <li>non-empty request set</li>
   *    <li>populated {@code Store} - some keys overlap request</li>
   *    <li>{@link Store#bulkCompute} throws after accessing writer</li>
   *    <li>populated {@code CacheLoaderWriter} - all keys overlap</li>
   *    <li>some {@link CacheLoaderWriter#deleteAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testRemoveAllStoreSomeOverlapCacheAccessExceptionAfterWriterFullOverlapSomeFail() throws Exception {
    final Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    final FakeStore fakeStore = new FakeStore(originalStoreContent, Collections.singleton("keyB3"));
    this.store = spy(fakeStore);

    final Map<String, String> originalWriterContent = getEntryMap(KEY_SET_B, KEY_SET_C, KEY_SET_D);
    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent, KEY_SET_D);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);

    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    final Set<String> contentUpdates = fanIn(KEY_SET_B, KEY_SET_C, KEY_SET_D);
    final Set<String> expectedFailures = KEY_SET_D;
    final Set<String> expectedSuccesses = copyWithout(contentUpdates, expectedFailures);
    try {
      ehcache.removeAll(contentUpdates);
      fail();
    } catch (BulkCacheWritingException e) {
      // Expected
    }

    final InOrder ordered = inOrder(this.store, this.cacheLoaderWriter, this.spiedResilienceStrategy);
    ordered.verify(this.store, atLeast(1))
        .bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), everyItem(isIn(contentUpdates)));
    // ResilienceStrategy invoked; no assertions about Store content
    ordered.verify(this.cacheLoaderWriter, atLeast(1)).deleteAll(getAnyStringIterable());
    assertThat(fakeLoaderWriter.getEntryMap(), equalTo(copyWithout(originalWriterContent, expectedSuccesses)));
    ordered.verify(this.spiedResilienceStrategy)
        .removeAllFailure(eq(contentUpdates), any(CacheAccessException.class), this.bulkExceptionCaptor.capture());

    assertThat(this.bulkExceptionCaptor.getValue().getSuccesses(), Matchers.<Set<?>>equalTo(expectedSuccesses));
    assertThat(this.bulkExceptionCaptor.getValue().getFailures().keySet(), Matchers.<Set<?>>equalTo(expectedFailures));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.RemoveOutcome.class));
  }

  /**
   * Tests {@link Ehcache#removeAll(Set)} for
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
    final Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    final FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);

    final Map<String, String> originalWriterContent = getEntryMap(KEY_SET_B, KEY_SET_C, KEY_SET_D);
    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent, KEY_SET_D);
    fakeLoaderWriter.setCompleteFailureKey("keyC4");
    this.cacheLoaderWriter = spy(fakeLoaderWriter);

    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    final Set<String> contentUpdates = fanIn(KEY_SET_B, KEY_SET_C, KEY_SET_D);
    final Set<String> expectedFailures = union(KEY_SET_D, Collections.singleton("keyC4"));
    final Set<String> expectedSuccesses = copyWithout(contentUpdates, expectedFailures);
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
    verifyZeroInteractions(this.spiedResilienceStrategy);

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.RemoveOutcome.class));
  }

  /**
   * Tests {@link Ehcache#removeAll(Set)} for
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
  public void testRemoveAllStoreSomeOverlapCacheAccessExceptionBeforeWriterFullOverlapSomeFailWithAbort() throws Exception {
    final Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    final FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);
    doThrow(new CacheAccessException("")).when(this.store)
        .bulkCompute(getAnyStringSet(), getAnyEntryIterableFunction());

    final Map<String, String> originalWriterContent = getEntryMap(KEY_SET_B, KEY_SET_C, KEY_SET_D);
    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent, KEY_SET_D);
    fakeLoaderWriter.setCompleteFailureKey("keyC4");
    this.cacheLoaderWriter = spy(fakeLoaderWriter);

    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    final Set<String> contentUpdates = fanIn(KEY_SET_B, KEY_SET_C, KEY_SET_D);
    final Set<String> expectedFailures = union(KEY_SET_D, Collections.singleton("keyC4"));
    final Set<String> expectedSuccesses = copyWithout(contentUpdates, expectedFailures);
    try {
      ehcache.removeAll(contentUpdates);
      fail();
    } catch (BulkCacheWritingException e) {
      // Expected
    }

    final InOrder ordered = inOrder(this.store, this.cacheLoaderWriter, this.spiedResilienceStrategy);
    ordered.verify(this.store, atLeast(1))
        .bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), everyItem(isIn(contentUpdates)));
    // ResilienceStrategy invoked; no assertions about Store content
    ordered.verify(this.cacheLoaderWriter, atLeast(1)).deleteAll(getAnyStringIterable());
    ordered.verify(this.spiedResilienceStrategy)
        .removeAllFailure(eq(contentUpdates), any(CacheAccessException.class), this.bulkExceptionCaptor.capture());

    @SuppressWarnings("unchecked")
    final Set<String> bcweSuccesses = (Set<String>)this.bulkExceptionCaptor.getValue().getSuccesses();
    @SuppressWarnings("unchecked")
    final Map<String, Exception> bcweFailures = (Map<String, Exception>)this.bulkExceptionCaptor.getValue().getFailures();

    assertThat(union(bcweSuccesses, bcweFailures.keySet()), equalTo(contentUpdates));
    assertThat(Collections.disjoint(bcweSuccesses, bcweFailures.keySet()), is(true));
    assertThat(bcweSuccesses, everyItem(isIn(expectedSuccesses)));
    assertThat(expectedFailures, everyItem(isIn(bcweFailures.keySet())));
    assertThat(copyWithout(fakeLoaderWriter.getEntryMap(), bcweFailures.keySet()),
        equalTo(copyWithout(copyWithout(originalWriterContent, copyOnly(contentUpdates, bcweSuccesses)),
            bcweFailures.keySet())));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.RemoveOutcome.class));

    this.dumpResults(fakeStore, originalStoreContent, fakeLoaderWriter, originalWriterContent, contentUpdates, expectedFailures,
        expectedSuccesses, bcweSuccesses, bcweFailures);
  }

  /**
   * Tests {@link Ehcache#removeAll(Set)} for
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
  public void testRemoveAllStoreSomeOverlapCacheAccessExceptionAfterWriterFullOverlapSomeFailWithAbort() throws Exception {
    final Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    final FakeStore fakeStore = new FakeStore(originalStoreContent, Collections.singleton("keyB3"));
    this.store = spy(fakeStore);

    final Map<String, String> originalWriterContent = getEntryMap(KEY_SET_B, KEY_SET_C, KEY_SET_D);
    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent, KEY_SET_D);
    fakeLoaderWriter.setCompleteFailureKey("keyC4");
    this.cacheLoaderWriter = spy(fakeLoaderWriter);

    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    final Set<String> contentUpdates = fanIn(KEY_SET_B, KEY_SET_C, KEY_SET_D);
    final Set<String> expectedFailures = union(KEY_SET_D, Collections.singleton("keyC4"));
    final Set<String> expectedSuccesses = copyWithout(contentUpdates, expectedFailures);
    try {
      ehcache.removeAll(contentUpdates);
      fail();
    } catch (BulkCacheWritingException e) {
      // Expected
    }

    final InOrder ordered = inOrder(this.store, this.cacheLoaderWriter, this.spiedResilienceStrategy);
    ordered.verify(this.store, atLeast(1))
        .bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), everyItem(isIn(contentUpdates)));
    // ResilienceStrategy invoked; no assertions about Store content
    ordered.verify(this.cacheLoaderWriter, atLeast(1)).deleteAll(getAnyStringIterable());
    ordered.verify(this.spiedResilienceStrategy)
        .removeAllFailure(eq(contentUpdates), any(CacheAccessException.class), this.bulkExceptionCaptor.capture());

    @SuppressWarnings("unchecked")
    final Set<String> bcweSuccesses = (Set<String>)this.bulkExceptionCaptor.getValue().getSuccesses();
    @SuppressWarnings("unchecked")
    final Map<String, Exception> bcweFailures = (Map<String, Exception>)this.bulkExceptionCaptor.getValue().getFailures();

    assertThat(union(bcweSuccesses, bcweFailures.keySet()), equalTo(contentUpdates));
    assertThat(Collections.disjoint(bcweSuccesses, bcweFailures.keySet()), is(true));
    assertThat(bcweSuccesses, everyItem(isIn(expectedSuccesses)));
    assertThat(expectedFailures, everyItem(isIn(bcweFailures.keySet())));
    assertThat(copyWithout(fakeLoaderWriter.getEntryMap(), bcweFailures.keySet()),
        equalTo(copyWithout(copyWithout(originalWriterContent, copyOnly(contentUpdates, bcweSuccesses)),
            bcweFailures.keySet())));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.RemoveOutcome.class));

    this.dumpResults(fakeStore, originalStoreContent, fakeLoaderWriter, originalWriterContent, contentUpdates, expectedFailures,
        expectedSuccesses, bcweSuccesses, bcweFailures);
  }

  /**
   * Tests {@link Ehcache#removeAll(Set)} for
   * <ul>
   *    <li>non-empty request set</li>
   *    <li>populated {@code Store} - some keys overlap request</li>
   *    <li>populated {@code CacheLoaderWriter} - all keys overlap</li>
   *    <li>all {@link CacheLoaderWriter#deleteAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testRemoveAllStoreSomeOverlapWriterFullOverlapAllFail() throws Exception {
    final Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    final FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);

    final Map<String, String> originalWriterContent = getEntryMap(KEY_SET_B, KEY_SET_C, KEY_SET_D);
    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);
    doThrow(new Exception("deleteAll failed")).when(this.cacheLoaderWriter).deleteAll(getAnyStringIterable());

    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    final Set<String> contentUpdates = fanIn(KEY_SET_B, KEY_SET_C, KEY_SET_D);
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
    verifyZeroInteractions(this.spiedResilienceStrategy);

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.RemoveOutcome.class));
  }

  /**
   * Tests {@link Ehcache#removeAll(Set)} for
   * <ul>
   *    <li>non-empty request set</li>
   *    <li>populated {@code Store} - some keys overlap request</li>
   *    <li>{@link Store#bulkCompute} throws before accessing writer</li>
   *    <li>populated {@code CacheLoaderWriter} - all keys overlap</li>
   *    <li>all {@link CacheLoaderWriter#deleteAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testRemoveAllStoreSomeOverlapCacheAccessExceptionBeforeWriterFullOverlapAllFail() throws Exception {
    final Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    final FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);
    doThrow(new CacheAccessException("")).when(this.store)
        .bulkCompute(getAnyStringSet(), getAnyEntryIterableFunction());

    final Map<String, String> originalWriterContent = getEntryMap(KEY_SET_B, KEY_SET_C, KEY_SET_D);
    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);
    doThrow(new Exception("deleteAll failed")).when(this.cacheLoaderWriter).deleteAll(getAnyStringIterable());

    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    final Set<String> contentUpdates = fanIn(KEY_SET_B, KEY_SET_C, KEY_SET_D);
    try {
      ehcache.removeAll(contentUpdates);
      fail();
    } catch (BulkCacheWritingException e) {
      // Expected
    }

    final InOrder ordered = inOrder(this.store, this.cacheLoaderWriter, this.spiedResilienceStrategy);
    ordered.verify(this.store, atLeast(1))
        .bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), everyItem(isIn(contentUpdates)));
    // ResilienceStrategy invoked; no assertions about Store content
    ordered.verify(this.cacheLoaderWriter, atLeast(1)).deleteAll(getAnyStringIterable());
    assertThat(fakeLoaderWriter.getEntryMap(), equalTo(originalWriterContent));
    ordered.verify(this.spiedResilienceStrategy)
        .removeAllFailure(eq(contentUpdates), any(CacheAccessException.class), this.bulkExceptionCaptor.capture());

    assertThat(this.bulkExceptionCaptor.getValue().getSuccesses(), empty());
    assertThat(this.bulkExceptionCaptor.getValue().getFailures().keySet(), Matchers.<Set<?>>equalTo(contentUpdates));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.RemoveOutcome.class));
  }

  /**
   * Tests {@link Ehcache#removeAll(Set)} for
   * <ul>
   *    <li>non-empty request set</li>
   *    <li>populated {@code Store} - some keys overlap request</li>
   *    <li>{@link Store#bulkCompute} throws after accessing writer</li>
   *    <li>populated {@code CacheLoaderWriter} - all keys overlap</li>
   *    <li>all {@link CacheLoaderWriter#deleteAll(Iterable)} calls fail</li>
   * </ul>
   */
  @Test
  public void testRemoveAllStoreSomeOverlapCacheAccessExceptionAfterWriterFullOverlapAllFail() throws Exception {
    final Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    final FakeStore fakeStore = new FakeStore(originalStoreContent, Collections.singleton("keyB3"));
    this.store = spy(fakeStore);

    final Map<String, String> originalWriterContent = getEntryMap(KEY_SET_B, KEY_SET_C, KEY_SET_D);
    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);
    doThrow(new Exception("deleteAll failed")).when(this.cacheLoaderWriter).deleteAll(getAnyStringIterable());

    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    final Set<String> contentUpdates = fanIn(KEY_SET_B, KEY_SET_C, KEY_SET_D);
    try {
      ehcache.removeAll(contentUpdates);
      fail();
    } catch (BulkCacheWritingException e) {
      // Expected
    }

    final InOrder ordered = inOrder(this.store, this.cacheLoaderWriter, this.spiedResilienceStrategy);
    ordered.verify(this.store, atLeast(1))
        .bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), everyItem(isIn(contentUpdates)));
    // ResilienceStrategy invoked; no assertions about Store content
    ordered.verify(this.cacheLoaderWriter, atLeast(1)).deleteAll(getAnyStringIterable());
    assertThat(fakeLoaderWriter.getEntryMap(), equalTo(originalWriterContent));
    ordered.verify(this.spiedResilienceStrategy)
        .removeAllFailure(eq(contentUpdates), any(CacheAccessException.class), this.bulkExceptionCaptor.capture());

    assertThat(this.bulkExceptionCaptor.getValue().getSuccesses(), empty());
    assertThat(this.bulkExceptionCaptor.getValue().getFailures().keySet(), Matchers.<Set<?>>equalTo(contentUpdates));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.RemoveOutcome.class));
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
    final Ehcache<String, String> ehcache = new Ehcache<String, String>(CACHE_CONFIGURATION, this.store, cacheLoaderWriter, LoggerFactory.getLogger(Ehcache.class + "-" + "EhcacheBasicRemoveAllTest"));
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
   * Returns a Mockito {@code any} Matcher for a {@link org.ehcache.function.Function} over a {@code Map.Entry} {@code Iterable}.
   *
   * @return a Mockito {@code any} matcher for {@code Function}
   */
  @SuppressWarnings("unchecked")
  private static Function<Iterable<? extends Map.Entry<? extends String, ? extends String>>, Iterable<? extends Map.Entry<? extends String, ? extends String>>> getAnyEntryIterableFunction() {
    return any(Function.class);   // unchecked
  }

  /**
   * Returns a Mockito {@code any} Matcher for a {@code String} {@code Iterable}.
   *
   * @return a Mockito {@code any} matcher for {@code Iterable}
   */
  @SuppressWarnings("unchecked")
  private static Iterable<? extends String> getAnyStringIterable() {
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
    debugResults = Boolean.parseBoolean(System.getProperty(EhcacheBasicRemoveAllTest.class.getName() + ".debug", "false"));
  }

  @Rule public TestName name = new TestName();

  /**
   * Writes a dump of test object details to {@code System.out} if, and only if, {@link #debugResults} is enabled.
   *
   * @param fakeStore the {@link org.ehcache.EhcacheBasicCrudBase.FakeStore FakeStore} instance used in the test
   * @param originalStoreContent  the original content provided to {@code fakeStore}
   * @param fakeLoaderWriter the {@link org.ehcache.EhcacheBasicCrudBase.FakeCacheLoaderWriter FakeCacheLoaderWriter} instances used in the test
   * @param originalWriterContent the original content provided to {@code fakeLoaderWriter}
   * @param contentUpdates the {@code Set} provided to the {@link Ehcache#removeAll(java.util.Set)} call in the test
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
      final Set<String> contentUpdates,
      final Set<String> expectedFailures,
      final Set<String> expectedSuccesses,
      final Set<String> bcweSuccesses,
      final Map<String, Exception> bcweFailures) {

    if (!debugResults) {
      return;
    }

    final StringBuilder sb = new StringBuilder(2048);
    final Formatter fmt = new Formatter(sb);
    fmt.format("Dumping results of %s:%n", this.name.getMethodName());

    fmt.format("    Content Update Keys: %s%n", sort(contentUpdates));
    fmt.format("    Original Store Keys : %s%n", sort(originalStoreContent.keySet()));
    fmt.format("    Final Store Keys    : %s%n", sort(fakeStore.getEntryMap().keySet()));
    fmt.format("    Original Writer Keys: %s%n", sort(originalWriterContent.keySet()));
    fmt.format("    Final Writer Keys   : %s%n", sort(fakeLoaderWriter.getEntryMap().keySet()));
    fmt.format("    Expected Successes: %s%n", sort(expectedSuccesses));
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
}
