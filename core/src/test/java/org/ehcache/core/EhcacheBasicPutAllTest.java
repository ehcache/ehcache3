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
import org.ehcache.spi.loaderwriter.BulkCacheWritingException;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.ehcache.spi.resilience.StoreAccessException;
import org.hamcrest.Matchers;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Captor;
import org.mockito.InOrder;
import org.slf4j.LoggerFactory;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Formatter;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.Function;

import static org.ehcache.core.EhcacheBasicBulkUtil.KEY_SET_A;
import static org.ehcache.core.EhcacheBasicBulkUtil.KEY_SET_B;
import static org.ehcache.core.EhcacheBasicBulkUtil.KEY_SET_C;
import static org.ehcache.core.EhcacheBasicBulkUtil.fanIn;
import static org.ehcache.core.EhcacheBasicBulkUtil.getAltEntryMap;
import static org.ehcache.core.EhcacheBasicBulkUtil.getEntryMap;
import static org.ehcache.core.EhcacheBasicBulkUtil.union;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isIn;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

/**
 * Provides testing of basic PUT_ALL operations on an {@code Ehcache}.
 *
 * @author Clifford W. Johnson
 */
@SuppressWarnings("ThrowableResultOfMethodCallIgnored")
public class EhcacheBasicPutAllTest extends EhcacheBasicCrudBase {

  /**
   * A Mockito {@code ArgumentCaptor} for the {@code Set} argument to the
   * {@link Store#bulkCompute(Set, Function, java.util.function.Supplier)
   *    Store.bulkCompute(Set, Function, NullaryFunction} method.
   */
  @Captor
  private ArgumentCaptor<Set<String>> bulkComputeSetCaptor;

  @Test
  public void testPutAllNull() throws Exception {
    final Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    final FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);

    final Ehcache<String, String> ehcache = this.getEhcache();
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

    final Map<String, String> entries = new LinkedHashMap<>();
    for (final Map.Entry<String, String> entry : getEntryMap(KEY_SET_A).entrySet()) {
      final String key = entry.getKey();
      entries.put(key, entry.getValue());
      if ("keyA2".equals(key)) {
        entries.put(null, "nullKey");
      }
    }
    final Ehcache<String, String> ehcache = this.getEhcache();
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

    final Map<String, String> entries = new LinkedHashMap<>();
    for (final Map.Entry<String, String> entry : getEntryMap(KEY_SET_A).entrySet()) {
      final String key = entry.getKey();
      entries.put(key, entry.getValue());
      if ("keyA2".equals(key)) {
        entries.put("keyA2a", null);
      }
    }
    final Ehcache<String, String> ehcache = this.getEhcache();
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

    final Ehcache<String, String> ehcache = this.getEhcache();
    ehcache.putAll(Collections.<String, String>emptyMap());

    verify(this.store, never()).bulkCompute(eq(Collections.<String>emptySet()), getAnyEntryIterableFunction());
    assertThat(fakeStore.getEntryMap(), equalTo(originalStoreContent));
    verify(this.resilienceStrategy, never()).putAllFailure(eq(Collections.<String, String>emptyMap()), any(StoreAccessException.class));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.PutOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.PutAllOutcome.SUCCESS));
    assertThat(ehcache.getBulkMethodEntries().get(BulkOps.PUT_ALL).intValue(), is(0));
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

    final Ehcache<String, String> ehcache = this.getEhcache();

    final Map<String, String> contentUpdates = getAltEntryMap("new_", fanIn(KEY_SET_A, KEY_SET_C));
    ehcache.putAll(contentUpdates);

    verify(this.store, atLeast(1)).bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), equalTo(contentUpdates.keySet()));
    assertThat(fakeStore.getEntryMap(), equalTo(union(originalStoreContent, contentUpdates)));
    verifyZeroInteractions(this.resilienceStrategy);

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.PutOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.PutAllOutcome.SUCCESS));
    assertThat(ehcache.getBulkMethodEntries().get(BulkOps.PUT_ALL).intValue(), is(contentUpdates.size()));
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
  public void testPutAllStoreSomeOverlapStoreAccessExceptionBeforeNoWriter() throws Exception {
    final Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    final FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);
    doThrow(new StoreAccessException("")).when(this.store)
        .bulkCompute(getAnyStringSet(), getAnyEntryIterableFunction());

    final Ehcache<String, String> ehcache = this.getEhcache();

    final Map<String, String> contentUpdates = getAltEntryMap("new_", fanIn(KEY_SET_A, KEY_SET_C));
    ehcache.putAll(contentUpdates);

    final InOrder ordered = inOrder(this.store, this.resilienceStrategy);
    ordered.verify(this.store, atLeast(1)).bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), everyItem(isIn(contentUpdates.keySet())));
    // ResilienceStrategy invoked; no assertions about Store content
    ordered.verify(this.resilienceStrategy)
        .putAllFailure(eq(contentUpdates), any(StoreAccessException.class));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.PutOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.PutAllOutcome.FAILURE));
    assertThat(ehcache.getBulkMethodEntries().get(BulkOps.PUT_ALL).intValue(), is(0));
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
  public void testPutAllStoreSomeOverlapStoreAccessExceptionAfterNoWriter() throws Exception {
    final Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    final FakeStore fakeStore = new FakeStore(originalStoreContent, Collections.singleton("keyA3"));
    this.store = spy(fakeStore);

    final Ehcache<String, String> ehcache = this.getEhcache();

    final Map<String, String> contentUpdates = getAltEntryMap("new_", fanIn(KEY_SET_A, KEY_SET_C));
    ehcache.putAll(contentUpdates);

    final InOrder ordered = inOrder(this.store, this.resilienceStrategy);
    ordered.verify(this.store, atLeast(1)).bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), everyItem(isIn(contentUpdates.keySet())));
    // ResilienceStrategy invoked; no assertions about Store content
    ordered.verify(this.resilienceStrategy)
        .putAllFailure(eq(contentUpdates), any(StoreAccessException.class));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.PutOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.PutAllOutcome.FAILURE));
    assertThat(ehcache.getBulkMethodEntries().get(BulkOps.PUT_ALL).intValue(), is(0));
  }


  /**
   * Gets an initialized {@link Ehcache Ehcache} instance
   *
   * @return a new {@code Ehcache} instance
   */
  private Ehcache<String, String> getEhcache() {
    final Ehcache<String, String> ehcache = new Ehcache<>(CACHE_CONFIGURATION, this.store, resilienceStrategy, cacheEventDispatcher, LoggerFactory
      .getLogger(Ehcache.class + "-" + "EhcacheBasicPutAllTest"));
    ehcache.init();
    assertThat("cache not initialized", ehcache.getStatus(), Matchers.is(Status.AVAILABLE));
    return ehcache;
  }

  /**
   * Returns a Mockito {@code any} Matcher for {@code java.util.Set<String>}.
   *
   * @return a Mockito {@code any} matcher for {@code Set<String>}
   */
  @SuppressWarnings("unchecked")
  static Set<? extends String> getAnyStringSet() {
    return any(Set.class);   // unchecked
  }

  /**
   * Returns a Mockito {@code any} Matcher for a {@link Function} over a {@code Map.Entry} {@code Iterable}.
   *
   * @return a Mockito {@code any} matcher for {@code Function}
   */
  @SuppressWarnings("unchecked")
  static Function<Iterable<? extends Map.Entry<? extends String, ? extends String>>, Iterable<? extends Map.Entry<? extends String, ? extends String>>> getAnyEntryIterableFunction() {
    return any(Function.class);   // unchecked
  }

  /**
   * Returns a Mockito {@code any} Matcher for a {@code Map.Entry} {@code Iterable}.
   *
   * @return a Mockito {@code any} matcher for {@code Iterable}
   */
  @SuppressWarnings("unchecked")
  static Iterable<? extends Map.Entry<? extends String, ? extends String>> getAnyEntryIterable() {
    return any(Iterable.class);
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
   * @param fakeStore the {@link org.ehcache.core.EhcacheBasicCrudBase.FakeStore FakeStore} instance used in the test
   * @param originalStoreContent  the original content provided to {@code fakeStore}
   * @param fakeLoaderWriter the {@link org.ehcache.core.EhcacheBasicCrudBase.FakeCacheLoaderWriter FakeCacheLoaderWriter} instances used in the test
   * @param originalWriterContent the original content provided to {@code fakeLoaderWriter}
   * @param contentUpdates the {@code Map} provided to the {@link Ehcache#putAll(java.util.Map)} call in the test
   * @param expectedFailures the {@code Set} of failing keys expected for the test
   * @param expectedSuccesses the {@code Set} of successful keys expected for the test
   * @param bcweSuccesses the {@code Set} from {@link BulkCacheWritingException#getSuccesses()}
   * @param bcweFailures the {@code Map} from {@link BulkCacheWritingException#getFailures()}
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
    return new TreeMap<>(input);
  }

  /**
   * Collects all arguments captured by {@link #bulkComputeSetCaptor}.
   *
   * @return the argument values collected by {@link #bulkComputeSetCaptor}; the
   *    {@code Iterator} over the resulting {@code Set} returns the values
   *    in the order observed by the captor.
   */
  private Set<String> getBulkComputeArgs() {
    final Set<String> bulkComputeArgs = new LinkedHashSet<>();
    for (final Set<String> set : this.bulkComputeSetCaptor.getAllValues()) {
      bulkComputeArgs.addAll(set);
    }
    return bulkComputeArgs;
  }
}
