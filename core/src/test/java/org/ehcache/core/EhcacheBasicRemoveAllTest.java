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
import org.mockito.Captor;
import org.mockito.InOrder;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Formatter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static org.ehcache.core.EhcacheBasicBulkUtil.KEY_SET_A;
import static org.ehcache.core.EhcacheBasicBulkUtil.KEY_SET_B;
import static org.ehcache.core.EhcacheBasicBulkUtil.KEY_SET_C;
import static org.ehcache.core.EhcacheBasicBulkUtil.copyWithout;
import static org.ehcache.core.EhcacheBasicBulkUtil.fanIn;
import static org.ehcache.core.EhcacheBasicBulkUtil.getEntryMap;
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
 * Provides testing of basic REMOVE_ALL operations on an {@code Ehcache}.
 *
 * @author Clifford W. Johnson
 */
@SuppressWarnings("ThrowableResultOfMethodCallIgnored")
public class EhcacheBasicRemoveAllTest extends EhcacheBasicCrudBase {

  /**
   * A Mockito {@code ArgumentCaptor} for the {@code Set} argument to the
   * {@link Store#bulkCompute(Set, Function, java.util.function.Supplier)
   *    Store.bulkCompute(Set, Function, NullaryFunction} method.
   */
  @Captor
  protected ArgumentCaptor<Set<String>> bulkComputeSetCaptor;

  @Test
  public void testRemoveAllNull() throws Exception {
    final Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    final FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);

    final Ehcache<String, String> ehcache = this.getEhcache();
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

    final Set<String> keys = new LinkedHashSet<>();
    for (final String key : KEY_SET_A) {
      keys.add(key);
      if ("keyA2".equals(key)) {
        keys.add(null);
      }
    }
    final Ehcache<String, String> ehcache = this.getEhcache();
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

    final Ehcache<String, String> ehcache = this.getEhcache();
    ehcache.removeAll(Collections.<String>emptySet());

    verify(this.store, never()).bulkCompute(eq(Collections.<String>emptySet()), getAnyEntryIterableFunction());
    assertThat(fakeStore.getEntryMap(), equalTo(originalStoreContent));
    verify(this.resilienceStrategy, never()).removeAllFailure(eq(Collections.<String>emptySet()), any(StoreAccessException.class));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.RemoveOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.RemoveAllOutcome.SUCCESS));
    assertThat(ehcache.getBulkMethodEntries().get(BulkOps.REMOVE_ALL).intValue(), is(0));
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

    final Ehcache<String, String> ehcache = this.getEhcache();

    final Set<String> contentUpdates = fanIn(KEY_SET_A, KEY_SET_C);
    ehcache.removeAll(contentUpdates);

    verify(this.store, atLeast(1)).bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), equalTo(contentUpdates));
    assertThat(fakeStore.getEntryMap(), equalTo(copyWithout(originalStoreContent, contentUpdates)));
    verifyZeroInteractions(this.resilienceStrategy);

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.RemoveOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.RemoveAllOutcome.SUCCESS));
    assertThat(ehcache.getBulkMethodEntries().get(BulkOps.REMOVE_ALL).intValue(), is(KEY_SET_A.size()));
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
  public void testRemoveAllStoreSomeOverlapStoreAccessExceptionBeforeNoWriter() throws Exception {
    final Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    final FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);
    doThrow(new StoreAccessException("")).when(this.store)
        .bulkCompute(getAnyStringSet(), getAnyEntryIterableFunction());

    final Ehcache<String, String> ehcache = this.getEhcache();

    final Set<String> contentUpdates = fanIn(KEY_SET_A, KEY_SET_C);
    ehcache.removeAll(contentUpdates);

    final InOrder ordered = inOrder(this.store, this.resilienceStrategy);
    ordered.verify(this.store, atLeast(1)).bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), everyItem(isIn(contentUpdates)));
    // ResilienceStrategy invoked; no assertions about Store content
    ordered.verify(this.resilienceStrategy)
        .removeAllFailure(eq(contentUpdates), any(StoreAccessException.class));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.RemoveOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.RemoveAllOutcome.FAILURE));
    assertThat(ehcache.getBulkMethodEntries().get(BulkOps.REMOVE_ALL).intValue(), is(0));
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
  public void testRemoveAllStoreSomeOverlapStoreAccessExceptionAfterNoWriter() throws Exception {
    final Map<String, String> originalStoreContent = getEntryMap(KEY_SET_A, KEY_SET_B);
    final FakeStore fakeStore = new FakeStore(originalStoreContent, Collections.singleton("keyA3"));
    this.store = spy(fakeStore);

    final Ehcache<String, String> ehcache = this.getEhcache();

    final Set<String> contentUpdates = fanIn(KEY_SET_A, KEY_SET_C);
    ehcache.removeAll(contentUpdates);

    final InOrder ordered = inOrder(this.store, this.resilienceStrategy);
    ordered.verify(this.store, atLeast(1)).bulkCompute(this.bulkComputeSetCaptor.capture(), getAnyEntryIterableFunction());
    assertThat(this.getBulkComputeArgs(), everyItem(isIn(contentUpdates)));
    // ResilienceStrategy invoked; no assertions about Store content
    ordered.verify(this.resilienceStrategy)
        .removeAllFailure(eq(contentUpdates), any(StoreAccessException.class));

    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.RemoveOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.RemoveAllOutcome.FAILURE));
    assertThat(ehcache.getBulkMethodEntries().get(BulkOps.REMOVE_ALL).intValue(), is(0));
  }

  /**
   * Gets an initialized {@link Ehcache Ehcache} instance
   *
   * @return a new {@code Ehcache} instance
   */
  private Ehcache<String, String> getEhcache() {
    final Ehcache<String, String> ehcache = new Ehcache<>(CACHE_CONFIGURATION, this.store, resilienceStrategy, cacheEventDispatcher, LoggerFactory
      .getLogger(Ehcache.class + "-" + "EhcacheBasicRemoveAllTest"));
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
   * Returns a Mockito {@code any} Matcher for a {@code String} {@code Iterable}.
   *
   * @return a Mockito {@code any} matcher for {@code Iterable}
   */
  @SuppressWarnings("unchecked")
  static Iterable<? extends String> getAnyStringIterable() {
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
    final Set<String> bulkComputeArgs = new LinkedHashSet<>();
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
   * @param fakeStore the {@link org.ehcache.core.EhcacheBasicCrudBase.FakeStore FakeStore} instance used in the test
   * @param originalStoreContent  the original content provided to {@code fakeStore}
   * @param fakeLoaderWriter the {@link org.ehcache.core.EhcacheBasicCrudBase.FakeCacheLoaderWriter FakeCacheLoaderWriter} instances used in the test
   * @param originalWriterContent the original content provided to {@code fakeLoaderWriter}
   * @param contentUpdates the {@code Set} provided to the {@link Ehcache#removeAll(java.util.Set)} call in the test
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
