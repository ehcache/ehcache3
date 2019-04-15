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

import org.ehcache.Cache;
import org.ehcache.Status;
import org.ehcache.core.spi.store.Store;
import org.ehcache.core.spi.store.Store.RemoveStatus;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.ehcache.spi.resilience.StoreAccessException;
import org.hamcrest.Matchers;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.hasEntry;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * Provides testing of basic ITERATOR operations on an {@code Ehcache}.
 * <p>
 * The {@link Ehcache#iterator()} tests require the use of a {@link Store Store}
 * implementation which returns an {@link java.util.Iterator} from the
 * {@link Store#iterator() Store.iterator} method that does <b>not</b>
 * throw a {@link java.util.ConcurrentModificationException ConcurrentModificationException}.
 *
 * @author Clifford W. Johnson
 */
public class EhcacheBasicIteratorTest extends EhcacheBasicCrudBase {

  /**
   * Tests {@link Ehcache#iterator()} on an empty cache.
   */
  @Test
  public void testIteratorEmptyStoreGet() throws Exception {
    this.store = new FakeStore(Collections.emptyMap());
    final InternalCache<String, String> ehcache = this.getEhcache();
    assertThat(ehcache.iterator(), is(notNullValue()));
  }

  /**
   * Tests {@link java.util.Iterator#hasNext()} from {@link Ehcache#iterator()} on an empty cache.
   */
  @Test
  public void testIteratorEmptyStoreHasNext() throws Exception {
    this.store = new FakeStore(Collections.emptyMap());
    final InternalCache<String, String> ehcache = this.getEhcache();
    final Iterator<Cache.Entry<String, String>> iterator = ehcache.iterator();
    assertThat(iterator.hasNext(), is(false));
  }

  /**
   * Tests {@link java.util.Iterator#next()} from {@link Ehcache#iterator()} on an empty cache.
   */
  @Test
  public void testIteratorEmptyStoreNext() throws Exception {
    this.store = new FakeStore(Collections.emptyMap());
    final InternalCache<String, String> ehcache = this.getEhcache();
    final Iterator<Cache.Entry<String, String>> iterator = ehcache.iterator();
    try {
      iterator.next();
      fail();
    } catch (NoSuchElementException e) {
      // expected
    }
  }

  /**
   * Tests {@link java.util.Iterator#remove()} from {@link Ehcache#iterator()} on an empty cache.
   */
  @Test
  public void testIteratorEmptyStoreRemoveBeforeNext() throws Exception {
    this.store = new FakeStore(Collections.emptyMap());
    final InternalCache<String, String> ehcache = this.getEhcache();
    final Iterator<Cache.Entry<String, String>> iterator = ehcache.iterator();
    try {
      iterator.remove();
      fail();
    } catch (IllegalStateException e) {
      // expected
    }
  }

  /**
   * Tests {@link Ehcache#iterator()} on a non-empty cache.
   */
  @Test
  public void testIteratorNonEmptyStoreGet() throws Exception {
    this.store = new FakeStore(this.getTestStoreEntries());
    final InternalCache<String, String> ehcache = this.getEhcache();

    assertThat(ehcache.iterator(), is(notNullValue()));
  }

  /**
   * Tests {@link java.util.Iterator#hasNext()} from {@link Ehcache#iterator()} on a non-empty cache.
   */
  @Test
  public void testIteratorNonEmptyStoreHasNext() throws Exception {
    this.store = new FakeStore(this.getTestStoreEntries());
    final InternalCache<String, String> ehcache = this.getEhcache();

    final Iterator<Cache.Entry<String, String>> iterator = ehcache.iterator();
    assertThat(iterator.hasNext(), is(true));
  }

  /**
   * Tests {@link java.util.Iterator#next()} from {@link Ehcache#iterator()} on a non-empty cache.
   */
  @Test
  public void testIteratorNonEmptyStoreNext() throws Exception {
    this.store = new FakeStore(this.getTestStoreEntries());
    final InternalCache<String, String> ehcache = this.getEhcache();
    final Iterator<Cache.Entry<String, String>> iterator = ehcache.iterator();
    assertThat(iterator.next(), is(notNullValue()));
  }

  /**
   * Tests fetching all entries via an {@link Ehcache#iterator()} on a non-empty cache.
   */
  @Test
  public void testIteratorNonEmptyAll() throws Exception {
    final Map<String, String> testStoreEntries = this.getTestStoreEntries();
    this.store = new FakeStore(testStoreEntries);
    final InternalCache<String, String> ehcache = this.getEhcache();
    for (Cache.Entry<String, String> cacheEntry : ehcache) {
      final String cacheEntryKey = cacheEntry.getKey();
      assertThat(testStoreEntries, hasEntry(equalTo(cacheEntryKey), equalTo(cacheEntry.getValue())));
      testStoreEntries.remove(cacheEntryKey);
    }
    assertThat("Iterator did not return all values", testStoreEntries.isEmpty(), is(true));
  }

  /**
   * Tests {@link java.util.Iterator#hasNext()} <b>after</b> exhausting the {@code Iterator} returned
   * from {@link Ehcache#iterator()} on a non-empty cache.
   */
  @Test
  public void testIteratorNonEmptyHasNextAfterLast() throws Exception {
    this.store = new FakeStore(this.getTestStoreEntries());
    final InternalCache<String, String> ehcache = this.getEhcache();

    final Iterator<Cache.Entry<String, String>> iterator = ehcache.iterator();
    while (iterator.hasNext()) {
      iterator.next();
    }

    assertThat(iterator.hasNext(), is(false));
  }

  /**
   * Tests {@link java.util.Iterator#next()} <b>after</b> exhausting the {@code Iterator} returned
   * from {@link Ehcache#iterator()} on a non-empty cache.
   */
  @Test
  public void testIteratorNonEmptyNextAfterLast() throws Exception {
    this.store = new FakeStore(this.getTestStoreEntries());
    final InternalCache<String, String> ehcache = this.getEhcache();

    final Iterator<Cache.Entry<String, String>> iterator = ehcache.iterator();
    while (iterator.hasNext()) {
      iterator.next();
    }

    try {
      iterator.next();
      fail();
    } catch (NoSuchElementException e) {
      // expected
    }
  }

  /**
   * Tests the {@link java.util.Iterator} returned when the {@link Store Store}
   * throws a {@link StoreAccessException StoreAccessException} from
   * {@code Store.iterator}.
   */
  @Test
  public void testIteratorStoreAccessException() throws Exception {
    @SuppressWarnings("unchecked")
    Store.ValueHolder<String> valueHolder = mock(Store.ValueHolder.class);
    doReturn("bar").when(valueHolder).get();

    @SuppressWarnings("unchecked")
    Cache.Entry<String, Store.ValueHolder<String>> storeEntry = mock(Cache.Entry.class);
    doReturn(valueHolder).when(storeEntry).getValue();
    doReturn("foo").when(storeEntry).getKey();

    @SuppressWarnings("unchecked")
    Store.Iterator<Cache.Entry<String, Store.ValueHolder<String>>> storeIterator = mock(Store.Iterator.class);
    doReturn(true).when(storeIterator).hasNext();
    doReturn(storeEntry).when(storeIterator).next();

    doReturn(storeIterator).when(this.store).iterator();
    doReturn(valueHolder).when(this.store).get(eq("foo"));

    final InternalCache<String, String> ehcache = this.getEhcache();
    final Iterator<Cache.Entry<String, String>> iterator = ehcache.iterator();
    assertThat(iterator, is(notNullValue()));
    assertThat(iterator.hasNext(), is(true));
    StoreAccessException exception = new StoreAccessException("");
    doThrow(exception).when(storeIterator).next();
    Cache.Entry<String, String> entry = iterator.next();
    assertThat(entry.getKey(), is("foo"));
    assertThat(entry.getValue(), is("bar"));

    doReturn(RemoveStatus.REMOVED).when(this.store).remove(anyString(), anyString());

    iterator.next();
    verify(resilienceStrategy).iteratorFailure(exception);

    assertThat(iterator.hasNext(), is(false));

    try {
      iterator.next();
      fail();
    } catch (NoSuchElementException e) {
      // Expected
    }

    try {
      iterator.remove();
    } catch (Exception e) {
      fail();
    }

    try {
      iterator.remove();
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }
  }

  protected Map<String, String> getTestStoreEntries() {
    final Map<String, String> storeEntries = new HashMap<>();
    storeEntries.put("key1", "value1");
    storeEntries.put("keyA", "valueA");
    storeEntries.put("key2", "value2");
    storeEntries.put("keyB", "valueB");
    return storeEntries;
  }

  /**
   * Gets an initialized {@link InternalCache Ehcache} instance using a
   * mock {@link CacheLoaderWriter} instance which throws for any method called.
   *
   * @return a new {@code Ehcache} instance
   */
  @SuppressWarnings("unchecked")
  protected InternalCache<String, String> getEhcache() throws Exception {
    final Ehcache<String, String> ehcache = new Ehcache<>(CACHE_CONFIGURATION, this.store, resilienceStrategy, cacheEventDispatcher, LoggerFactory
      .getLogger(Ehcache.class + "-" + "EhcacheBasicIteratorTest"));
    ehcache.init();
    assertThat("cache not initialized", ehcache.getStatus(), Matchers.is(Status.AVAILABLE));
    return ehcache;
  }

  @SuppressWarnings("unchecked")
  static Iterable<? extends String> getAnyStringIterable() {
    return any(Iterable.class);
  }

  @SuppressWarnings("unchecked")
  static Iterable<? extends Map.Entry<? extends String, ? extends String>> getAnyMapEntryIterable() {
    return any(Iterable.class);
  }
}
