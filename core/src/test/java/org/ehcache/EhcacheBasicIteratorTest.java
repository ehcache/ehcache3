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

import org.ehcache.spi.loader.CacheLoader;
import org.ehcache.spi.writer.CacheWriter;
import org.junit.Ignore;
import org.junit.Test;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

import static junit.framework.Assert.fail;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.hasEntry;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Matchers.any;

/**
 * Provides testing of basic ITERATOR operations on an {@code Ehcache}.
 * <p/>
 * The {@link Ehcache#iterator()} tests require the use of a {@link org.ehcache.spi.cache.Store Store}
 * implementation which returns an {@link java.util.Iterator} from the
 * {@link org.ehcache.spi.cache.Store#iterator() Store.iterator} method that does <b>not</b>
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
    final Ehcache<String, String> ehcache = this.getEhcache();
    assertThat(ehcache.iterator(), is(notNullValue()));
  }

  /**
   * Tests {@link java.util.Iterator#hasNext()} from {@link Ehcache#iterator()} on an empty cache.
   */
  @Ignore("Iterator.hasNext incorrectly throws NullPointerException")
  @Test
  public void testIteratorEmptyStoreHasNext() throws Exception {
    final Ehcache<String, String> ehcache = this.getEhcache();
    final Iterator<Cache.Entry<String, String>> iterator = ehcache.iterator();
    assertThat(iterator.hasNext(), is(false));
  }

  /**
   * Tests {@link java.util.Iterator#next()} from {@link Ehcache#iterator()} on an empty cache.
   */
  @Ignore("Iterator.next incorrectly throws NullPointerException")
  @Test
  public void testIteratorEmptyStoreNext() throws Exception {
    final Ehcache<String, String> ehcache = this.getEhcache();
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
  @Ignore("Iterator.remove incorrectly throws NullPointerException")
  @Test
  public void testIteratorEmptyStoreRemoveBeforeNext() throws Exception {
    final Ehcache<String, String> ehcache = this.getEhcache();
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
    this.store = new MockStore(this.getTestStoreEntries());
    final Ehcache<String, String> ehcache = this.getEhcache();

    assertThat(ehcache.iterator(), is(notNullValue()));
  }

  /**
   * Tests {@link java.util.Iterator#hasNext()} from {@link Ehcache#iterator()} on a non-empty cache.
   */
  @Test
  public void testIteratorNonEmptyStoreHasNext() throws Exception {
    this.store = new MockStore(this.getTestStoreEntries());
    final Ehcache<String, String> ehcache = this.getEhcache();

    final Iterator<Cache.Entry<String, String>> iterator = ehcache.iterator();
    assertThat(iterator.hasNext(), is(true));
  }

  /**
   * Tests {@link java.util.Iterator#next()} from {@link Ehcache#iterator()} on a non-empty cache.
   */
  @Test
  public void testIteratorNonEmptyStoreNext() throws Exception {
    this.store = new MockStore(this.getTestStoreEntries());
    final Ehcache<String, String> ehcache = this.getEhcache();
    final Iterator<Cache.Entry<String, String>> iterator = ehcache.iterator();
    assertThat(iterator.next(), is(notNullValue()));
  }

  /**
   * Tests fetching all entries via an {@link Ehcache#iterator()} on a non-empty cache.
   */
  @Test
  public void testIteratorNonEmptyAll() throws Exception {
    final Map<String, String> testStoreEntries = this.getTestStoreEntries();
    this.store = new MockStore(testStoreEntries);
    final Ehcache<String, String> ehcache = this.getEhcache();
    for (Cache.Entry<String, String> cacheEntry : ehcache) {
      final String cacheEntryKey = cacheEntry.getKey();
      assertThat(testStoreEntries, hasEntry(equalTo(cacheEntryKey), equalTo(cacheEntry.getValue())));
      testStoreEntries.remove(cacheEntryKey);
    }
    assertThat("Iterator did not return all values", testStoreEntries.isEmpty(), is(true));
  }

  /**
   * Tests {@link java.util.Iterator#remove()} from {@link Ehcache#iterator()} on a non-empty cache.
   */
  @Test
  public void testIteratorNonEmptyRemoveOne() throws Exception {
    final Map<String, String> testStoreEntries = this.getTestStoreEntries();
    final MockStore realStore = new MockStore(testStoreEntries);
    this.store = realStore;

    // Set CacheWriter & Store to have the same entries initially
    final MockCacheWriter realCacheWriter = new MockCacheWriter(testStoreEntries);
    final Ehcache<String, String> ehcache = this.getEhcache(realCacheWriter);

    final Iterator<Cache.Entry<String, String>> iterator = ehcache.iterator();
    while (iterator.hasNext()) {
      final Cache.Entry<String, String> entry = iterator.next();
      if (entry.getKey().equals("keyA")) {
        iterator.remove();
      }
    }

    testStoreEntries.remove("keyA");
    final Map<String, String> storeEntries = new HashMap<String, String>(realStore.getMap());
    for (Map.Entry<String, String> expectedEntry : testStoreEntries.entrySet()) {
      final String expectedEntryKey = expectedEntry.getKey();
      assertThat(storeEntries, hasEntry(equalTo(expectedEntryKey), equalTo(expectedEntry.getValue())));
      storeEntries.remove(expectedEntryKey);
    }
    assertThat("Iterator.remove removed incorrect Store entry", storeEntries.isEmpty(), is(true));

    final Map<String, String> writerEntries = new HashMap<String, String>(realCacheWriter.getEntries());
    for (Map.Entry<String, String> expectedEntry : testStoreEntries.entrySet()) {
      final String expectedEntryKey = expectedEntry.getKey();
      assertThat(writerEntries, hasEntry(equalTo(expectedEntryKey), equalTo(expectedEntry.getValue())));
      writerEntries.remove(expectedEntryKey);
    }
    assertThat("Iterator.remove removed incorrect Writer entry", writerEntries.isEmpty(), is(true));
  }

  /**
   * Tests removal of all entries from {@link Ehcache#iterator()} on a non-empty cache.
   */
  @Test
  public void testIteratorNonEmptyRemoveAll() throws Exception {
    final Map<String, String> testStoreEntries = this.getTestStoreEntries();
    final MockStore realStore = new MockStore(testStoreEntries);
    this.store = realStore;

    // Set CacheWriter & Store to have the same entries initially
    final MockCacheWriter realCacheWriter = new MockCacheWriter(testStoreEntries);
    final Ehcache<String, String> ehcache = this.getEhcache(realCacheWriter);

    final Iterator<Cache.Entry<String, String>> iterator = ehcache.iterator();
    while (iterator.hasNext()) {
      iterator.next();
      iterator.remove();
    }
    assertThat("Failed to remove all entries from Store", realStore.getMap().isEmpty(), is(true));
    assertThat("Failed to remove all entries via CacheWriter", realCacheWriter.getEntries().isEmpty(), is(true));
  }

  /**
   * Tests {@link java.util.Iterator#hasNext()} <b>after</b> exhausting the {@code Iterator} returned
   * from {@link Ehcache#iterator()} on a non-empty cache.
   */
  @Test
  public void testIteratorNonEmptyHasNextAfterLast() throws Exception {
    this.store = new MockStore(this.getTestStoreEntries());
    final Ehcache<String, String> ehcache = this.getEhcache();

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
    this.store = new MockStore(this.getTestStoreEntries());
    final Ehcache<String, String> ehcache = this.getEhcache();

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
   * Tests {@link java.util.Iterator#remove()} twice on the same entry returned from the {@code Iterator}
   * returned from {@link Ehcache#iterator()} on a non-empty cache.
   */
  @Ignore("Iterator.remove on removed entry incorrectly succeeds")
  @Test
  public void testIteratorNonEmptyRemoveTwice() throws Exception {
    final Map<String, String> testStoreEntries = this.getTestStoreEntries();
    this.store = new MockStore(testStoreEntries);

    // Set CacheWriter & Store to have the same entries initially
    final MockCacheWriter realCacheWriter = new MockCacheWriter(testStoreEntries);
    final Ehcache<String, String> ehcache = this.getEhcache(realCacheWriter);

    final Iterator<Cache.Entry<String, String>> iterator = ehcache.iterator();
    while (iterator.hasNext()) {
      final Cache.Entry<String, String> entry = iterator.next();
      if (entry.getKey().equals("keyA")) {
        iterator.remove();
        try {
          iterator.remove();
          fail();
        } catch (IllegalStateException e) {
          // Expected
        }
        break;
      }
    }
  }

  /**
   * Tests {@link java.util.Iterator#remove()} <b>after</b> removing the last entry returned from the {@code Iterator}
   * returned from {@link Ehcache#iterator()} on a non-empty cache.
   */
  @Ignore("Iterator.remove on removed entry incorrectly succeeds")
  @Test
  public void testIteratorNonEmptyRemoveAfterLast() throws Exception {
    final Map<String, String> testStoreEntries = this.getTestStoreEntries();
    this.store = new MockStore(testStoreEntries);

    // Set CacheWriter & Store to have the same entries initially
    final MockCacheWriter realCacheWriter = new MockCacheWriter(testStoreEntries);
    final Ehcache<String, String> ehcache = this.getEhcache(realCacheWriter);

    final Iterator<Cache.Entry<String, String>> iterator = ehcache.iterator();
    while (iterator.hasNext()) {
      iterator.next();
    }

    iterator.remove();      // Expected to remove last observed entry
    try {
      iterator.remove();
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }
  }

  private Map<String, String> getTestStoreEntries() {
    final Map<String, String> storeEntries = new HashMap<String, String>();
    storeEntries.put("key1", "value1");
    storeEntries.put("keyA", "valueA");
    storeEntries.put("key2", "value2");
    storeEntries.put("keyB", "valueB");
    return storeEntries;
  }

  /**
   * Gets an initialized {@link org.ehcache.Ehcache Ehcache} instance using mock
   * {@link org.ehcache.spi.loader.CacheLoader CacheLoader} and
   * {@link org.ehcache.spi.writer.CacheWriter CacheWriter} instances
   * which throw for any method called.
   *
   * @return a new {@code Ehcache} instance
   */
  private Ehcache<String, String> getEhcache() throws Exception {

    @SuppressWarnings("unchecked")
    final CacheWriter<String, String> cacheWriter = mock(CacheWriter.class);
    when(cacheWriter.delete(anyString())).thenThrow(new UnsupportedOperationException());
    when(cacheWriter.delete(anyString(), anyString())).thenThrow(new UnsupportedOperationException());
    when(cacheWriter.deleteAll(getAnyStringIterable())).thenThrow(new UnsupportedOperationException());
    doThrow(new UnsupportedOperationException()).when(cacheWriter).write(anyString(), anyString());
    doThrow(new UnsupportedOperationException()).when(cacheWriter).write(anyString(), anyString(), anyString());
    when(cacheWriter.writeAll(getAnyMapEntryIterable())).thenThrow(new UnsupportedOperationException());

    return this.getEhcache(cacheWriter);
  }

  /**
   * Gets an initialized {@link Ehcache Ehcache} instance using a mock
   * {@link org.ehcache.spi.loader.CacheLoader CacheLoader} instance which throws for
   * any any method called and the
   * {@link org.ehcache.spi.writer.CacheWriter CacheWriter} provided.
   *
   * @param cacheWriter the {@code CacheWriter} to use in the {@link org.ehcache.Ehcache Ehcache} instance
   * @return a new {@code Ehcache} instance
   */
  private Ehcache<String, String> getEhcache(final CacheWriter<String, String> cacheWriter) throws Exception {
    @SuppressWarnings("unchecked")
    final CacheLoader<String, String> cacheLoader = mock(CacheLoader.class);
    when(cacheLoader.load(anyString())).thenThrow(new UnsupportedOperationException());
    when(cacheLoader.loadAll(getAnyStringIterable())).thenThrow(new UnsupportedOperationException());

    final Ehcache<String, String> ehcache = new Ehcache<String, String>(CACHE_CONFIGURATION, this.store, cacheLoader, cacheWriter);
    ehcache.init();
    assertThat("cache not initialized", ehcache.getStatus(), is(Status.AVAILABLE));
    return ehcache;
  }

  @SuppressWarnings("unchecked")
  private static Iterable<? extends String> getAnyStringIterable() {
    return any(Iterable.class);
  }

  @SuppressWarnings("unchecked")
  private static Iterable<? extends Map.Entry<? extends String, ? extends String>> getAnyMapEntryIterable() {
    return any(Iterable.class);
  }
}
