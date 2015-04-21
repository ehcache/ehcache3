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

import org.ehcache.exceptions.CacheAccessException;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
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
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
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
    this.store = new FakeStore(Collections.<String,String>emptyMap());
    final Ehcache<String, String> ehcache = this.getEhcache();
    assertThat(ehcache.iterator(), is(notNullValue()));
  }

  /**
   * Tests {@link java.util.Iterator#hasNext()} from {@link Ehcache#iterator()} on an empty cache.
   */
  @Test
  public void testIteratorEmptyStoreHasNext() throws Exception {
    this.store = new FakeStore(Collections.<String,String>emptyMap());
    final Ehcache<String, String> ehcache = this.getEhcache();
    final Iterator<Cache.Entry<String, String>> iterator = ehcache.iterator();
    assertThat(iterator.hasNext(), is(false));
  }

  /**
   * Tests {@link java.util.Iterator#next()} from {@link Ehcache#iterator()} on an empty cache.
   */
  @Test
  public void testIteratorEmptyStoreNext() throws Exception {
    this.store = new FakeStore(Collections.<String,String>emptyMap());
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
  @Test
  public void testIteratorEmptyStoreRemoveBeforeNext() throws Exception {
    this.store = new FakeStore(Collections.<String,String>emptyMap());
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
    this.store = new FakeStore(this.getTestStoreEntries());
    final Ehcache<String, String> ehcache = this.getEhcache();

    assertThat(ehcache.iterator(), is(notNullValue()));
  }

  /**
   * Tests {@link java.util.Iterator#hasNext()} from {@link Ehcache#iterator()} on a non-empty cache.
   */
  @Test
  public void testIteratorNonEmptyStoreHasNext() throws Exception {
    this.store = new FakeStore(this.getTestStoreEntries());
    final Ehcache<String, String> ehcache = this.getEhcache();

    final Iterator<Cache.Entry<String, String>> iterator = ehcache.iterator();
    assertThat(iterator.hasNext(), is(true));
  }

  /**
   * Tests {@link java.util.Iterator#next()} from {@link Ehcache#iterator()} on a non-empty cache.
   */
  @Test
  public void testIteratorNonEmptyStoreNext() throws Exception {
    this.store = new FakeStore(this.getTestStoreEntries());
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
    this.store = new FakeStore(testStoreEntries);
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
    final FakeStore fakeStore = new FakeStore(testStoreEntries);
    this.store = fakeStore;

    // Set CacheLoaderWriter & Store to have the same entries initially
    final FakeCacheLoaderWriter fakeWriterWriter = new FakeCacheLoaderWriter(testStoreEntries);
    final Ehcache<String, String> ehcache = this.getEhcache(fakeWriterWriter);

    final Iterator<Cache.Entry<String, String>> iterator = ehcache.iterator();
    while (iterator.hasNext()) {
      final Cache.Entry<String, String> entry = iterator.next();
      if (entry.getKey().equals("keyA")) {
        iterator.remove();
      }
    }

    testStoreEntries.remove("keyA");
    final Map<String, String> storeEntries = new HashMap<String, String>(fakeStore.getEntryMap());
    for (Map.Entry<String, String> expectedEntry : testStoreEntries.entrySet()) {
      final String expectedEntryKey = expectedEntry.getKey();
      assertThat(storeEntries, hasEntry(equalTo(expectedEntryKey), equalTo(expectedEntry.getValue())));
      storeEntries.remove(expectedEntryKey);
    }
    assertThat("Iterator.remove removed incorrect Store entry", storeEntries.isEmpty(), is(true));

    final Map<String, String> writerEntries = new HashMap<String, String>(fakeWriterWriter.getEntryMap());
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
    final FakeStore fakeStore = new FakeStore(testStoreEntries);
    this.store = fakeStore;

    // Set CacheLoaderWriter & Store to have the same entries initially
    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(testStoreEntries);
    final Ehcache<String, String> ehcache = this.getEhcache(fakeLoaderWriter);

    final Iterator<Cache.Entry<String, String>> iterator = ehcache.iterator();
    while (iterator.hasNext()) {
      iterator.next();
      iterator.remove();
    }
    assertThat("Failed to remove all entries from Store", fakeStore.getEntryMap().isEmpty(), is(true));
    assertThat("Failed to remove all entries via CacheLoaderWriter", fakeLoaderWriter.getEntryMap().isEmpty(), is(true));
  }

  /**
   * Tests {@link java.util.Iterator#hasNext()} <b>after</b> exhausting the {@code Iterator} returned
   * from {@link Ehcache#iterator()} on a non-empty cache.
   */
  @Test
  public void testIteratorNonEmptyHasNextAfterLast() throws Exception {
    this.store = new FakeStore(this.getTestStoreEntries());
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
    this.store = new FakeStore(this.getTestStoreEntries());
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
  @Test
  public void testIteratorNonEmptyRemoveTwice() throws Exception {
    final Map<String, String> testStoreEntries = this.getTestStoreEntries();
    this.store = new FakeStore(testStoreEntries);

    // Set CacheLoaderWriter & Store to have the same entries initially
    final FakeCacheLoaderWriter fakeWriterWriter = new FakeCacheLoaderWriter(testStoreEntries);
    final Ehcache<String, String> ehcache = this.getEhcache(fakeWriterWriter);

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
  @Test
  public void testIteratorNonEmptyRemoveAfterLast() throws Exception {
    final Map<String, String> testStoreEntries = this.getTestStoreEntries();
    this.store = new FakeStore(testStoreEntries);

    // Set CacheLoaderWriter & Store to have the same entries initially
    final FakeCacheLoaderWriter fakeWriterWriter = new FakeCacheLoaderWriter(testStoreEntries);
    final Ehcache<String, String> ehcache = this.getEhcache(fakeWriterWriter);

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

  /**
   * Tests the {@link java.util.Iterator} returned when the {@link org.ehcache.spi.cache.Store Store}
   * throws a {@link org.ehcache.exceptions.CacheAccessException CacheAccessException} from
   * {@code Store.iterator}.
   */
  @Test
  public void testIteratorCacheAccessException() throws Exception {
    doThrow(new CacheAccessException("")).when(this.store).iterator();
    final Ehcache<String, String> ehcache = this.getEhcache();

    final Iterator<Cache.Entry<String, String>> iterator = ehcache.iterator();
    assertThat(iterator, is(notNullValue()));
    verify(this.spiedResilienceStrategy).iteratorFailure(any(CacheAccessException.class));
    assertThat(iterator.hasNext(), is(false));

    try {
      iterator.next();
      fail();
    } catch (NoSuchElementException e) {
      // Expected
    }

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
   * Gets an initialized {@link org.ehcache.Ehcache Ehcache} instance using a 
   * mock {@link CacheLoaderWriter} instance which throws for any method called.
   *
   * @return a new {@code Ehcache} instance
   */
  private Ehcache<String, String> getEhcache() throws Exception {

    @SuppressWarnings("unchecked")
    final CacheLoaderWriter<String, String> cacheLoaderWriter = mock(CacheLoaderWriter.class);
    doThrow(new UnsupportedOperationException()).when(cacheLoaderWriter).delete(anyString());
    doThrow(new UnsupportedOperationException()).when(cacheLoaderWriter).deleteAll(getAnyStringIterable());
    doThrow(new UnsupportedOperationException()).when(cacheLoaderWriter).write(anyString(), anyString());
    doThrow(new UnsupportedOperationException()).when(cacheLoaderWriter).writeAll(getAnyMapEntryIterable());
    doThrow(new UnsupportedOperationException()).when(cacheLoaderWriter).load(anyString());
    doThrow(new UnsupportedOperationException()).when(cacheLoaderWriter).loadAll(getAnyStringIterable());
    

    return this.getEhcache(cacheLoaderWriter);
  }

  /**
   * Gets an initialized {@link Ehcache Ehcache} instance using the
   * {@link CacheLoaderWriter} provided.
   *
   * @param cacheLoaderWriter the {@code CacheLoaderWriter} to use in the {@link org.ehcache.Ehcache Ehcache} instance
   * @return a new {@code Ehcache} instance
   */
  private Ehcache<String, String> getEhcache(CacheLoaderWriter<String, String> cacheLoaderWriter) throws Exception {
    InternalRuntimeConfigurationImpl<String, String> internalRuntimeConfiguration =
        new InternalRuntimeConfigurationImpl<String, String>(CACHE_CONFIGURATION, this.store, null);
    RuntimeConfiguration<String, String> runtimeConfiguration =
        new RuntimeConfiguration<String, String>(CACHE_CONFIGURATION, internalRuntimeConfiguration);
    final Ehcache<String, String> ehcache = new Ehcache<String, String>(runtimeConfiguration, this.store, cacheLoaderWriter, LoggerFactory.getLogger(Ehcache.class + "-" + "EhcacheBasicIteratorTest"));
    ehcache.init();
    assertThat("cache not initialized", ehcache.getStatus(), is(Status.AVAILABLE));
    this.spiedResilienceStrategy = this.setResilienceStrategySpy(ehcache);
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
