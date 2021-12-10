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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.ehcache.Cache;
import org.ehcache.Status;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.hamcrest.Matchers;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

/**
 * @author Abhilash
 *
 */
public class EhcacheWithLoaderWriterBasicIteratorTest extends EhcacheBasicIteratorTest {

  /**
   * Tests {@link java.util.Iterator#remove()} from {@link EhcacheWithLoaderWriter#iterator()} on a non-empty cache.
   */
  @Test
  public void testIteratorNonEmptyRemoveOne() throws Exception {
    final Map<String, String> testStoreEntries = this.getTestStoreEntries();
    final FakeStore fakeStore = new FakeStore(testStoreEntries);
    this.store = fakeStore;

    // Set CacheLoaderWriter & Store to have the same entries initially
    final FakeCacheLoaderWriter fakeWriterWriter = new FakeCacheLoaderWriter(testStoreEntries);
    final InternalCache<String, String> ehcache = this.getEhcache(fakeWriterWriter);

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
   * Tests removal of all entries from {@link EhcacheWithLoaderWriter#iterator()} on a non-empty cache.
   */
  @Test
  public void testIteratorNonEmptyRemoveAll() throws Exception {
    final Map<String, String> testStoreEntries = this.getTestStoreEntries();
    final FakeStore fakeStore = new FakeStore(testStoreEntries);
    this.store = fakeStore;

    // Set CacheLoaderWriter & Store to have the same entries initially
    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(testStoreEntries);
    final InternalCache<String, String> ehcache = this.getEhcache(fakeLoaderWriter);

    final Iterator<Cache.Entry<String, String>> iterator = ehcache.iterator();
    while (iterator.hasNext()) {
      iterator.next();
      iterator.remove();
    }
    assertThat("Failed to remove all entries from Store", fakeStore.getEntryMap().isEmpty(), is(true));
    assertThat("Failed to remove all entries via CacheLoaderWriter", fakeLoaderWriter.getEntryMap().isEmpty(), is(true));
  }

  /**
   * Tests {@link java.util.Iterator#remove()} twice on the same entry returned from the {@code Iterator}
   * returned from {@link EhcacheWithLoaderWriter#iterator()} on a non-empty cache.
   */
  @Test
  public void testIteratorNonEmptyRemoveTwice() throws Exception {
    final Map<String, String> testStoreEntries = this.getTestStoreEntries();
    this.store = new FakeStore(testStoreEntries);

    // Set CacheLoaderWriter & Store to have the same entries initially
    final FakeCacheLoaderWriter fakeWriterWriter = new FakeCacheLoaderWriter(testStoreEntries);
    final InternalCache<String, String> ehcache = this.getEhcache(fakeWriterWriter);

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
   * returned from {@link EhcacheWithLoaderWriter#iterator()} on a non-empty cache.
   */
  @Test
  public void testIteratorNonEmptyRemoveAfterLast() throws Exception {
    final Map<String, String> testStoreEntries = this.getTestStoreEntries();
    this.store = new FakeStore(testStoreEntries);

    // Set CacheLoaderWriter & Store to have the same entries initially
    final FakeCacheLoaderWriter fakeWriterWriter = new FakeCacheLoaderWriter(testStoreEntries);
    final InternalCache<String, String> ehcache = this.getEhcache(fakeWriterWriter);

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

  @Override
  protected InternalCache<String, String> getEhcache() throws Exception {

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
  * creates an instance {@code EhcacheWithLoaderWriter}
  *
  * @param cacheLoaderWriter
  * @return instance {@link EhcacheWithLoaderWriter}
  * @throws Exception
  */
  private InternalCache<String, String> getEhcache(CacheLoaderWriter<String, String> cacheLoaderWriter) throws Exception {
    final EhcacheWithLoaderWriter<String, String> ehcache = new EhcacheWithLoaderWriter<String, String>(CACHE_CONFIGURATION, this.store, cacheLoaderWriter, cacheEventDispatcher, LoggerFactory.getLogger(EhcacheWithLoaderWriter.class + "-" + "EhcacheWithLoaderWriterBasicIteratorTest"));
    ehcache.init();
    assertThat("cache not initialized", ehcache.getStatus(), Matchers.is(Status.AVAILABLE));
    this.spiedResilienceStrategy = this.setResilienceStrategySpy(ehcache);
    return ehcache;
  }
}
