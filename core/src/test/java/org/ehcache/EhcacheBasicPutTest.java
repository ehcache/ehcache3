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

import org.ehcache.spi.writer.CacheWriter;
import org.junit.Test;
import org.mockito.Mock;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * Provides testing of basic PUT operations on an {@code Ehcache}.
 *
 * @author Clifford W. Johnson
 */
public class EhcacheBasicPutTest extends EhcacheBasicCrudBase {

  @Mock
  protected CacheWriter<String, String> cacheWriter;

  @Test
  public void testPutNullNull() {
    final Ehcache<String, String> ehcache = this.getEhcache(null);

    try {
      ehcache.put(null, null);
      fail();
    } catch (NullPointerException e) {
      // expected
    }
  }

  @Test
  public void testPutKeyNull() {
    final Ehcache<String, String> ehcache = this.getEhcache(null);

    try {
      ehcache.put("key", null);
      fail();
    } catch (NullPointerException e) {
      // expected
    }
  }

  @Test
  public void testPutNullValue() {
    final Ehcache<String, String> ehcache = this.getEhcache(null);

    try {
      ehcache.put(null, "value");
      fail();
    } catch (NullPointerException e) {
      // expected
    }
  }

  @Test
  public void testPutNoStoreEntryNoCacheWriter() throws Exception {
  }

  @Test
  public void testPutNoStoreEntryNoCacheWriterEntry() throws Exception {
  }

  @Test
  public void testPutNoStoreEntryHasCacheWriterEntry() throws Exception {
  }

  @Test
  public void testPutNoStoreEntryCacheWriterException() throws Exception {
  }

  @Test
  public void testPutNoStoreEntryCacheAccessExceptionNoCacheWriter() throws Exception {
  }

  @Test
  public void testPutNoStoreEntryCacheAccessExceptionNoCacheWriterEntry() throws Exception {
  }

  @Test
  public void testPutNoStoreEntryCacheAccessExceptionHasCacheWriterEntry() throws Exception {
  }

  @Test
  public void testPutNoStoreEntryCacheAccessExceptionCacheWriterException() throws Exception {
  }

  @Test
  public void testPutHasStoreEntryNoCacheWriter() throws Exception {
  }

  @Test
  public void testPutHasStoreEntryNoCacheWriterEntry() throws Exception {
  }

  @Test
  public void testPutHasStoreEntryHasCacheWriterEntry() throws Exception {
  }

  @Test
  public void testPutHasStoreEntryCacheWriterException() throws Exception {
  }

  @Test
  public void testPutHasStoreEntryCacheAccessExceptionNoCacheWriter() throws Exception {
  }

  @Test
  public void testPutHasStoreEntryCacheAccessExceptionNoCacheWriterEntry() throws Exception {
  }

  @Test
  public void testPutHasStoreEntryCacheAccessExceptionHasCacheWriterEntry() throws Exception {
  }

  @Test
  public void testPutHasStoreEntryCacheAccessExceptionCacheWriterException() throws Exception {
  }

  /**
   * Gets an initialized {@link Ehcache Ehcache} instance using the
   * {@link org.ehcache.spi.writer.CacheWriter CacheWriter} provided.
   *
   * @param cacheWriter
   *    the {@code CacheWriter} to use; may be {@code null}
   *
   * @return a new {@code Ehcache} instance
   */
  private Ehcache<String, String> getEhcache(final CacheWriter<String, String> cacheWriter) {
    final Ehcache<String, String> ehcache = new Ehcache<String, String>(CACHE_CONFIGURATION, this.store, null, cacheWriter);
    ehcache.init();
    assertThat("cache not initialized", ehcache.getStatus(), is(Status.AVAILABLE));
    return ehcache;
  }
}
