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
import org.ehcache.spi.loader.CacheLoader;
import org.ehcache.spi.writer.CacheWriter;
import org.junit.Test;
import org.mockito.Mock;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

/**
 * Provides testing of basic CLEAR operations on an {@code Ehcache}.
 *
 * @author Clifford W. Johnson
 */
public class EhcacheBasicClearTest extends EhcacheBasicCrudBase {

  @Mock
  private CacheLoader<String, String> cacheLoader;

  @Mock
  private CacheWriter<String, String> cacheWriter;

  /**
   * Tests {@link Ehcache#clear()} over an empty cache.
   */
  @Test
  public void testClearEmpty() throws Exception {
    final MockStore realStore = new MockStore(Collections.<String, String>emptyMap());
    this.store = spy(realStore);
    final Ehcache<String, String> ehcache = this.getEhcache();

    ehcache.clear();
    verifyZeroInteractions(this.cacheLoader);
    verifyZeroInteractions(this.cacheWriter);
    verifyZeroInteractions(this.spiedResilienceStrategy);
    assertThat(realStore.getMap().isEmpty(), is(true));
  }

  /**
   * Tests {@link Ehcache#clear()} over an empty cache where
   * {@link org.ehcache.spi.cache.Store#clear() Store.clear} throws a
   * {@link org.ehcache.exceptions.CacheAccessException CacheAccessException}.
   */
  @Test
  public void testClearEmptyCacheAccessException() throws Exception {
    final MockStore realStore = new MockStore(Collections.<String, String>emptyMap());
    this.store = spy(realStore);
    doThrow(new CacheAccessException("")).when(this.store).clear();
    final Ehcache<String, String> ehcache = this.getEhcache();

    ehcache.clear();
    verifyZeroInteractions(this.cacheLoader);
    verifyZeroInteractions(this.cacheWriter);
    verify(this.spiedResilienceStrategy).clearFailure(any(CacheAccessException.class));
  }

  /**
   * Tests {@link Ehcache#clear()} over a non-empty cache.
   */
  @Test
  public void testClearNonEmpty() throws Exception {
    final MockStore realStore = new MockStore(this.getTestStoreEntries());
    this.store = spy(realStore);
    final Ehcache<String, String> ehcache = this.getEhcache();
    assertThat(realStore.getMap().isEmpty(), is(false));

    ehcache.clear();
    verifyZeroInteractions(this.cacheLoader);
    verifyZeroInteractions(this.cacheWriter);
    verifyZeroInteractions(this.spiedResilienceStrategy);
    assertThat(realStore.getMap().isEmpty(), is(true));
  }

  /**
   * Tests {@link Ehcache#clear()} over a non-empty cache where
   * {@link org.ehcache.spi.cache.Store#clear() Store.clear} throws a
   * {@link org.ehcache.exceptions.CacheAccessException CacheAccessException}.
   */
  @Test
  public void testClearNonEmptyCacheAccessException() throws Exception {
    final MockStore realStore = new MockStore(this.getTestStoreEntries());
    this.store = spy(realStore);
    doThrow(new CacheAccessException("")).when(this.store).clear();
    final Ehcache<String, String> ehcache = this.getEhcache();
    assertThat(realStore.getMap().isEmpty(), is(false));

    ehcache.clear();
    verifyZeroInteractions(this.cacheLoader);
    verifyZeroInteractions(this.cacheWriter);
    verify(this.spiedResilienceStrategy).clearFailure(any(CacheAccessException.class));
    // Not testing ResilienceStrategy implementation here
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
   * Gets an initialized {@link Ehcache Ehcache} instance using {@link #cacheLoader}
   * and {@link #cacheWriter}.
   *
   * @return a new {@code Ehcache} instance
   */
  private Ehcache<String, String> getEhcache()
      throws Exception {
    final Ehcache<String, String> ehcache =
        new Ehcache<String, String>(CACHE_CONFIGURATION, this.store, this.cacheLoader, this.cacheWriter);
    ehcache.init();
    assertThat("cache not initialized", ehcache.getStatus(), is(Status.AVAILABLE));
    this.spiedResilienceStrategy = this.setResilienceStrategySpy(ehcache);
    return ehcache;
  }
}
