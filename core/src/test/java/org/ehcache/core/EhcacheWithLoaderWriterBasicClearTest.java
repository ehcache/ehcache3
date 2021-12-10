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
import org.ehcache.spi.resilience.StoreAccessException;
import org.hamcrest.Matchers;
import org.junit.Test;
import org.mockito.Mock;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.ehcache.spi.loaderwriter.CacheLoaderWriter;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

/**
 * Provides testing of basic CLEAR operations on an {@code EhcacheWithLoaderWriter}.
 *
 * @author Clifford W. Johnson
 */
public class EhcacheWithLoaderWriterBasicClearTest extends EhcacheBasicCrudBase {

  @Mock
  private CacheLoaderWriter<String, String> cacheLoaderWriter;

  /**
   * Tests {@link EhcacheWithLoaderWriter#clear()} over an empty cache.
   */
  @Test
  public void testClearEmpty() throws Exception {
    final FakeStore realStore = new FakeStore(Collections.<String, String>emptyMap());
    this.store = spy(realStore);
    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache();

    ehcache.clear();
    verifyZeroInteractions(this.cacheLoaderWriter);
    verifyZeroInteractions(this.resilienceStrategy);
    assertThat(realStore.getEntryMap().isEmpty(), is(true));
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#clear()} over an empty cache where
   * {@link Store#clear() Store.clear} throws a
   * {@link StoreAccessException StoreAccessException}.
   */
  @Test
  public void testClearEmptyStoreAccessException() throws Exception {
    final FakeStore realStore = new FakeStore(Collections.<String, String>emptyMap());
    this.store = spy(realStore);
    doThrow(new StoreAccessException("")).when(this.store).clear();
    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache();

    ehcache.clear();
    verifyZeroInteractions(this.cacheLoaderWriter);
    verify(this.resilienceStrategy).clearFailure(any(StoreAccessException.class));
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#clear()} over a non-empty cache.
   */
  @Test
  public void testClearNonEmpty() throws Exception {
    final FakeStore realStore = new FakeStore(this.getTestStoreEntries());
    this.store = spy(realStore);
    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache();
    assertThat(realStore.getEntryMap().isEmpty(), is(false));

    ehcache.clear();
    verifyZeroInteractions(this.cacheLoaderWriter);
    verifyZeroInteractions(this.resilienceStrategy);
    assertThat(realStore.getEntryMap().isEmpty(), is(true));
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#clear()} over a non-empty cache where
   * {@link Store#clear() Store.clear} throws a
   * {@link StoreAccessException StoreAccessException}.
   */
  @Test
  public void testClearNonEmptyStoreAccessException() throws Exception {
    final FakeStore realStore = new FakeStore(this.getTestStoreEntries());
    this.store = spy(realStore);
    doThrow(new StoreAccessException("")).when(this.store).clear();
    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache();
    assertThat(realStore.getEntryMap().isEmpty(), is(false));

    ehcache.clear();
    verifyZeroInteractions(this.cacheLoaderWriter);
    verify(this.resilienceStrategy).clearFailure(any(StoreAccessException.class));
    // Not testing ResilienceStrategy implementation here
  }

  private Map<String, String> getTestStoreEntries() {
    final Map<String, String> storeEntries = new HashMap<>();
    storeEntries.put("key1", "value1");
    storeEntries.put("keyA", "valueA");
    storeEntries.put("key2", "value2");
    storeEntries.put("keyB", "valueB");
    return storeEntries;
  }

  /**
   * Gets an initialized {@link EhcacheWithLoaderWriter Ehcache} instance using {@link #cacheLoaderWriter}.
   *
   * @return a new {@code EhcacheWithLoaderWriter} instance
   */
  private EhcacheWithLoaderWriter<String, String> getEhcache()
      throws Exception {
    final EhcacheWithLoaderWriter<String, String> ehcache =
      new EhcacheWithLoaderWriter<>(CACHE_CONFIGURATION, this.store, resilienceStrategy, this.cacheLoaderWriter, cacheEventDispatcher, LoggerFactory
        .getLogger(EhcacheWithLoaderWriter.class + "-" + "EhcacheWithLoaderWriterBasicClearTest"));
    ehcache.init();
    assertThat("cache not initialized", ehcache.getStatus(), Matchers.is(Status.AVAILABLE));
    return ehcache;
  }
}
