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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

/**
 * Provides testing of basic CONTAINS_KEY operations on an {@code EhcacheWithLoaderWriter}.
 *
 * @author Clifford W. Johnson
 */
public class EhcacheWithLoaderWriterBasicContainsKeyTest extends EhcacheBasicCrudBase {

  @Mock
  private CacheLoaderWriter<String, String> cacheLoaderWriter;

  /**
   * Tests {@link EhcacheWithLoaderWriter#containsKey(Object) EhcacheWithLoaderWriter.containsKey} with a {@code null} key.
   */
  @Test
  public void testContainsKeyNull() throws Exception {
    final FakeStore realStore = new FakeStore(Collections.<String, String>emptyMap());
    this.store = spy(realStore);
    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache();

    try {
      ehcache.containsKey(null);
      fail();
    } catch (NullPointerException e) {
      // Expected
    }
    verifyZeroInteractions(this.cacheLoaderWriter);
    verifyZeroInteractions(this.resilienceStrategy);
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#containsKey(Object) EhcacheWithLoaderWriter.containsKey} over an empty cache.
   */
  @Test
  public void testContainsKeyEmpty() throws Exception {
    final FakeStore realStore = new FakeStore(Collections.<String, String>emptyMap());
    this.store = spy(realStore);
    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache();

    assertFalse(ehcache.containsKey("key"));
    verifyZeroInteractions(this.cacheLoaderWriter);
    verifyZeroInteractions(this.resilienceStrategy);
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#containsKey(Object) EhcacheWithLoaderWriter.containsKey} over an empty cache
   * where {@link Store#containsKey(Object) Store.containsKey} throws a
   * {@link StoreAccessException StoreAccessException}.
   */
  @Test
  public void testContainsKeyEmptyStoreAccessException() throws Exception {
    final FakeStore realStore = new FakeStore(Collections.<String, String>emptyMap());
    this.store = spy(realStore);
    doThrow(new StoreAccessException("")).when(this.store).containsKey("key");
    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache();

    ehcache.containsKey("key");
    verifyZeroInteractions(this.cacheLoaderWriter);
    verify(this.resilienceStrategy).containsKeyFailure(eq("key"), any(StoreAccessException.class));
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#containsKey(Object) EhcacheWithLoaderWriter.containsKey} over a cache holding
   * the target key.
   */
  @Test
  public void testContainsKeyContains() throws Exception {
    final FakeStore realStore = new FakeStore(this.getTestStoreEntries());
    this.store = spy(realStore);
    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache();

    assertTrue(ehcache.containsKey("keyA"));
    verifyZeroInteractions(this.cacheLoaderWriter);
    verifyZeroInteractions(this.resilienceStrategy);
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#containsKey(Object) EhcacheWithLoaderWriter.containsKey} over a cache holding
   * the target key where {@link Store#containsKey(Object) Store.containsKey}
   * throws a {@link StoreAccessException StoreAccessException}.
   */
  @Test
  public void testContainsKeyContainsStoreAccessException() throws Exception {
    final FakeStore realStore = new FakeStore(this.getTestStoreEntries());
    this.store = spy(realStore);
    doThrow(new StoreAccessException("")).when(this.store).containsKey("keyA");
    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache();

    ehcache.containsKey("keyA");
    verifyZeroInteractions(this.cacheLoaderWriter);
    verify(this.resilienceStrategy).containsKeyFailure(eq("keyA"), any(StoreAccessException.class));
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#containsKey(Object) EhcacheWithLoaderWriter.containsKey} over a non-empty cache
   * not holding the target key.
   */
  @Test
  public void testContainsKeyMissing() throws Exception {
    final FakeStore realStore = new FakeStore(this.getTestStoreEntries());
    this.store = spy(realStore);
    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache();

    assertFalse(ehcache.containsKey("missingKey"));
    verifyZeroInteractions(this.cacheLoaderWriter);
    verifyZeroInteractions(this.resilienceStrategy);
  }

  /**
   * Tests {@link EhcacheWithLoaderWriter#containsKey(Object) EhcacheWithLoaderWriter.containsKey} over a non-empty cache
   * not holding the target key where {@link Store#containsKey(Object) Store.containsKey}
   * throws a {@link StoreAccessException StoreAccessException}.
   */
  @Test
  public void testContainsKeyMissingStoreAccessException() throws Exception {
    final FakeStore realStore = new FakeStore(this.getTestStoreEntries());
    this.store = spy(realStore);
    doThrow(new StoreAccessException("")).when(this.store).containsKey("missingKey");
    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache();

    ehcache.containsKey("missingKey");
    verifyZeroInteractions(this.cacheLoaderWriter);
    verify(this.resilienceStrategy).containsKeyFailure(eq("missingKey"), any(StoreAccessException.class));
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
        .getLogger(EhcacheWithLoaderWriter.class + "-" + "EhcacheWithLoaderWriterBasicContainsKeyTest"));
    ehcache.init();
    assertThat("cache not initialized", ehcache.getStatus(), Matchers.is(Status.AVAILABLE));
    return ehcache;
  }
}
