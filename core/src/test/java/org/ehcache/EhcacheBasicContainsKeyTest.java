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
import org.junit.Test;
import org.mockito.Mock;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.ehcache.spi.loaderwriter.CacheLoaderWriter;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

/**
 * Provides testing of basic CONTAINS_KEY operations on an {@code Ehcache}.
 *
 * @author Clifford W. Johnson
 */
public class EhcacheBasicContainsKeyTest extends EhcacheBasicCrudBase {

  @Mock
  private CacheLoaderWriter<String, String> cacheLoaderWriter;

  /**
   * Tests {@link org.ehcache.Ehcache#containsKey(Object) Ehcache.containsKey} with a {@code null} key.
   */
  @Test
  public void testContainsKeyNull() throws Exception {
    final FakeStore realStore = new FakeStore(Collections.<String, String>emptyMap());
    this.store = spy(realStore);
    final Ehcache<String, String> ehcache = this.getEhcache();

    try {
      ehcache.containsKey(null);
      fail();
    } catch (NullPointerException e) {
      // Expected
    }
    verifyZeroInteractions(this.cacheLoaderWriter);
    verifyZeroInteractions(this.spiedResilienceStrategy);
  }

  /**
   * Tests {@link org.ehcache.Ehcache#containsKey(Object) Ehcache.containsKey} over an empty cache.
   */
  @Test
  public void testContainsKeyEmpty() throws Exception {
    final FakeStore realStore = new FakeStore(Collections.<String, String>emptyMap());
    this.store = spy(realStore);
    final Ehcache<String, String> ehcache = this.getEhcache();

    assertFalse(ehcache.containsKey("key"));
    verifyZeroInteractions(this.cacheLoaderWriter);
    verifyZeroInteractions(this.spiedResilienceStrategy);
  }

  /**
   * Tests {@link org.ehcache.Ehcache#containsKey(Object) Ehcache.containsKey} over an empty cache
   * where {@link org.ehcache.spi.cache.Store#containsKey(Object) Store.containsKey} throws a
   * {@link org.ehcache.exceptions.CacheAccessException CacheAccessException}.
   */
  @Test
  public void testContainsKeyEmptyCacheAccessException() throws Exception {
    final FakeStore realStore = new FakeStore(Collections.<String, String>emptyMap());
    this.store = spy(realStore);
    doThrow(new CacheAccessException("")).when(this.store).containsKey("key");
    final Ehcache<String, String> ehcache = this.getEhcache();

    ehcache.containsKey("key");
    verifyZeroInteractions(this.cacheLoaderWriter);
    verify(this.spiedResilienceStrategy).containsKeyFailure(eq("key"), any(CacheAccessException.class));
  }

  /**
   * Tests {@link org.ehcache.Ehcache#containsKey(Object) Ehcache.containsKey} over a cache holding
   * the target key.
   */
  @Test
  public void testContainsKeyContains() throws Exception {
    final FakeStore realStore = new FakeStore(this.getTestStoreEntries());
    this.store = spy(realStore);
    final Ehcache<String, String> ehcache = this.getEhcache();

    assertTrue(ehcache.containsKey("keyA"));
    verifyZeroInteractions(this.cacheLoaderWriter);
    verifyZeroInteractions(this.spiedResilienceStrategy);
  }

  /**
   * Tests {@link org.ehcache.Ehcache#containsKey(Object) Ehcache.containsKey} over a cache holding
   * the target key where {@link org.ehcache.spi.cache.Store#containsKey(Object) Store.containsKey}
   * throws a {@link org.ehcache.exceptions.CacheAccessException CacheAccessException}.
   */
  @Test
  public void testContainsKeyContainsCacheAccessException() throws Exception {
    final FakeStore realStore = new FakeStore(this.getTestStoreEntries());
    this.store = spy(realStore);
    doThrow(new CacheAccessException("")).when(this.store).containsKey("keyA");
    final Ehcache<String, String> ehcache = this.getEhcache();

    ehcache.containsKey("keyA");
    verifyZeroInteractions(this.cacheLoaderWriter);
    verify(this.spiedResilienceStrategy).containsKeyFailure(eq("keyA"), any(CacheAccessException.class));
  }

  /**
   * Tests {@link org.ehcache.Ehcache#containsKey(Object) Ehcache.containsKey} over a non-empty cache
   * not holding the target key.
   */
  @Test
  public void testContainsKeyMissing() throws Exception {
    final FakeStore realStore = new FakeStore(this.getTestStoreEntries());
    this.store = spy(realStore);
    final Ehcache<String, String> ehcache = this.getEhcache();

    assertFalse(ehcache.containsKey("missingKey"));
    verifyZeroInteractions(this.cacheLoaderWriter);
    verifyZeroInteractions(this.spiedResilienceStrategy);
  }

  /**
   * Tests {@link org.ehcache.Ehcache#containsKey(Object) Ehcache.containsKey} over a non-empty cache
   * not holding the target key where {@link org.ehcache.spi.cache.Store#containsKey(Object) Store.containsKey}
   * throws a {@link org.ehcache.exceptions.CacheAccessException CacheAccessException}.
   */
  @Test
  public void testContainsKeyMissingCacheAccessException() throws Exception {
    final FakeStore realStore = new FakeStore(this.getTestStoreEntries());
    this.store = spy(realStore);
    doThrow(new CacheAccessException("")).when(this.store).containsKey("missingKey");
    final Ehcache<String, String> ehcache = this.getEhcache();

    ehcache.containsKey("missingKey");
    verifyZeroInteractions(this.cacheLoaderWriter);
    verify(this.spiedResilienceStrategy).containsKeyFailure(eq("missingKey"), any(CacheAccessException.class));
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
   * Gets an initialized {@link Ehcache Ehcache} instance using {@link #cacheLoaderWriter}.
   *
   * @return a new {@code Ehcache} instance
   */
  private Ehcache<String, String> getEhcache()
      throws Exception {
    RuntimeConfiguration<String, String> runtimeConfiguration = new RuntimeConfiguration<String, String>(CACHE_CONFIGURATION, null);
    final Ehcache<String, String> ehcache =
        new Ehcache<String, String>(runtimeConfiguration, this.store, this.cacheLoaderWriter, LoggerFactory.getLogger(Ehcache.class + "-" + "EhcacheBasicContainsKeyTest"));
    ehcache.init();
    assertThat("cache not initialized", ehcache.getStatus(), is(Status.AVAILABLE));
    this.spiedResilienceStrategy = this.setResilienceStrategySpy(ehcache);
    return ehcache;
  }
}
