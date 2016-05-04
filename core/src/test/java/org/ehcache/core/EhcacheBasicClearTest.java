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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.ehcache.Status;
import org.ehcache.core.spi.store.Store;
import org.ehcache.core.spi.store.StoreAccessException;
import org.hamcrest.Matchers;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

/**
 * @author Abhilash
 *
 */
public class EhcacheBasicClearTest extends EhcacheBasicCrudBase {

  /**
   * Tests {@link Ehcache#clear()} over an empty cache.
   */
  @Test
  public void testClearEmpty() throws Exception {
    final FakeStore realStore = new FakeStore(Collections.<String, String>emptyMap());
    this.store = spy(realStore);
    final Ehcache<String, String> ehcache = this.getEhcache();

    ehcache.clear();
    verifyZeroInteractions(this.spiedResilienceStrategy);
    assertThat(realStore.getEntryMap().isEmpty(), is(true));
  }

  /**
   * Tests {@link Ehcache#clear()} over an empty cache where
   * {@link Store#clear() Store.clear} throws a
   * {@link StoreAccessException StoreAccessException}.
   */
  @Test
  public void testClearEmptyStoreAccessException() throws Exception {
    final FakeStore realStore = new FakeStore(Collections.<String, String>emptyMap());
    this.store = spy(realStore);
    doThrow(new StoreAccessException("")).when(this.store).clear();
    final Ehcache<String, String> ehcache = this.getEhcache();

    ehcache.clear();
    verify(this.spiedResilienceStrategy).clearFailure(any(StoreAccessException.class));
  }

  /**
   * Tests {@link Ehcache#clear()} over a non-empty cache.
   */
  @Test
  public void testClearNonEmpty() throws Exception {
    final FakeStore realStore = new FakeStore(this.getTestStoreEntries());
    this.store = spy(realStore);
    final Ehcache<String, String> ehcache = this.getEhcache();
    assertThat(realStore.getEntryMap().isEmpty(), is(false));

    ehcache.clear();
    verifyZeroInteractions(this.spiedResilienceStrategy);
    assertThat(realStore.getEntryMap().isEmpty(), is(true));
  }

  /**
   * Tests {@link Ehcache#clear()} over a non-empty cache where
   * {@link Store#clear() Store.clear} throws a
   * {@link StoreAccessException StoreAccessException}.
   */
  @Test
  public void testClearNonEmptyStoreAccessException() throws Exception {
    final FakeStore realStore = new FakeStore(this.getTestStoreEntries());
    this.store = spy(realStore);
    doThrow(new StoreAccessException("")).when(this.store).clear();
    final Ehcache<String, String> ehcache = this.getEhcache();
    assertThat(realStore.getEntryMap().isEmpty(), is(false));

    ehcache.clear();
    verify(this.spiedResilienceStrategy).clearFailure(any(StoreAccessException.class));
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
   * Gets an initialized {@link Ehcache Ehcache}
   *
   * @return a new {@code Ehcache} instance
   */
  private Ehcache<String, String> getEhcache()
      throws Exception {
    final Ehcache<String, String> ehcache =
        new Ehcache<String, String>(CACHE_CONFIGURATION, this.store, cacheEventDispatcher, LoggerFactory.getLogger(Ehcache.class + "-" + "EhcacheBasicClearTest"));
    ehcache.init();
    assertThat("cache not initialized", ehcache.getStatus(), Matchers.is(Status.AVAILABLE));
    this.spiedResilienceStrategy = this.setResilienceStrategySpy(ehcache);
    return ehcache;
  }
}
