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
import java.util.EnumSet;

import org.ehcache.Status;
import org.ehcache.core.statistics.CacheOperationOutcomes;
import org.ehcache.core.spi.store.StoreAccessException;
import org.hamcrest.CoreMatchers;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verifyZeroInteractions;

/**
 * @author Abhilash
 *
 */
public class EhcacheBasicRemoveTest extends EhcacheBasicCrudBase {

  @Test
  public void testRemoveNull() {
    final Ehcache<String, String> ehcache = this.getEhcache();

    try {
      ehcache.remove(null);
      fail();
    } catch (NullPointerException e) {
      // expected
    }
  }

  /**
   * Tests the effect of a {@link Ehcache#remove(Object)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   * </ul>
   */
  @Test
  public void testRemoveNoStoreEntry() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.<String, String>emptyMap());
    this.store = spy(fakeStore);

    final Ehcache<String, String> ehcache = this.getEhcache();

    ehcache.remove("key");
    verify(this.store).remove(eq("key"));
    verifyZeroInteractions(this.spiedResilienceStrategy);
    assertThat(fakeStore.getEntryMap().containsKey("key"), is(false));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.RemoveOutcome.NOOP));
  }

  /**
   * Tests the effect of a {@link Ehcache#remove(Object)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   *   <li>{@code Store.remove} throws</li>
   * </ul>
   */
  @Test
  public void testRemoveNoStoreEntryStoreAccessException() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.<String, String>emptyMap());
    this.store = spy(fakeStore);
    doThrow(new StoreAccessException("")).when(this.store).remove(eq("key"));

    final Ehcache<String, String> ehcache = this.getEhcache();

    ehcache.remove("key");
    verify(this.store, times(2)).remove(eq("key"));
    verify(this.spiedResilienceStrategy).removeFailure(eq("key"), any(StoreAccessException.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.RemoveOutcome.FAILURE));
  }


  /**
   * Tests the effect of a {@link Ehcache#remove(Object)} for
   * <ul>
   *   <li>key present in {@code Store}</li>
   * </ul>
   */
  @Test
  public void testRemoveHasStoreEntry() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "oldValue"));
    this.store = spy(fakeStore);

    final Ehcache<String, String> ehcache = this.getEhcache();

    ehcache.remove("key");
    verify(this.store).remove(eq("key"));
    verifyZeroInteractions(this.spiedResilienceStrategy);
    assertThat(fakeStore.getEntryMap().containsKey("key"), is(false));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.RemoveOutcome.SUCCESS));
  }

  /**
   * Tests the effect of a {@link Ehcache#remove(Object)} for
   * <ul>
   *   <li>key present in {@code Store}</li>
   *   <li>{@code Store.remove} throws</li>
   * </ul>
   */
  @Test
  public void testRemoveHasStoreEntryStoreAccessException() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "oldValue"));
    this.store = spy(fakeStore);
    doThrow(new StoreAccessException("")).when(this.store).remove(eq("key"));

    final Ehcache<String, String> ehcache = this.getEhcache();

    ehcache.remove("key");
    verify(this.store, times(2)).remove(eq("key"));
    verify(this.spiedResilienceStrategy).removeFailure(eq("key"), any(StoreAccessException.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.RemoveOutcome.FAILURE));
  }

  /**
   * Gets an initialized {@link Ehcache Ehcache} instance
   *
   * @return a new {@code Ehcache} instance
   */
  private Ehcache<String, String> getEhcache() {
    final Ehcache<String, String> ehcache = new Ehcache<String, String>(CACHE_CONFIGURATION, this.store, cacheEventDispatcher, LoggerFactory.getLogger(Ehcache.class + "-" + "EhcacheBasicRemoveTest"));
    ehcache.init();
    assertThat("cache not initialized", ehcache.getStatus(), CoreMatchers.is(Status.AVAILABLE));
    this.spiedResilienceStrategy = this.setResilienceStrategySpy(ehcache);
    return ehcache;
  }
}
