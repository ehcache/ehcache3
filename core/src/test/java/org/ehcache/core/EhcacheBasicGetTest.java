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
import org.ehcache.spi.resilience.StoreAccessException;
import org.hamcrest.CoreMatchers;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

/**
 * @author Abhilash
 *
 */
public class EhcacheBasicGetTest extends EhcacheBasicCrudBase {

  @Test
  public void testGetNull() {
    final Ehcache<String, String> ehcache = this.getEhcache();

    try {
      ehcache.get(null);
      fail();
    } catch (NullPointerException e) {
      // expected
    }
  }

  /**
   * Tests the effect of a {@link Ehcache#get(Object)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   * </ul>
   */
  @Test
  public void testGetNoStoreEntry() throws Exception {
    final Ehcache<String, String> ehcache = this.getEhcache();

    assertThat(ehcache.get("key"), is(nullValue()));
    verify(this.store).get(eq("key"));
    verifyZeroInteractions(this.resilienceStrategy);
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetOutcome.MISS));
  }

  /**
   * Tests the effect of a {@link Ehcache#get(Object)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   *   <li>{@code Store.get} throws</li>
   * </ul>
   */
  @Test
  public void testGetNoStoreEntryStoreAccessException() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.<String, String>emptyMap());
    this.store = spy(fakeStore);
    doThrow(new StoreAccessException("")).when(this.store).get(eq("key"));

    final Ehcache<String, String> ehcache = this.getEhcache();

    ehcache.get("key");
    verify(this.store).get(eq("key"));
    verify(this.resilienceStrategy).getFailure(eq("key"), any(StoreAccessException.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link Ehcache#get(Object)} for
   * <ul>
   *   <li>key present in {@code Store}</li>
   * </ul>
   */
  @Test
  public void testGetHasStoreEntry() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "value"));
    this.store = spy(fakeStore);
    assertThat(fakeStore.getEntryMap().get("key"), equalTo("value"));

    final Ehcache<String, String> ehcache = this.getEhcache();

    assertThat(ehcache.get("key"), equalTo("value"));
    verify(this.store).get(eq("key"));
    verifyZeroInteractions(this.resilienceStrategy);
    assertThat(fakeStore.getEntryMap().get("key"), equalTo("value"));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetOutcome.HIT));
  }

  /**
   * Tests the effect of a {@link Ehcache#get(Object)} for
   * <ul>
   *   <li>key present in {@code Store}</li>
   *   <li>{@code Store.get} throws</li>
   * </ul>
   */
  @Test
  public void testGetHasStoreEntryStoreAccessExceptionNoCacheLoaderWriter() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "value"));
    this.store = spy(fakeStore);
    assertThat(fakeStore.getEntryMap().get("key"), equalTo("value"));
    doThrow(new StoreAccessException("")).when(this.store).get(eq("key"));

    final Ehcache<String, String> ehcache = this.getEhcache();

    ehcache.get("key");
    verify(this.store).get(eq("key"));
    verify(this.resilienceStrategy).getFailure(eq("key"), any(StoreAccessException.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetOutcome.FAILURE));
  }

  /**
   * Gets an initialized {@link Ehcache Ehcache} instance
   *
   * @return a new {@code Ehcache} instance
   */
  private Ehcache<String, String> getEhcache() {
    final Ehcache<String, String> ehcache = new Ehcache<>(CACHE_CONFIGURATION, this.store, resilienceStrategy, cacheEventDispatcher, LoggerFactory
      .getLogger(Ehcache.class + "-" + "EhcacheBasicGetTest"));
    ehcache.init();
    assertThat("cache not initialized", ehcache.getStatus(), CoreMatchers.is(Status.AVAILABLE));
    return ehcache;
  }
}
