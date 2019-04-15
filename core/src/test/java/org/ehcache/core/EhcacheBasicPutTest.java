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
import org.ehcache.config.CacheConfiguration;
import org.ehcache.core.statistics.CacheOperationOutcomes;
import org.ehcache.spi.resilience.StoreAccessException;
import org.hamcrest.CoreMatchers;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;

/**
 * @author Abhilash
 *
 */
public class EhcacheBasicPutTest extends EhcacheBasicCrudBase {

  @Test
  public void testPutNullNull() {
    final Ehcache<String, String> ehcache = this.getEhcache();

    try {
      ehcache.put(null, null);
      fail();
    } catch (NullPointerException e) {
      // expected
    }
  }

  @Test
  public void testPutKeyNull() {
    final Ehcache<String, String> ehcache = this.getEhcache();

    try {
      ehcache.put("key", null);
      fail();
    } catch (NullPointerException e) {
      // expected
    }
  }

  @Test
  public void testPutNullValue() {
    final Ehcache<String, String> ehcache = this.getEhcache();

    try {
      ehcache.put(null, "value");
      fail();
    } catch (NullPointerException e) {
      // expected
    }
  }

  /**
   * Tests the effect of a {@link Ehcache#put(Object, Object)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   * </ul>
   */
  @Test
  public void testPutNoStoreEntry() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.<String, String>emptyMap());
    this.store = spy(fakeStore);

    final Ehcache<String, String> ehcache = this.getEhcache();

    ehcache.put("key", "value");
    verify(this.store).put(eq("key"), eq("value"));
    verifyZeroInteractions(this.resilienceStrategy);
    assertThat(fakeStore.getEntryMap().get("key"), equalTo("value"));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.PutOutcome.PUT));
  }

  /**
   * Tests the effect of a {@link Ehcache#put(Object, Object)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   *   <li>{@code Store.put} throws</li>
   * </ul>
   */
  @Test
  public void testPutNoStoreEntryStoreAccessException() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.<String, String>emptyMap());
    this.store = spy(fakeStore);
    doThrow(new StoreAccessException("")).when(this.store).put(eq("key"), eq("value"));

    final Ehcache<String, String> ehcache = this.getEhcache();

    ehcache.put("key", "value");
    verify(this.store).put(eq("key"), eq("value"));
    verify(this.resilienceStrategy).putFailure(eq("key"), eq("value"), any(StoreAccessException.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.PutOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link Ehcache#put(Object, Object)} for
   * <ul>
   *   <li>key present in {@code Store}</li>
   * </ul>
   */
  @Test
  public void testPutHasStoreEntry() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "oldValue"));
    this.store = spy(fakeStore);

    final Ehcache<String, String> ehcache = this.getEhcache();

    ehcache.put("key", "value");
    verify(this.store).put(eq("key"), eq("value"));
    verifyZeroInteractions(this.resilienceStrategy);
    assertThat(fakeStore.getEntryMap().get("key"), equalTo("value"));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.PutOutcome.PUT));
  }

  /**
   * Tests the effect of a {@link Ehcache#put(Object, Object)} for
   * <ul>
   *   <li>key present in {@code Store}</li>
   *   <li>{@code Store.put} throws</li>
   * </ul>
   */
  @Test
  public void testPutHasStoreEntryStoreAccessException() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "oldValue"));
    this.store = spy(fakeStore);
    doThrow(new StoreAccessException("")).when(this.store).put(eq("key"), eq("value"));

    final Ehcache<String, String> ehcache = this.getEhcache();

    ehcache.put("key", "value");
    verify(this.store).put(eq("key"), eq("value"));
    verify(this.resilienceStrategy).putFailure(eq("key"), eq("value"), any(StoreAccessException.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.PutOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link Ehcache#put(Object, Object)} for
   * <ul>
   *   <li>key present in {@code Store}</li>
   *   <li>{@code Store.put} throws a {@code RuntimeException}</li>
   * </ul>
   */
  @Test
  public void testPutThrowsExceptionShouldKeepTheValueInPlace() throws Exception {
    FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "oldValue"));
    this.store = spy(fakeStore);
    doThrow(new RuntimeException("failed")).when(this.store).put(eq("key"), eq("value"));

    Ehcache<String, String> ehcache = this.getEhcache();

    try {
      ehcache.put("key", "value");
      fail();
    } catch(RuntimeException e) {
      // expected
      assertThat(e.getMessage(), equalTo("failed"));
    }

    // Key and old value should still be in place
    assertThat(ehcache.get("key"), equalTo("oldValue"));

    verify(this.store).put(eq("key"), eq("value"));
    verifyNoMoreInteractions(this.resilienceStrategy);
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.PutOutcome.FAILURE));
  }

  /**
   * Gets an initialized {@link Ehcache Ehcache} instance
   *
   * @return a new {@code Ehcache} instance
   */
  private Ehcache<String, String> getEhcache() {
    return getEhcache(CACHE_CONFIGURATION);
  }

  @SuppressWarnings("unchecked")
  private Ehcache<String, String> getEhcache(CacheConfiguration<String, String> config) {
    final Ehcache<String, String> ehcache = new Ehcache<>(config, this.store, resilienceStrategy, cacheEventDispatcher, LoggerFactory.getLogger(Ehcache.class + "-" + "EhcacheBasicPutTest"));
    ehcache.init();
    assertThat("cache not initialized", ehcache.getStatus(), CoreMatchers.is(Status.AVAILABLE));
    return ehcache;
  }
}
