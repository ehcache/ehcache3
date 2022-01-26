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
import org.hamcrest.Matchers;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
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
public class EhcacheBasicPutIfAbsentTest extends EhcacheBasicCrudBase {

  @Test
  public void testPutIfAbsentNullNull() {
    final Ehcache<String, String> ehcache = this.getEhcache();

    try {
      ehcache.putIfAbsent(null, null);
      fail();
    } catch (NullPointerException e) {
      // expected
    }
  }

  @Test
  public void testPutIfAbsentKeyNull() {
    final Ehcache<String, String> ehcache = this.getEhcache();

    try {
      ehcache.putIfAbsent("key", null);
      fail();
    } catch (NullPointerException e) {
      // expected
    }
  }

  @Test
  public void testPutIfAbsentNullValue() {
    final Ehcache<String, String> ehcache = this.getEhcache();

    try {
      ehcache.putIfAbsent(null, "value");
      fail();
    } catch (NullPointerException e) {
      // expected
    }
  }

  /**
   * Tests the effect of a {@link Ehcache#putIfAbsent(Object, Object)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   * </ul>
   */
  @Test
  public void testPutIfAbsentNoStoreEntry() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.<String, String>emptyMap());
    this.store = spy(fakeStore);

    final Ehcache<String, String> ehcache = this.getEhcache();

    assertThat(ehcache.putIfAbsent("key", "value"), is(nullValue()));
    verify(this.store).putIfAbsent(eq("key"), eq("value"), any());
    verifyZeroInteractions(this.resilienceStrategy);
    assertThat(fakeStore.getEntryMap().get("key"), equalTo("value"));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.PutIfAbsentOutcome.PUT));
  }

  /**
   * Tests the effect of a {@link Ehcache#putIfAbsent(Object, Object)} for
   * <ul>
   *   <li>key present in {@code Store}</li>
   * </ul>
   */
  @Test
  public void testPutIfAbsentHasStoreEntry() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "oldValue"));
    this.store = spy(fakeStore);

    final Ehcache<String, String> ehcache = this.getEhcache();

    assertThat(ehcache.putIfAbsent("key", "value"), is(equalTo("oldValue")));
    verify(this.store).putIfAbsent(eq("key"), eq("value"), any());
    verifyZeroInteractions(this.resilienceStrategy);
    assertThat(fakeStore.getEntryMap().get("key"), equalTo("oldValue"));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.PutIfAbsentOutcome.HIT));
  }

  /**
   * Tests the effect of a {@link Ehcache#putIfAbsent(Object, Object)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   *   <li>{@code Store.putIfAbsent} throws</li>
   * </ul>
   */
  @Test
  public void testPutIfAbsentNoStoreEntryStoreAccessException() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.<String, String>emptyMap());
    this.store = spy(fakeStore);
    doThrow(new StoreAccessException("")).when(this.store).putIfAbsent(eq("key"), eq("value"), any());

    final Ehcache<String, String> ehcache = this.getEhcache();

    ehcache.putIfAbsent("key", "value");
    verify(this.store).putIfAbsent(eq("key"), eq("value"), any());
    verify(this.resilienceStrategy).putIfAbsentFailure(eq("key"), eq("value"), any(StoreAccessException.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.PutIfAbsentOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link Ehcache#putIfAbsent(Object, Object)} for
   * <ul>
   *   <li>key present in {@code Store}</li>
   *   <li>{@code Store.putIfAbsent} throws</li>
   * </ul>
   */
  @Test
  public void testPutIfAbsentHasStoreEntryStoreAccessException() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "oldValue"));
    this.store = spy(fakeStore);
    doThrow(new StoreAccessException("")).when(this.store).putIfAbsent(eq("key"), eq("value"), any());

    final Ehcache<String, String> ehcache = this.getEhcache();

    ehcache.putIfAbsent("key", "value");
    verify(this.store).putIfAbsent(eq("key"), eq("value"), any());
    verify(this.resilienceStrategy).putIfAbsentFailure(eq("key"), eq("value"), any(StoreAccessException.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.PutIfAbsentOutcome.FAILURE));
  }

  /**
   * Gets an initialized {@link Ehcache Ehcache} instance
   *
   * @return a new {@code Ehcache} instance
   */
  private Ehcache<String, String> getEhcache() {
    final Ehcache<String, String> ehcache = new Ehcache<>(CACHE_CONFIGURATION, this.store, resilienceStrategy, cacheEventDispatcher, LoggerFactory.getLogger(Ehcache.class + "-" + "EhcacheBasicPutIfAbsentTest"));
    ehcache.init();
    assertThat("cache not initialized", ehcache.getStatus(), Matchers.is(Status.AVAILABLE));
    return ehcache;
  }
}
