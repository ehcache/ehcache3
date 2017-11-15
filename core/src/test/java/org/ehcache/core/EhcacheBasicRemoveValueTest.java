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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
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
 * @author Abhilash
 *
 */
public class EhcacheBasicRemoveValueTest extends EhcacheBasicCrudBase {

  @Test
  public void testRemoveNullNull() {
    final Ehcache<String, String> ehcache = this.getEhcache();

    try {
      ehcache.remove(null, null);
      fail();
    } catch (NullPointerException e) {
      // expected
    }
  }

  @Test
  public void testRemoveKeyNull() throws Exception {
    final Ehcache<String, String> ehcache = this.getEhcache();

    try {
      ehcache.remove("key", null);
      fail();
    } catch (NullPointerException e) {
      // expected
    }
  }

  @Test
  public void testRemoveNullValue() throws Exception {
    final Ehcache<String, String> ehcache = this.getEhcache();

    try {
      ehcache.remove(null, "value");
      fail();
    } catch (NullPointerException e) {
      // expected
    }
  }

  /**
   * Tests the effect of a {@link Ehcache#remove(Object, Object)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   * </ul>
   */
  @Test
  public void testRemoveValueNoStoreEntry() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.<String, String>emptyMap());
    this.store = spy(fakeStore);

    final Ehcache<String, String> ehcache = this.getEhcache();

    assertFalse(ehcache.remove("key", "value"));
    verify(this.store).remove(eq("key"), eq("value"));
    verifyZeroInteractions(this.spiedResilienceStrategy);
    assertThat(fakeStore.getEntryMap().containsKey("key"), is(false));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ConditionalRemoveOutcome.FAILURE_KEY_MISSING));
  }

  /**
   * Tests the effect of a {@link Ehcache#remove(Object, Object)} for
   * <ul>
   *   <li>key with unequal value in {@code Store}</li>
   * </ul>
   */
  @Test
  public void testRemoveValueUnequalStoreEntry() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "unequalValue"));
    this.store = spy(fakeStore);

    final Ehcache<String, String> ehcache = this.getEhcache();

    assertFalse(ehcache.remove("key", "value"));
    verify(this.store).remove(eq("key"), eq("value"));
    verifyZeroInteractions(this.spiedResilienceStrategy);
    assertThat(fakeStore.getEntryMap().get("key"), is(equalTo("unequalValue")));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ConditionalRemoveOutcome.FAILURE_KEY_PRESENT));
  }

  /**
   * Tests the effect of a {@link Ehcache#remove(Object, Object)} for
   * <ul>
   *   <li>key with equal value in {@code Store}</li>
   * </ul>
   */
  @Test
  public void testRemoveValueEqualStoreEntry() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "value"));
    this.store = spy(fakeStore);

    final Ehcache<String, String> ehcache = this.getEhcache();

    assertTrue(ehcache.remove("key", "value"));
    verify(this.store).remove(eq("key"), eq("value"));
    verifyZeroInteractions(this.spiedResilienceStrategy);
    assertThat(fakeStore.getEntryMap().containsKey("key"), is(false));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ConditionalRemoveOutcome.SUCCESS));
  }

  /**
   * Tests the effect of a {@link Ehcache#remove(Object, Object)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   *   <li>>{@code Store.remove} throws</li>
   * </ul>
   */
  @Test
  public void testRemoveValueNoStoreEntryStoreAccessException() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.<String, String>emptyMap());
    this.store = spy(fakeStore);
    doThrow(new StoreAccessException("")).when(this.store).remove(eq("key"), eq("value"));

    final Ehcache<String, String> ehcache = this.getEhcache();

    ehcache.remove("key", "value");
    verify(this.store).remove(eq("key"), eq("value"));
    verify(this.spiedResilienceStrategy)
        .removeFailure(eq("key"), eq("value"), any(StoreAccessException.class), eq(false));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ConditionalRemoveOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link Ehcache#remove(Object, Object)} for
   * <ul>
   *   <li>key with unequal value present in {@code Store}</li>
   *   <li>>{@code Store.remove} throws</li>
   * </ul>
   */
  @Test
  public void testRemoveValueUnequalStoreEntryStoreAccessException() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "unequalValue"));
    this.store = spy(fakeStore);
    doThrow(new StoreAccessException("")).when(this.store).remove(eq("key"), eq("value"));

    final Ehcache<String, String> ehcache = this.getEhcache();

    ehcache.remove("key", "value");
    verify(this.store).remove(eq("key"), eq("value"));
    verify(this.spiedResilienceStrategy)
        .removeFailure(eq("key"), eq("value"), any(StoreAccessException.class), eq(false));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ConditionalRemoveOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link Ehcache#remove(Object, Object)} for
   * <ul>
   *   <li>key with equal value present in {@code Store}</li>
   *   <li>>{@code Store.remove} throws</li>
   * </ul>
   */
  @Test
  public void testRemoveValueEqualStoreEntryStoreAccessException() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "value"));
    this.store = spy(fakeStore);
    doThrow(new StoreAccessException("")).when(this.store).remove(eq("key"), eq("value"));

    final Ehcache<String, String> ehcache = this.getEhcache();

    ehcache.remove("key", "value");
    verify(this.store).remove(eq("key"), eq("value"));
    verify(this.spiedResilienceStrategy)
        .removeFailure(eq("key"), eq("value"), any(StoreAccessException.class), eq(false));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ConditionalRemoveOutcome.FAILURE));
  }

  /**
   * Gets an initialized {@link Ehcache Ehcache} instance
   *
   * @return a new {@code Ehcache} instance
   */
  private Ehcache<String, String> getEhcache() {
    final Ehcache<String, String> ehcache = new Ehcache<String, String>(CACHE_CONFIGURATION, this.store, cacheEventDispatcher, LoggerFactory.getLogger(Ehcache.class + "-" + "EhcacheBasicRemoveValueTest"));
    ehcache.init();
    assertThat("cache not initialized", ehcache.getStatus(), CoreMatchers.is(Status.AVAILABLE));
    this.spiedResilienceStrategy = this.setResilienceStrategySpy(ehcache);
    return ehcache;
  }
}
