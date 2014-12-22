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
import org.ehcache.exceptions.CacheLoaderException;
import org.ehcache.exceptions.ExceptionFactory;
import org.ehcache.spi.loader.CacheLoader;
import org.ehcache.statistics.CacheOperationOutcomes;
import org.junit.Test;
import org.mockito.Mock;

import java.util.Collections;
import java.util.EnumSet;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isNull;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

/**
 * Provides testing of basic GET operations on an {@code Ehcache}.
 *
 * @author Clifford W. Johnson
 */
public class EhcacheBasicGetTest extends EhcacheBasicCrudBase {

  @Mock
  protected CacheLoader<String, String> cacheLoader;

  @Test
  public void testGetNull() {
    final Ehcache<String, String> ehcache = this.getEhcache(null);

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
   *   <li>no {@code CacheLoader}</li>
   * </ul>
   */
  @Test
  public void testGetNoStoreEntryNoCacheLoader() throws Exception {
    final Ehcache<String, String> ehcache = this.getEhcache(null);

    assertThat(ehcache.get("key"), is(nullValue()));
    verify(this.store).computeIfAbsent(eq("key"), getAnyFunction());
    verifyZeroInteractions(this.spiedResilienceStrategy);
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetOutcome.MISS_NO_LOADER));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoaderOutcome.class));
  }

  /**
   * Tests the effect of a {@link Ehcache#get(Object)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   *   <li>key not available via {@code CacheLoader}</li>
   * </ul>
   */
  @Test
  public void testGetNoStoreEntryNoCacheLoaderEntry() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.<String, String>emptyMap());
    this.store = spy(fakeStore);

    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoader);

    assertThat(ehcache.get("key"), is(nullValue()));
    verify(this.store).computeIfAbsent(eq("key"), getAnyFunction());
    verify(this.cacheLoader).load(eq("key"));
    verifyZeroInteractions(this.spiedResilienceStrategy);
    assertThat(fakeStore.getEntryMap().containsKey("key"), is(false));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetOutcome.MISS_WITH_LOADER));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.CacheLoaderOutcome.SUCCESS));
  }

  /**
   * Tests the effect of a {@link Ehcache#get(Object)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   *   <li>key available via {@code CacheLoader}</li>
   * </ul>
   */
  @Test
  public void testGetNoStoreEntryHasCacheLoaderEntry() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.<String, String>emptyMap());
    this.store = spy(fakeStore);

    when(this.cacheLoader.load("key")).thenReturn("value");
    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoader);

    assertThat(ehcache.get("key"), is("value"));
    verify(this.store).computeIfAbsent(eq("key"), getAnyFunction());
    verify(this.cacheLoader).load(eq("key"));
    verifyZeroInteractions(this.spiedResilienceStrategy);
    assertThat(fakeStore.getEntryMap().get("key"), equalTo("value"));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetOutcome.HIT_WITH_LOADER));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.CacheLoaderOutcome.SUCCESS));
  }

  /**
   * Tests the effect of a {@link Ehcache#get(Object)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   *   <li>{@code CacheLoader.load} throws</li>
   * </ul>
   */
  @Test
  public void testGetNoStoreEntryCacheLoaderException() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.<String, String>emptyMap());
    this.store = spy(fakeStore);

    when(this.cacheLoader.load("key")).thenThrow(new Exception());
    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoader);

    try {
      ehcache.get("key");
      fail();
    } catch (CacheLoaderException e) {
      // Expected
    }
    verify(this.store).computeIfAbsent(eq("key"), getAnyFunction());
    verify(this.cacheLoader).load(eq("key"));
    verifyZeroInteractions(this.spiedResilienceStrategy);
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.GetOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.CacheLoaderOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link Ehcache#get(Object)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   *   <li>{@code Store.computeIfAbsent} throws</li>
   *   <li>{@code CacheLoader} omitted</li>
   * </ul>
   */
  @Test
  public void testGetNoStoreEntryCacheAccessExceptionNoCacheLoader() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.<String, String>emptyMap());
    this.store = spy(fakeStore);
    doThrow(new CacheAccessException("")).when(this.store).computeIfAbsent(eq("key"), getAnyFunction());

    final Ehcache<String, String> ehcache = this.getEhcache(null);

    ehcache.get("key");
    verify(this.store).computeIfAbsent(eq("key"), getAnyFunction());
    verify(this.spiedResilienceStrategy).getFailure(eq("key"), any(CacheAccessException.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetOutcome.FAILURE));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoaderOutcome.class));
  }

  /**
   * Tests the effect of a {@link Ehcache#get(Object)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   *   <li>{@code Store.computeIfAbsent} throws</li>
   *   <li>key not available via {@code CacheLoader}</li>
   * </ul>
   */
  @Test
  public void testGetNoStoreEntryCacheAccessExceptionNoCacheLoaderEntry() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.<String, String>emptyMap());
    this.store = spy(fakeStore);
    doThrow(new CacheAccessException("")).when(this.store).computeIfAbsent(eq("key"), getAnyFunction());

    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoader);

    ehcache.get("key");
    verify(this.store).computeIfAbsent(eq("key"), getAnyFunction());
    verify(this.cacheLoader).load(eq("key"));
    verify(this.spiedResilienceStrategy).getFailure(eq("key"), isNull(String.class), any(CacheAccessException.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetOutcome.FAILURE));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.CacheLoaderOutcome.SUCCESS));
  }

  /**
   * Tests the effect of a {@link Ehcache#get(Object)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   *   <li>{@code Store.computeIfAbsent} throws</li>
   *   <li>key available via {@code CacheLoader}</li>
   * </ul>
   */
  @Test
  public void testGetNoStoreEntryCacheAccessExceptionHasCacheLoaderEntry() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.<String, String>emptyMap());
    this.store = spy(fakeStore);
    doThrow(new CacheAccessException("")).when(this.store).computeIfAbsent(eq("key"), getAnyFunction());

    when(this.cacheLoader.load("key")).thenReturn("value");
    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoader);

    ehcache.get("key");
    verify(this.store).computeIfAbsent(eq("key"), getAnyFunction());
    verify(this.cacheLoader).load(eq("key"));
    verify(this.spiedResilienceStrategy).getFailure(eq("key"), eq("value"), any(CacheAccessException.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetOutcome.FAILURE));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.CacheLoaderOutcome.SUCCESS));
  }

  /**
   * Tests the effect of a {@link Ehcache#get(Object)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   *   <li>{@code Store.computeIfAbsent} throws</li>
   *   <li>{@code CacheLoader.load} throws</li>
   * </ul>
   */
  @Test
  public void testGetNoStoreEntryCacheAccessExceptionCacheLoaderException() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.<String, String>emptyMap());
    this.store = spy(fakeStore);
    doThrow(new CacheAccessException("")).when(this.store).computeIfAbsent(eq("key"), getAnyFunction());

    when(this.cacheLoader.load("key")).thenThrow(new Exception());
    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoader);

    try {
      ehcache.get("key");
      fail();
    } catch (CacheLoaderException e) {
      // Expected
    }
    verify(this.store).computeIfAbsent(eq("key"), getAnyFunction());
    verify(this.cacheLoader).load(eq("key"));
    verify(this.spiedResilienceStrategy).getFailure(eq("key"), any(CacheAccessException.class), any(CacheLoaderException.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetOutcome.FAILURE));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.CacheLoaderOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link Ehcache#get(Object)} for
   * <ul>
   *   <li>key present in {@code Store}</li>
   *   <li>no {@code CacheLoader}</li>
   * </ul>
   */
  @Test
  public void testGetHasStoreEntryNoCacheLoader() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "value"));
    this.store = spy(fakeStore);
    assertThat(fakeStore.getEntryMap().get("key"), equalTo("value"));

    final Ehcache<String, String> ehcache = this.getEhcache(null);

    assertThat(ehcache.get("key"), equalTo("value"));
    verify(this.store).computeIfAbsent(eq("key"), getAnyFunction());
    verifyZeroInteractions(this.spiedResilienceStrategy);
    assertThat(fakeStore.getEntryMap().get("key"), equalTo("value"));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetOutcome.HIT_NO_LOADER));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoaderOutcome.class));
  }

  /**
   * Tests the effect of a {@link Ehcache#get(Object)} for
   * <ul>
   *   <li>key present in {@code Store}</li>
   *   <li>key not available via {@code CacheLoader}</li>
   * </ul>
   */
  @Test
  public void testGetHasStoreEntryNoCacheLoaderEntry() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "value"));
    this.store = spy(fakeStore);
    assertThat(fakeStore.getEntryMap().get("key"), equalTo("value"));

    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoader);

    assertThat(ehcache.get("key"), equalTo("value"));
    verify(this.store).computeIfAbsent(eq("key"), getAnyFunction());
    verify(this.cacheLoader, never()).load(eq("key"));
    verifyZeroInteractions(this.spiedResilienceStrategy);
    assertThat(fakeStore.getEntryMap().get("key"), equalTo("value"));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetOutcome.HIT_WITH_LOADER));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoaderOutcome.class));
  }

  /**
   * Tests the effect of a {@link Ehcache#get(Object)} for
   * <ul>
   *   <li>key present in {@code Store}</li>
   *   <li>key available via {@code CacheLoader}</li>
   * </ul>
   */
  @Test
  public void testGetHasStoreEntryHasCacheLoaderEntry() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "value"));
    this.store = spy(fakeStore);
    assertThat(fakeStore.getEntryMap().get("key"), equalTo("value"));

    when(this.cacheLoader.load("key")).thenReturn("value");
    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoader);

    assertThat(ehcache.get("key"), equalTo("value"));
    verify(this.store).computeIfAbsent(eq("key"), getAnyFunction());
    verify(this.cacheLoader, never()).load(eq("key"));
    verifyZeroInteractions(this.spiedResilienceStrategy);
    assertThat(fakeStore.getEntryMap().get("key"), equalTo("value"));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetOutcome.HIT_WITH_LOADER));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoaderOutcome.class));
  }

  /**
   * Tests the effect of a {@link Ehcache#get(Object)} for
   * <ul>
   *   <li>key present in {@code Store}</li>
   *   <li>{@code CacheLoader.load} throws</li>
   * </ul>
   */
  @Test
  public void testGetHasStoreEntryCacheLoaderException() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "value"));
    this.store = spy(fakeStore);
    assertThat(fakeStore.getEntryMap().get("key"), equalTo("value"));

    when(this.cacheLoader.load("key")).thenThrow(ExceptionFactory.newCacheLoaderException(new Exception()));
    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoader);

    assertThat(ehcache.get("key"), equalTo("value"));
    verify(this.store).computeIfAbsent(eq("key"), getAnyFunction());
    verify(this.cacheLoader, never()).load(eq("key"));
    verifyZeroInteractions(this.spiedResilienceStrategy);
    assertThat(fakeStore.getEntryMap().get("key"), equalTo("value"));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetOutcome.HIT_WITH_LOADER));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoaderOutcome.class));
  }

  /**
   * Tests the effect of a {@link Ehcache#get(Object)} for
   * <ul>
   *   <li>key present in {@code Store}</li>
   *   <li>{@code Store.computeIfAbsent} throws</li>
   *   <li>no {@code CacheLoader}</li>
   * </ul>
   */
  @Test
  public void testGetHasStoreEntryCacheAccessExceptionNoCacheLoader() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "value"));
    this.store = spy(fakeStore);
    assertThat(fakeStore.getEntryMap().get("key"), equalTo("value"));
    doThrow(new CacheAccessException("")).when(this.store).computeIfAbsent(eq("key"), getAnyFunction());

    final Ehcache<String, String> ehcache = this.getEhcache(null);

    ehcache.get("key");
    verify(this.store).computeIfAbsent(eq("key"), getAnyFunction());
    verify(this.spiedResilienceStrategy).getFailure(eq("key"), any(CacheAccessException.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetOutcome.FAILURE));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoaderOutcome.class));
  }

  /**
   * Tests the effect of a {@link Ehcache#get(Object)} for
   * <ul>
   *   <li>key present in {@code Store}</li>
   *   <li>{@code Store.computeIfAbsent} throws</li>
   *   <li>key not available via {@code CacheLoader}</li>
   * </ul>
   */
  @Test
  public void testGetHasStoreEntryCacheAccessExceptionNoCacheLoaderEntry() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "value"));
    this.store = spy(fakeStore);
    assertThat(fakeStore.getEntryMap().get("key"), equalTo("value"));
    doThrow(new CacheAccessException("")).when(this.store).computeIfAbsent(eq("key"), getAnyFunction());

    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoader);

    ehcache.get("key");
    verify(this.store).computeIfAbsent(eq("key"), getAnyFunction());
    verify(this.cacheLoader).load(eq("key"));
    verify(this.spiedResilienceStrategy).getFailure(eq("key"), isNull(String.class), any(CacheAccessException.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetOutcome.FAILURE));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.CacheLoaderOutcome.SUCCESS));
  }

  /**
   * Tests the effect of a {@link Ehcache#get(Object)} for
   * <ul>
   *   <li>key present in {@code Store}</li>
   *   <li>{@code Store.computeIfAbsent} throws</li>
   *   <li>key available via {@code CacheLoader}</li>
   * </ul>
   */
  @Test
  public void testGetHasStoreEntryCacheAccessExceptionHasCacheLoaderEntry() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "value"));
    this.store = spy(fakeStore);
    assertThat(fakeStore.getEntryMap().get("key"), equalTo("value"));
    doThrow(new CacheAccessException("")).when(this.store).computeIfAbsent(eq("key"), getAnyFunction());

    when(this.cacheLoader.load("key")).thenReturn("value");
    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoader);

    ehcache.get("key");
    verify(this.store).computeIfAbsent(eq("key"), getAnyFunction());
    verify(this.cacheLoader).load(eq("key"));
    verify(this.spiedResilienceStrategy).getFailure(eq("key"), eq("value"), any(CacheAccessException.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetOutcome.FAILURE));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.CacheLoaderOutcome.SUCCESS));
  }

  /**
   * Tests the effect of a {@link Ehcache#get(Object)} for
   * <ul>
   *   <li>key present in {@code Store}</li>
   *   <li>{@code Store.computeIfAbsent} throws</li>
   *   <li>{@code CacheLoader.load} throws</li>
   * </ul>
   */
  @Test
  public void testGetHasStoreEntryCacheAccessExceptionCacheLoaderException() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "value"));
    this.store = spy(fakeStore);
    assertThat(fakeStore.getEntryMap().get("key"), equalTo("value"));
    doThrow(new CacheAccessException("")).when(this.store).computeIfAbsent(eq("key"), getAnyFunction());

    when(this.cacheLoader.load("key")).thenThrow(new Exception());
    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoader);

    try {
      ehcache.get("key");
      fail();
    } catch (CacheLoaderException e) {
      // Expected
    }
    verify(this.store).computeIfAbsent(eq("key"), getAnyFunction());
    verify(this.cacheLoader).load(eq("key"));
    verify(this.spiedResilienceStrategy).getFailure(eq("key"), any(CacheAccessException.class), any(CacheLoaderException.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetOutcome.FAILURE));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.CacheLoaderOutcome.FAILURE));
  }

  /**
   * Gets an initialized {@link Ehcache Ehcache} instance using the
   * {@link org.ehcache.spi.loader.CacheLoader CacheLoader} provided.
   *
   * @param cacheLoader
   *    the {@code CacheLoader} to use; may be {@code null}
   *
   * @return a new {@code Ehcache} instance
   */
  private Ehcache<String, String> getEhcache(final CacheLoader<String, String> cacheLoader) {
    final Ehcache<String, String> ehcache = new Ehcache<String, String>(CACHE_CONFIGURATION, this.store, cacheLoader);
    ehcache.init();
    assertThat("cache not initialized", ehcache.getStatus(), is(Status.AVAILABLE));
    this.spiedResilienceStrategy = this.setResilienceStrategySpy(ehcache);
    return ehcache;
  }
}
