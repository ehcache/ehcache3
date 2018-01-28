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
import org.ehcache.core.internal.resilience.RobustLoaderWriterResilienceStrategy;
import org.ehcache.core.resilience.DefaultRecoveryStore;
import org.ehcache.spi.loaderwriter.CacheLoadingException;
import org.ehcache.core.exceptions.ExceptionFactory;
import org.ehcache.core.statistics.CacheOperationOutcomes;
import org.ehcache.spi.resilience.StoreAccessException;
import org.hamcrest.CoreMatchers;
import org.junit.Test;
import org.mockito.Mock;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.EnumSet;

import org.ehcache.spi.loaderwriter.CacheLoaderWriter;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

/**
 * Provides testing of basic GET operations on an {@code EhcacheWithLoaderWriter}.
 *
 * @author Clifford W. Johnson
 */
public class EhcacheWithLoaderWriterBasicGetTest extends EhcacheBasicCrudBase {

  @Mock
  protected CacheLoaderWriter<String, String> cacheLoaderWriter;

  @Test
  public void testGetNull() {
    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    try {
      ehcache.get(null);
      fail();
    } catch (NullPointerException e) {
      // expected
    }
  }

  /**
   * Tests the effect of a {@link EhcacheWithLoaderWriter#get(Object)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   * </ul>
   */
  @Test
  public void testGetNoStoreEntry() throws Exception {
    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    assertThat(ehcache.get("key"), is(nullValue()));
    verify(this.store).computeIfAbsent(eq("key"), getAnyFunction());
    verifyZeroInteractions(this.resilienceStrategy, this.cacheLoaderWriter);
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetOutcome.MISS));
  }

  /**
   * Tests the effect of a {@link EhcacheWithLoaderWriter#get(Object)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   *   <li>key not available via {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testGetNoStoreEntryNoCacheLoaderWriterEntry() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.<String, String>emptyMap());
    this.store = spy(fakeStore);

    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    assertThat(ehcache.get("key"), is(nullValue()));
    verify(this.store).computeIfAbsent(eq("key"), getAnyFunction());
    verify(this.cacheLoaderWriter).load(eq("key"));
    verifyZeroInteractions(this.resilienceStrategy);
    assertThat(fakeStore.getEntryMap().containsKey("key"), is(false));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetOutcome.MISS));
  }

  /**
   * Tests the effect of a {@link EhcacheWithLoaderWriter#get(Object)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   *   <li>key available via {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testGetNoStoreEntryHasCacheLoaderWriterEntry() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.<String, String>emptyMap());
    this.store = spy(fakeStore);

    when(this.cacheLoaderWriter.load("key")).thenReturn("value");
    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    assertThat(ehcache.get("key"), is("value"));
    verify(this.store).computeIfAbsent(eq("key"), getAnyFunction());
    verify(this.cacheLoaderWriter).load(eq("key"));
    verifyZeroInteractions(this.resilienceStrategy);
    assertThat(fakeStore.getEntryMap().get("key"), equalTo("value"));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetOutcome.HIT));
  }

  /**
   * Tests the effect of a {@link EhcacheWithLoaderWriter#get(Object)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   *   <li>{@code CacheLoaderWriter.load} throws</li>
   * </ul>
   */
  @Test
  public void testGetNoStoreEntryCacheLoadingException() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.<String, String>emptyMap());
    this.store = spy(fakeStore);

    when(this.cacheLoaderWriter.load("key")).thenThrow(new Exception());
    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    try {
      ehcache.get("key");
      fail();
    } catch (CacheLoadingException e) {
      // Expected
    }
    verify(this.store).computeIfAbsent(eq("key"), getAnyFunction());
    verify(this.cacheLoaderWriter).load(eq("key"));
    verifyZeroInteractions(this.resilienceStrategy);
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link EhcacheWithLoaderWriter#get(Object)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   *   <li>{@code Store.computeIfAbsent} throws</li>
   *   <li>key not available via {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testGetNoStoreEntryStoreAccessExceptionNoCacheLoaderWriterEntry() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.<String, String>emptyMap());
    this.store = spy(fakeStore);
    doThrow(new StoreAccessException("")).when(this.store).computeIfAbsent(eq("key"), getAnyFunction());

    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    ehcache.get("key");
    verify(this.store).computeIfAbsent(eq("key"), getAnyFunction());
    verify(this.cacheLoaderWriter).load(eq("key"));
    verify(this.resilienceStrategy).getFailure(eq("key"), any(StoreAccessException.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link EhcacheWithLoaderWriter#get(Object)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   *   <li>{@code Store.computeIfAbsent} throws</li>
   *   <li>key available via {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testGetNoStoreEntryStoreAccessExceptionHasCacheLoaderWriterEntry() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.<String, String>emptyMap());
    this.store = spy(fakeStore);
    doThrow(new StoreAccessException("")).when(this.store).computeIfAbsent(eq("key"), getAnyFunction());

    when(this.cacheLoaderWriter.load("key")).thenReturn("value");
    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    ehcache.get("key");
    verify(this.store).computeIfAbsent(eq("key"), getAnyFunction());
    verify(this.cacheLoaderWriter).load(eq("key"));
    verify(this.resilienceStrategy).getFailure(eq("key"), any(StoreAccessException.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link EhcacheWithLoaderWriter#get(Object)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   *   <li>{@code Store.computeIfAbsent} throws</li>
   *   <li>{@code CacheLoaderWriter.load} throws</li>
   * </ul>
   */
  @Test
  public void testGetNoStoreEntryStoreAccessExceptionCacheLoadingException() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.<String, String>emptyMap());
    this.store = spy(fakeStore);
    doThrow(new StoreAccessException("")).when(this.store).computeIfAbsent(eq("key"), getAnyFunction());

    when(this.cacheLoaderWriter.load("key")).thenThrow(new Exception());
    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    try {
      ehcache.get("key");
      fail();
    } catch (CacheLoadingException e) {
      // Expected
    }
    verify(this.store).computeIfAbsent(eq("key"), getAnyFunction());
    verify(this.cacheLoaderWriter).load(eq("key"));
    verify(this.resilienceStrategy).getFailure(eq("key"), any(StoreAccessException.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link EhcacheWithLoaderWriter#get(Object)} for
   * <ul>
   *   <li>key present in {@code Store}</li>
   * </ul>
   */
  @Test
  public void testGetHasStoreEntry() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "value"));
    this.store = spy(fakeStore);
    assertThat(fakeStore.getEntryMap().get("key"), equalTo("value"));

    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    assertThat(ehcache.get("key"), equalTo("value"));
    verify(this.store).computeIfAbsent(eq("key"), getAnyFunction());
    verifyZeroInteractions(this.resilienceStrategy, this.cacheLoaderWriter);
    assertThat(fakeStore.getEntryMap().get("key"), equalTo("value"));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetOutcome.HIT));
  }

  /**
   * Tests the effect of a {@link EhcacheWithLoaderWriter#get(Object)} for
   * <ul>
   *   <li>key present in {@code Store}</li>
   *   <li>key not available via {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testGetHasStoreEntryNoCacheLoaderWriterEntry() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "value"));
    this.store = spy(fakeStore);
    assertThat(fakeStore.getEntryMap().get("key"), equalTo("value"));

    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    assertThat(ehcache.get("key"), equalTo("value"));
    verify(this.store).computeIfAbsent(eq("key"), getAnyFunction());
    verify(this.cacheLoaderWriter, never()).load(eq("key"));
    verifyZeroInteractions(this.resilienceStrategy);
    assertThat(fakeStore.getEntryMap().get("key"), equalTo("value"));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetOutcome.HIT));
  }

  /**
   * Tests the effect of a {@link EhcacheWithLoaderWriter#get(Object)} for
   * <ul>
   *   <li>key present in {@code Store}</li>
   *   <li>key available via {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testGetHasStoreEntryHasCacheLoaderWriterEntry() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "value"));
    this.store = spy(fakeStore);
    assertThat(fakeStore.getEntryMap().get("key"), equalTo("value"));

    when(this.cacheLoaderWriter.load("key")).thenReturn("value");
    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    assertThat(ehcache.get("key"), equalTo("value"));
    verify(this.store).computeIfAbsent(eq("key"), getAnyFunction());
    verify(this.cacheLoaderWriter, never()).load(eq("key"));
    verifyZeroInteractions(this.resilienceStrategy);
    assertThat(fakeStore.getEntryMap().get("key"), equalTo("value"));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetOutcome.HIT));
  }

  /**
   * Tests the effect of a {@link EhcacheWithLoaderWriter#get(Object)} for
   * <ul>
   *   <li>key present in {@code Store}</li>
   *   <li>{@code CacheLoaderWriter.load} throws</li>
   * </ul>
   */
  @Test
  public void testGetHasStoreEntryCacheLoadingException() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "value"));
    this.store = spy(fakeStore);
    assertThat(fakeStore.getEntryMap().get("key"), equalTo("value"));

    when(this.cacheLoaderWriter.load("key")).thenThrow(ExceptionFactory.newCacheLoadingException(new Exception()));
    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    assertThat(ehcache.get("key"), equalTo("value"));
    verify(this.store).computeIfAbsent(eq("key"), getAnyFunction());
    verify(this.cacheLoaderWriter, never()).load(eq("key"));
    verifyZeroInteractions(this.resilienceStrategy);
    assertThat(fakeStore.getEntryMap().get("key"), equalTo("value"));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetOutcome.HIT));
  }

  /**
   * Tests the effect of a {@link EhcacheWithLoaderWriter#get(Object)} for
   * <ul>
   *   <li>key present in {@code Store}</li>
   *   <li>{@code Store.computeIfAbsent} throws</li>
   *   <li>key not available via {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testGetHasStoreEntryStoreAccessExceptionNoCacheLoaderWriterEntry() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "value"));
    this.store = spy(fakeStore);
    assertThat(fakeStore.getEntryMap().get("key"), equalTo("value"));
    doThrow(new StoreAccessException("")).when(this.store).computeIfAbsent(eq("key"), getAnyFunction());

    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    ehcache.get("key");
    verify(this.store).computeIfAbsent(eq("key"), getAnyFunction());
    verify(this.cacheLoaderWriter).load(eq("key"));
    verify(this.resilienceStrategy).getFailure(eq("key"), any(StoreAccessException.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link EhcacheWithLoaderWriter#get(Object)} for
   * <ul>
   *   <li>key present in {@code Store}</li>
   *   <li>{@code Store.computeIfAbsent} throws</li>
   *   <li>key available via {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testGetHasStoreEntryStoreAccessExceptionHasCacheLoaderWriterEntry() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "value"));
    this.store = spy(fakeStore);
    assertThat(fakeStore.getEntryMap().get("key"), equalTo("value"));
    doThrow(new StoreAccessException("")).when(this.store).computeIfAbsent(eq("key"), getAnyFunction());

    when(this.cacheLoaderWriter.load("key")).thenReturn("value");
    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    ehcache.get("key");
    verify(this.store).computeIfAbsent(eq("key"), getAnyFunction());
    verify(this.cacheLoaderWriter).load(eq("key"));
    verify(this.resilienceStrategy).getFailure(eq("key"), any(StoreAccessException.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link EhcacheWithLoaderWriter#get(Object)} for
   * <ul>
   *   <li>key present in {@code Store}</li>
   *   <li>{@code Store.computeIfAbsent} throws</li>
   *   <li>{@code CacheLoaderWriter.load} throws</li>
   * </ul>
   */
  @Test
  public void testGetHasStoreEntryStoreAccessExceptionCacheLoadingException() throws Exception {
    FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "value"));
    this.store = spy(fakeStore);
    assertThat(fakeStore.getEntryMap().get("key"), equalTo("value"));
    doThrow(new StoreAccessException("")).when(this.store).computeIfAbsent(eq("key"), getAnyFunction());

    when(this.cacheLoaderWriter.load("key")).thenThrow(new Exception());
    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    try {
      ehcache.get("key");
      fail();
    } catch (CacheLoadingException e) {
      // Expected
    }
    verify(this.store).computeIfAbsent(eq("key"), getAnyFunction());
    verify(this.cacheLoaderWriter).load(eq("key"));
    verify(this.resilienceStrategy).getFailure(eq("key"), any(StoreAccessException.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetOutcome.FAILURE));
  }

  /**
   * Gets an initialized {@link EhcacheWithLoaderWriter Ehcache} instance using the
   * {@link CacheLoaderWriter} provided.
   *
   * @param cacheLoaderWriter
   *    the {@code CacheLoaderWriter} to use; may be {@code null}
   *
   * @return a new {@code EhcacheWithLoaderWriter} instance
   */
  private EhcacheWithLoaderWriter<String, String> getEhcache(final CacheLoaderWriter<String, String> cacheLoaderWriter) {
    this.resilienceStrategy = spy(new RobustLoaderWriterResilienceStrategy<>(new DefaultRecoveryStore<>(this.store), cacheLoaderWriter));
    final EhcacheWithLoaderWriter<String, String> ehcache = new EhcacheWithLoaderWriter<>(CACHE_CONFIGURATION, this.store, resilienceStrategy, cacheLoaderWriter, cacheEventDispatcher, LoggerFactory
      .getLogger(EhcacheWithLoaderWriter.class + "-" + "EhcacheWithLoaderWriterBasicGetTest"));
    ehcache.init();
    assertThat("cache not initialized", ehcache.getStatus(), CoreMatchers.is(Status.AVAILABLE));
    return ehcache;
  }
}
