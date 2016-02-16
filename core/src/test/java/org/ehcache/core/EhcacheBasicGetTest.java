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
import org.ehcache.core.Ehcache;
import org.ehcache.exceptions.CacheAccessException;
import org.ehcache.exceptions.CacheLoadingException;
import org.ehcache.core.exceptions.ExceptionFactory;
import org.ehcache.core.statistics.CacheOperationOutcomes;
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
  protected CacheLoaderWriter<String, String> cacheLoaderWriter;

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
   *   <li>no {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testGetNoStoreEntryNoCacheLoaderWriter() throws Exception {
    final Ehcache<String, String> ehcache = this.getEhcache(null);

    assertThat(ehcache.get("key"), is(nullValue()));
    verify(this.store).computeIfAbsent(eq("key"), getAnyFunction());
    verifyZeroInteractions(this.spiedResilienceStrategy);
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetOutcome.MISS_NO_LOADER));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoadingOutcome.class));
  }

  /**
   * Tests the effect of a {@link Ehcache#get(Object)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   *   <li>key not available via {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testGetNoStoreEntryNoCacheLoaderWriterEntry() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.<String, String>emptyMap());
    this.store = spy(fakeStore);

    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    assertThat(ehcache.get("key"), is(nullValue()));
    verify(this.store).computeIfAbsent(eq("key"), getAnyFunction());
    verify(this.cacheLoaderWriter).load(eq("key"));
    verifyZeroInteractions(this.spiedResilienceStrategy);
    assertThat(fakeStore.getEntryMap().containsKey("key"), is(false));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetOutcome.MISS_WITH_LOADER));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.CacheLoadingOutcome.SUCCESS));
  }

  /**
   * Tests the effect of a {@link Ehcache#get(Object)} for
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
    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    assertThat(ehcache.get("key"), is("value"));
    verify(this.store).computeIfAbsent(eq("key"), getAnyFunction());
    verify(this.cacheLoaderWriter).load(eq("key"));
    verifyZeroInteractions(this.spiedResilienceStrategy);
    assertThat(fakeStore.getEntryMap().get("key"), equalTo("value"));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetOutcome.HIT_WITH_LOADER));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.CacheLoadingOutcome.SUCCESS));
  }

  /**
   * Tests the effect of a {@link Ehcache#get(Object)} for
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
    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    try {
      ehcache.get("key");
      fail();
    } catch (CacheLoadingException e) {
      // Expected
    }
    verify(this.store).computeIfAbsent(eq("key"), getAnyFunction());
    verify(this.cacheLoaderWriter).load(eq("key"));
    verifyZeroInteractions(this.spiedResilienceStrategy);
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.GetOutcome.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.CacheLoadingOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link Ehcache#get(Object)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   *   <li>{@code Store.computeIfAbsent} throws</li>
   *   <li>{@code CacheLoaderWriter} omitted</li>
   * </ul>
   */
  @Test
  public void testGetNoStoreEntryCacheAccessExceptionNoCacheLoaderWriter() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.<String, String>emptyMap());
    this.store = spy(fakeStore);
    doThrow(new CacheAccessException("")).when(this.store).computeIfAbsent(eq("key"), getAnyFunction());

    final Ehcache<String, String> ehcache = this.getEhcache(null);

    ehcache.get("key");
    verify(this.store).computeIfAbsent(eq("key"), getAnyFunction());
    verify(this.spiedResilienceStrategy).getFailure(eq("key"), any(CacheAccessException.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetOutcome.FAILURE));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoadingOutcome.class));
  }

  /**
   * Tests the effect of a {@link Ehcache#get(Object)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   *   <li>{@code Store.computeIfAbsent} throws</li>
   *   <li>key not available via {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testGetNoStoreEntryCacheAccessExceptionNoCacheLoaderWriterEntry() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.<String, String>emptyMap());
    this.store = spy(fakeStore);
    doThrow(new CacheAccessException("")).when(this.store).computeIfAbsent(eq("key"), getAnyFunction());

    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    ehcache.get("key");
    verify(this.store).computeIfAbsent(eq("key"), getAnyFunction());
    verify(this.cacheLoaderWriter).load(eq("key"));
    verify(this.spiedResilienceStrategy).getFailure(eq("key"), isNull(String.class), any(CacheAccessException.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetOutcome.FAILURE));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.CacheLoadingOutcome.SUCCESS));
  }

  /**
   * Tests the effect of a {@link Ehcache#get(Object)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   *   <li>{@code Store.computeIfAbsent} throws</li>
   *   <li>key available via {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testGetNoStoreEntryCacheAccessExceptionHasCacheLoaderWriterEntry() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.<String, String>emptyMap());
    this.store = spy(fakeStore);
    doThrow(new CacheAccessException("")).when(this.store).computeIfAbsent(eq("key"), getAnyFunction());

    when(this.cacheLoaderWriter.load("key")).thenReturn("value");
    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    ehcache.get("key");
    verify(this.store).computeIfAbsent(eq("key"), getAnyFunction());
    verify(this.cacheLoaderWriter).load(eq("key"));
    verify(this.spiedResilienceStrategy).getFailure(eq("key"), eq("value"), any(CacheAccessException.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetOutcome.FAILURE));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.CacheLoadingOutcome.SUCCESS));
  }

  /**
   * Tests the effect of a {@link Ehcache#get(Object)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   *   <li>{@code Store.computeIfAbsent} throws</li>
   *   <li>{@code CacheLoaderWriter.load} throws</li>
   * </ul>
   */
  @Test
  public void testGetNoStoreEntryCacheAccessExceptionCacheLoadingException() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.<String, String>emptyMap());
    this.store = spy(fakeStore);
    doThrow(new CacheAccessException("")).when(this.store).computeIfAbsent(eq("key"), getAnyFunction());

    when(this.cacheLoaderWriter.load("key")).thenThrow(new Exception());
    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    try {
      ehcache.get("key");
      fail();
    } catch (CacheLoadingException e) {
      // Expected
    }
    verify(this.store).computeIfAbsent(eq("key"), getAnyFunction());
    verify(this.cacheLoaderWriter).load(eq("key"));
    verify(this.spiedResilienceStrategy).getFailure(eq("key"), any(CacheAccessException.class), any(CacheLoadingException.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetOutcome.FAILURE));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.CacheLoadingOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link Ehcache#get(Object)} for
   * <ul>
   *   <li>key present in {@code Store}</li>
   *   <li>no {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testGetHasStoreEntryNoCacheLoaderWriter() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "value"));
    this.store = spy(fakeStore);
    assertThat(fakeStore.getEntryMap().get("key"), equalTo("value"));

    final Ehcache<String, String> ehcache = this.getEhcache(null);

    assertThat(ehcache.get("key"), equalTo("value"));
    verify(this.store).computeIfAbsent(eq("key"), getAnyFunction());
    verifyZeroInteractions(this.spiedResilienceStrategy);
    assertThat(fakeStore.getEntryMap().get("key"), equalTo("value"));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetOutcome.HIT_NO_LOADER));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoadingOutcome.class));
  }

  /**
   * Tests the effect of a {@link Ehcache#get(Object)} for
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

    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    assertThat(ehcache.get("key"), equalTo("value"));
    verify(this.store).computeIfAbsent(eq("key"), getAnyFunction());
    verify(this.cacheLoaderWriter, never()).load(eq("key"));
    verifyZeroInteractions(this.spiedResilienceStrategy);
    assertThat(fakeStore.getEntryMap().get("key"), equalTo("value"));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetOutcome.HIT_WITH_LOADER));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoadingOutcome.class));
  }

  /**
   * Tests the effect of a {@link Ehcache#get(Object)} for
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
    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    assertThat(ehcache.get("key"), equalTo("value"));
    verify(this.store).computeIfAbsent(eq("key"), getAnyFunction());
    verify(this.cacheLoaderWriter, never()).load(eq("key"));
    verifyZeroInteractions(this.spiedResilienceStrategy);
    assertThat(fakeStore.getEntryMap().get("key"), equalTo("value"));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetOutcome.HIT_WITH_LOADER));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoadingOutcome.class));
  }

  /**
   * Tests the effect of a {@link Ehcache#get(Object)} for
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
    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    assertThat(ehcache.get("key"), equalTo("value"));
    verify(this.store).computeIfAbsent(eq("key"), getAnyFunction());
    verify(this.cacheLoaderWriter, never()).load(eq("key"));
    verifyZeroInteractions(this.spiedResilienceStrategy);
    assertThat(fakeStore.getEntryMap().get("key"), equalTo("value"));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetOutcome.HIT_WITH_LOADER));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoadingOutcome.class));
  }

  /**
   * Tests the effect of a {@link Ehcache#get(Object)} for
   * <ul>
   *   <li>key present in {@code Store}</li>
   *   <li>{@code Store.computeIfAbsent} throws</li>
   *   <li>no {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testGetHasStoreEntryCacheAccessExceptionNoCacheLoaderWriter() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "value"));
    this.store = spy(fakeStore);
    assertThat(fakeStore.getEntryMap().get("key"), equalTo("value"));
    doThrow(new CacheAccessException("")).when(this.store).computeIfAbsent(eq("key"), getAnyFunction());

    final Ehcache<String, String> ehcache = this.getEhcache(null);

    ehcache.get("key");
    verify(this.store).computeIfAbsent(eq("key"), getAnyFunction());
    verify(this.spiedResilienceStrategy).getFailure(eq("key"), any(CacheAccessException.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetOutcome.FAILURE));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.CacheLoadingOutcome.class));
  }

  /**
   * Tests the effect of a {@link Ehcache#get(Object)} for
   * <ul>
   *   <li>key present in {@code Store}</li>
   *   <li>{@code Store.computeIfAbsent} throws</li>
   *   <li>key not available via {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testGetHasStoreEntryCacheAccessExceptionNoCacheLoaderWriterEntry() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "value"));
    this.store = spy(fakeStore);
    assertThat(fakeStore.getEntryMap().get("key"), equalTo("value"));
    doThrow(new CacheAccessException("")).when(this.store).computeIfAbsent(eq("key"), getAnyFunction());

    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    ehcache.get("key");
    verify(this.store).computeIfAbsent(eq("key"), getAnyFunction());
    verify(this.cacheLoaderWriter).load(eq("key"));
    verify(this.spiedResilienceStrategy).getFailure(eq("key"), isNull(String.class), any(CacheAccessException.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetOutcome.FAILURE));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.CacheLoadingOutcome.SUCCESS));
  }

  /**
   * Tests the effect of a {@link Ehcache#get(Object)} for
   * <ul>
   *   <li>key present in {@code Store}</li>
   *   <li>{@code Store.computeIfAbsent} throws</li>
   *   <li>key available via {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testGetHasStoreEntryCacheAccessExceptionHasCacheLoaderWriterEntry() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "value"));
    this.store = spy(fakeStore);
    assertThat(fakeStore.getEntryMap().get("key"), equalTo("value"));
    doThrow(new CacheAccessException("")).when(this.store).computeIfAbsent(eq("key"), getAnyFunction());

    when(this.cacheLoaderWriter.load("key")).thenReturn("value");
    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    ehcache.get("key");
    verify(this.store).computeIfAbsent(eq("key"), getAnyFunction());
    verify(this.cacheLoaderWriter).load(eq("key"));
    verify(this.spiedResilienceStrategy).getFailure(eq("key"), eq("value"), any(CacheAccessException.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetOutcome.FAILURE));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.CacheLoadingOutcome.SUCCESS));
  }

  /**
   * Tests the effect of a {@link Ehcache#get(Object)} for
   * <ul>
   *   <li>key present in {@code Store}</li>
   *   <li>{@code Store.computeIfAbsent} throws</li>
   *   <li>{@code CacheLoaderWriter.load} throws</li>
   * </ul>
   */
  @Test
  public void testGetHasStoreEntryCacheAccessExceptionCacheLoadingException() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "value"));
    this.store = spy(fakeStore);
    assertThat(fakeStore.getEntryMap().get("key"), equalTo("value"));
    doThrow(new CacheAccessException("")).when(this.store).computeIfAbsent(eq("key"), getAnyFunction());

    when(this.cacheLoaderWriter.load("key")).thenThrow(new Exception());
    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    try {
      ehcache.get("key");
      fail();
    } catch (CacheLoadingException e) {
      // Expected
    }
    verify(this.store).computeIfAbsent(eq("key"), getAnyFunction());
    verify(this.cacheLoaderWriter).load(eq("key"));
    verify(this.spiedResilienceStrategy).getFailure(eq("key"), any(CacheAccessException.class), any(CacheLoadingException.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetOutcome.FAILURE));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.CacheLoadingOutcome.FAILURE));
  }

  /**
   * Gets an initialized {@link Ehcache Ehcache} instance using the
   * {@link CacheLoaderWriter} provided.
   *
   * @param cacheLoaderWriter
   *    the {@code CacheLoaderWriter} to use; may be {@code null}
   *
   * @return a new {@code Ehcache} instance
   */
  private Ehcache<String, String> getEhcache(final CacheLoaderWriter<String, String> cacheLoaderWriter) {
    final Ehcache<String, String> ehcache = new Ehcache<String, String>(CACHE_CONFIGURATION, this.store, cacheLoaderWriter, cacheEventDispatcher, LoggerFactory.getLogger(Ehcache.class + "-" + "EhcacheBasicGetTest"));
    ehcache.init();
    assertThat("cache not initialized", ehcache.getStatus(), CoreMatchers.is(Status.AVAILABLE));
    this.spiedResilienceStrategy = this.setResilienceStrategySpy(ehcache);
    return ehcache;
  }
}
