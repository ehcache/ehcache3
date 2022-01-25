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
import org.ehcache.config.CacheConfiguration;
import org.ehcache.core.config.BaseCacheConfiguration;
import org.ehcache.core.config.ResourcePoolsHelper;
import org.ehcache.core.events.CacheEventDispatcher;
import org.ehcache.core.internal.resilience.RobustLoaderWriterResilienceStrategy;
import org.ehcache.core.resilience.DefaultRecoveryStore;
import org.ehcache.core.statistics.CacheOperationOutcomes;
import org.ehcache.expiry.ExpiryPolicy;
import org.ehcache.spi.loaderwriter.CacheWritingException;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.ehcache.spi.resilience.StoreAccessException;
import org.hamcrest.Matchers;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.EnumSet;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

/**
 * Provides testing of basic PUT_IF_ABSENT operations on an {@code EhcacheWithLoaderWriter}.
 *
 * @author Clifford W. Johnson
 */
public class EhcacheWithLoaderWriterBasicPutIfAbsentTest extends EhcacheBasicCrudBase {

  @Mock
  protected CacheLoaderWriter<String, String> cacheLoaderWriter;
  @Mock
  private CacheEventDispatcher<String, String> cacheEventDispatcher;

  @Test
  public void testPutIfAbsentNullNull() {
    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    try {
      ehcache.putIfAbsent(null, null);
      fail();
    } catch (NullPointerException e) {
      // expected
    }
  }

  @Test
  public void testPutIfAbsentKeyNull() {
    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    try {
      ehcache.putIfAbsent("key", null);
      fail();
    } catch (NullPointerException e) {
      // expected
    }
  }

  @Test
  public void testPutIfAbsentNullValue() {
    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    try {
      ehcache.putIfAbsent(null, "value");
      fail();
    } catch (NullPointerException e) {
      // expected
    }
  }

  /**
   * Tests the effect of a {@link EhcacheWithLoaderWriter#putIfAbsent(Object, Object)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   * </ul>
   */
  @Test
  public void testPutIfAbsentNoStoreEntry() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.emptyMap());
    this.store = spy(fakeStore);

    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    assertThat(ehcache.putIfAbsent("key", "value"), is(nullValue()));
    verify(this.store).computeIfAbsent(eq("key"), getAnyFunction());
    verifyZeroInteractions(this.resilienceStrategy);
    assertThat(fakeStore.getEntryMap().get("key"), equalTo("value"));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.PutIfAbsentOutcome.PUT));
  }

  /**
   * Tests the effect of a {@link EhcacheWithLoaderWriter#putIfAbsent(Object, Object)} for
   * <ul>
   *   <li>key present in {@code Store}</li>
   * </ul>
   */
  @Test
  public void testPutIfAbsentHasStoreEntry() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "oldValue"));
    this.store = spy(fakeStore);

    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    assertThat(ehcache.putIfAbsent("key", "value"), is(equalTo("oldValue")));
    verify(this.store).computeIfAbsent(eq("key"), getAnyFunction());
    verifyZeroInteractions(this.resilienceStrategy);
    assertThat(fakeStore.getEntryMap().get("key"), equalTo("oldValue"));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.PutIfAbsentOutcome.HIT));
  }

  /**
   * Tests the effect of a {@link EhcacheWithLoaderWriter#putIfAbsent(Object, Object)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   *   <li>key not present via {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testPutIfAbsentNoStoreEntryNoCacheLoaderWriterEntry() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.emptyMap());
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(Collections.emptyMap());
    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(fakeLoaderWriter);

    assertThat(ehcache.putIfAbsent("key", "value"), is(nullValue()));
    verify(this.store).computeIfAbsent(eq("key"), getAnyFunction());
    verifyZeroInteractions(this.resilienceStrategy);
    assertThat(fakeStore.getEntryMap().get("key"), equalTo("value"));
    assertThat(fakeLoaderWriter.getEntryMap().get("key"), equalTo("value"));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.PutIfAbsentOutcome.PUT));
  }

  /**
   * Tests the effect of a {@link EhcacheWithLoaderWriter#putIfAbsent(Object, Object)} for
   * <ul>
   *   <li>key present in {@code Store}</li>
   *   <li>key not present via {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testPutIfAbsentHasStoreEntryNoCacheLoaderWriterEntry() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "oldValue"));
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(Collections.emptyMap());
    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(fakeLoaderWriter);

    assertThat(ehcache.putIfAbsent("key", "value"), is(equalTo("oldValue")));
    verify(this.store).computeIfAbsent(eq("key"), getAnyFunction());
    verifyZeroInteractions(this.resilienceStrategy);
    assertThat(fakeStore.getEntryMap().get("key"), equalTo("oldValue"));
    // Broken initial state: CacheLoaderWriter check omitted
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.PutIfAbsentOutcome.HIT));
  }

  /**
   * Tests the effect of a {@link EhcacheWithLoaderWriter#putIfAbsent(Object, Object)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   *   <li>{@code Store.computeIfAbsent} throws</li>
   *   <li>key not present via {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testPutIfAbsentNoStoreEntryStoreAccessExceptionNoCacheLoaderWriterEntry() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.emptyMap());
    this.store = spy(fakeStore);
    doThrow(new StoreAccessException("")).when(this.store).computeIfAbsent(eq("key"), getAnyFunction());

    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(Collections.emptyMap());
    this.cacheLoaderWriter = spy(fakeLoaderWriter);
    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    final InOrder ordered = inOrder(this.cacheLoaderWriter, this.resilienceStrategy);

    assertThat(ehcache.putIfAbsent("key", "value"), nullValue());
    verify(this.store).computeIfAbsent(eq("key"), getAnyFunction());
    ordered.verify(this.resilienceStrategy).putIfAbsentFailure(eq("key"), eq("value"), any(StoreAccessException.class));
    ordered.verify(this.cacheLoaderWriter).write(eq("key"), eq("value"));
    assertThat(fakeLoaderWriter.getEntryMap().get("key"), equalTo("value"));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.PutIfAbsentOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link EhcacheWithLoaderWriter#putIfAbsent(Object, Object)} for
   * <ul>
   *   <li>key present in {@code Store}</li>
   *   <li>{@code Store.computeIfAbsent} throws</li>
   *   <li>key not present via {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testPutIfAbsentHasStoreEntryStoreAccessExceptionNoCacheLoaderWriterEntry() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "oldValue"));
    this.store = spy(fakeStore);
    doThrow(new StoreAccessException("")).when(this.store).computeIfAbsent(eq("key"), getAnyFunction());

    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(Collections.emptyMap());
    this.cacheLoaderWriter = spy(fakeLoaderWriter);
    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    final InOrder ordered = inOrder(this.cacheLoaderWriter, this.resilienceStrategy);

    assertThat(ehcache.putIfAbsent("key", "value"), nullValue());
    verify(this.store).computeIfAbsent(eq("key"), getAnyFunction());
    ordered.verify(this.resilienceStrategy).putIfAbsentFailure(eq("key"), eq("value"), any(StoreAccessException.class));
    ordered.verify(this.cacheLoaderWriter).write(eq("key"), eq("value"));
    // Broken initial state: CacheLoaderWriter check omitted
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.PutIfAbsentOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link EhcacheWithLoaderWriter#putIfAbsent(Object, Object)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   *   <li>key present via {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testPutIfAbsentNoStoreEntryHasCacheLoaderWriterEntry() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.emptyMap());
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(Collections.singletonMap("key", "oldValue"));
    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(fakeLoaderWriter);

    assertThat(ehcache.putIfAbsent("key", "value"), is("oldValue"));
    verify(this.store).computeIfAbsent(eq("key"), getAnyFunction());
    verifyZeroInteractions(this.resilienceStrategy);
    assertThat(fakeStore.getEntryMap().get("key"), equalTo("oldValue"));
    assertThat(fakeLoaderWriter.getEntryMap().get("key"), equalTo("oldValue"));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.PutIfAbsentOutcome.HIT));
  }

  /**
   * Tests the effect of a {@link EhcacheWithLoaderWriter#putIfAbsent(Object, Object)} for
   * <ul>
   *   <li>key present in {@code Store}</li>
   *   <li>key present via {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testPutIfAbsentHasStoreEntryHasCacheLoaderWriterEntry() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "oldValue"));
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(Collections.singletonMap("key", "oldValue"));
    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(fakeLoaderWriter);

    assertThat(ehcache.putIfAbsent("key", "value"), is(equalTo("oldValue")));
    verify(this.store).computeIfAbsent(eq("key"), getAnyFunction());
    verifyZeroInteractions(this.resilienceStrategy);
    assertThat(fakeStore.getEntryMap().get("key"), equalTo("oldValue"));
    assertThat(fakeLoaderWriter.getEntryMap().get("key"), equalTo("oldValue"));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.PutIfAbsentOutcome.HIT));
  }

  /**
   * Tests the effect of a {@link EhcacheWithLoaderWriter#putIfAbsent(Object, Object)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   *   <li>{@code Store.computeIfAbsent} throws</li>
   *   <li>key present via {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testPutIfAbsentNoStoreEntryStoreAccessExceptionHasCacheLoaderWriterEntry() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.emptyMap());
    this.store = spy(fakeStore);
    doThrow(new StoreAccessException("")).when(this.store).computeIfAbsent(eq("key"), getAnyFunction());

    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(Collections.singletonMap("key", "oldValue"));
    this.cacheLoaderWriter = spy(fakeLoaderWriter);
    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    final InOrder ordered = inOrder(this.cacheLoaderWriter, this.resilienceStrategy);

    ehcache.putIfAbsent("key", "value");
    verify(this.store).computeIfAbsent(eq("key"), getAnyFunction());
    ordered.verify(this.resilienceStrategy).putIfAbsentFailure(eq("key"), eq("value"), any(StoreAccessException.class));
    ordered.verify(this.cacheLoaderWriter).load(eq("key"));
    assertThat(fakeLoaderWriter.getEntryMap().get("key"), equalTo("oldValue"));
    // Broken initial state: CacheLoaderWriter check omitted
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.PutIfAbsentOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link EhcacheWithLoaderWriter#putIfAbsent(Object, Object)} for
   * <ul>
   *   <li>key present in {@code Store}</li>
   *   <li>{@code Store.computeIfPresent} throws</li>
   *   <li>key present via {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testPutIfAbsentHasStoreEntryStoreAccessExceptionHasCacheLoaderWriterEntry() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "oldValue"));
    this.store = spy(fakeStore);
    doThrow(new StoreAccessException("")).when(this.store).computeIfAbsent(eq("key"), getAnyFunction());

    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(Collections.singletonMap("key", "oldValue"));
    this.cacheLoaderWriter = spy(fakeLoaderWriter);
    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    final InOrder ordered = inOrder(this.cacheLoaderWriter, this.resilienceStrategy);

    assertThat(ehcache.putIfAbsent("key", "value"), is("oldValue"));
    verify(this.store).computeIfAbsent(eq("key"), getAnyFunction());
    ordered.verify(this.resilienceStrategy).putIfAbsentFailure(eq("key"), eq("value"), any(StoreAccessException.class));
    ordered.verify(this.cacheLoaderWriter).load(eq("key"));
    assertThat(fakeLoaderWriter.getEntryMap().get("key"), equalTo("oldValue"));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.PutIfAbsentOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link EhcacheWithLoaderWriter#putIfAbsent(Object, Object)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   *   <li>{@code CacheLoaderWriter.write} throws</li>
   * </ul>
   */
  @Test
  public void testPutIfAbsentNoStoreEntryCacheLoaderWriterException() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.emptyMap());
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(Collections.emptyMap());
    this.cacheLoaderWriter = spy(fakeLoaderWriter);
    doThrow(new Exception()).when(this.cacheLoaderWriter).write("key", "value");
    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    try {
      ehcache.putIfAbsent("key", "value");
      fail();
    } catch (CacheWritingException e) {
      // Expected
    }
    verify(this.store).computeIfAbsent(eq("key"), getAnyFunction());
    verifyZeroInteractions(this.resilienceStrategy);
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.PutIfAbsentOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link EhcacheWithLoaderWriter#putIfAbsent(Object, Object)} for
   * <ul>
   *   <li>key present in {@code Store}</li>
   *   <li>{@code CacheLoaderWriter.write} throws</li>
   * </ul>
   */
  @Test
  public void testPutIfAbsentHasStoreEntryCacheLoaderWriterException() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "oldValue"));
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(Collections.singletonMap("key", "oldValue"));
    this.cacheLoaderWriter = spy(fakeLoaderWriter);
    doThrow(new Exception()).when(this.cacheLoaderWriter).write("key", "value");
    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    assertThat(ehcache.putIfAbsent("key", "value"), is(equalTo("oldValue")));
    verify(this.store).computeIfAbsent(eq("key"), getAnyFunction());
    verifyZeroInteractions(this.resilienceStrategy);
    assertThat(fakeStore.getEntryMap().get("key"), equalTo("oldValue"));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.PutIfAbsentOutcome.HIT));
  }

  /**
   * Tests the effect of a {@link EhcacheWithLoaderWriter#putIfAbsent(Object, Object)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   *   <li>{@code Store.computeIfPresent} throws</li>
   *   <li>{@code CacheLoaderWriter.write} throws</li>
   * </ul>
   */
  @Test
  public void testPutIfAbsentNoStoreEntryStoreAccessExceptionCacheLoaderWriterException() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.emptyMap());
    this.store = spy(fakeStore);
    doThrow(new StoreAccessException("")).when(this.store).computeIfAbsent(eq("key"), getAnyFunction());

    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(Collections.emptyMap());
    this.cacheLoaderWriter = spy(fakeLoaderWriter);
    doThrow(new Exception()).when(this.cacheLoaderWriter).write("key", "value");
    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    final InOrder ordered = inOrder(this.cacheLoaderWriter, this.resilienceStrategy);

    try {
      ehcache.putIfAbsent("key", "value");
      fail();
    } catch (CacheWritingException e) {
      // Expected
    }
    verify(this.store).computeIfAbsent(eq("key"), getAnyFunction());
    ordered.verify(this.resilienceStrategy).putIfAbsentFailure(eq("key"), eq("value"), any(StoreAccessException.class));
    ordered.verify(this.cacheLoaderWriter).write(eq("key"), eq("value"));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.PutIfAbsentOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link EhcacheWithLoaderWriter#putIfAbsent(Object, Object)} for
   * <ul>
   *   <li>key present in {@code Store}</li>
   *   <li>{@code Store.computeIfPresent} throws</li>
   *   <li>{@code CacheLoaderWriter.write} throws</li>
   * </ul>
   */
  @Test
  public void testPutIfAbsentHasStoreEntryStoreAccessExceptionCacheLoaderWriterException() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "oldValue"));
    this.store = spy(fakeStore);
    doThrow(new StoreAccessException("")).when(this.store).computeIfAbsent(eq("key"), getAnyFunction());

    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(Collections.emptyMap());
    this.cacheLoaderWriter = spy(fakeLoaderWriter);
    doThrow(new Exception()).when(this.cacheLoaderWriter).write("key", "value");
    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    final InOrder ordered = inOrder(this.cacheLoaderWriter, this.resilienceStrategy);

    try {
      ehcache.putIfAbsent("key", "value");
      fail();
    } catch (CacheWritingException e) {
      // Expected
    }
    verify(this.store).computeIfAbsent(eq("key"), getAnyFunction());
    ordered.verify(this.resilienceStrategy).putIfAbsentFailure(eq("key"), eq("value"), any(StoreAccessException.class));
    ordered.verify(this.cacheLoaderWriter).write(eq("key"), eq("value"));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.PutIfAbsentOutcome.FAILURE));
  }

  /**
   * Gets an initialized {@link EhcacheWithLoaderWriter Ehcache} instance using the
   * {@link org.ehcache.spi.loaderwriter.CacheLoaderWriter CacheLoaderWriter} provided.
   *
   * @param cacheLoaderWriter
   *    the {@code CacheLoaderWriter} to use; may be {@code null}
   *
   * @return a new {@code Ehcache} instance
   */
  private EhcacheWithLoaderWriter<String, String> getEhcache(final CacheLoaderWriter<String, String> cacheLoaderWriter) {
    return getEhcache(cacheLoaderWriter, ExpiryPolicy.NO_EXPIRY);
  }

  private EhcacheWithLoaderWriter<String, String> getEhcache(final CacheLoaderWriter<String, String> cacheLoaderWriter, ExpiryPolicy<? super String, ? super String> expiry) {
    CacheConfiguration<String, String> config = new BaseCacheConfiguration<>(String.class, String.class, null, null,
      expiry, ResourcePoolsHelper.createHeapOnlyPools());
    this.resilienceStrategy = spy(new RobustLoaderWriterResilienceStrategy<>(new DefaultRecoveryStore<>(this.store), cacheLoaderWriter));
    final EhcacheWithLoaderWriter<String, String> ehcache = new EhcacheWithLoaderWriter<>(config, this.store, resilienceStrategy, cacheLoaderWriter, cacheEventDispatcher, LoggerFactory
      .getLogger(EhcacheWithLoaderWriter.class + "-" + "EhcacheWithLoaderWriterBasicPutIfAbsentTest"));
    ehcache.init();
    assertThat("cache not initialized", ehcache.getStatus(), Matchers.is(Status.AVAILABLE));
    return ehcache;
  }
}
