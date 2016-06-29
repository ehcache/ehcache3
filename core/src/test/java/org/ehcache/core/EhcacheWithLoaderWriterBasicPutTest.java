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
import org.ehcache.core.statistics.CacheOperationOutcomes;
import org.ehcache.core.spi.store.StoreAccessException;
import org.ehcache.spi.loaderwriter.CacheWritingException;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.hamcrest.CoreMatchers;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.EnumSet;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

/**
 * Provides testing of basic PUT operations on an {@code EhcacheWithLoaderWriter}.
 *
 * @author Clifford W. Johnson
 */
public class EhcacheWithLoaderWriterBasicPutTest extends EhcacheBasicCrudBase {

  @Mock
  protected CacheLoaderWriter<String, String> cacheLoaderWriter;

  @Test
  public void testPutNullNull() {
    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    try {
      ehcache.put(null, null);
      fail();
    } catch (NullPointerException e) {
      // expected
    }
  }

  @Test
  public void testPutKeyNull() {
    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    try {
      ehcache.put("key", null);
      fail();
    } catch (NullPointerException e) {
      // expected
    }
  }

  @Test
  public void testPutNullValue() {
    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    try {
      ehcache.put(null, "value");
      fail();
    } catch (NullPointerException e) {
      // expected
    }
  }

  /**
   * Tests the effect of a {@link EhcacheWithLoaderWriter#put(Object, Object)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   * </ul>
   */
  @Test
  public void testPutNoStoreEntry() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.<String, String>emptyMap());
    this.store = spy(fakeStore);

    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    ehcache.put("key", "value");
    verify(this.store).compute(eq("key"), getAnyBiFunction());
    verifyZeroInteractions(this.spiedResilienceStrategy);
    assertThat(fakeStore.getEntryMap().get("key"), equalTo("value"));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.PutOutcome.PUT));
  }

  /**
   * Tests the effect of a {@link EhcacheWithLoaderWriter#put(Object, Object)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   *   <li>key not present via {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testPutNoStoreEntryNoCacheLoaderWriterEntry() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.<String, String>emptyMap());
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeWriter = new FakeCacheLoaderWriter(Collections.<String, String>emptyMap());
    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(fakeWriter);

    ehcache.put("key", "value");
    verify(this.store).compute(eq("key"), getAnyBiFunction());
    verifyZeroInteractions(this.spiedResilienceStrategy);
    assertThat(fakeStore.getEntryMap().get("key"), equalTo("value"));
    assertThat(fakeWriter.getEntryMap().get("key"), equalTo("value"));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.PutOutcome.PUT));
  }

  /**
   * Tests the effect of a {@link EhcacheWithLoaderWriter#put(Object, Object)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   *   <li>key present via {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testPutNoStoreEntryHasCacheLoaderWriterEntry() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.<String, String>emptyMap());
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeWriter = new FakeCacheLoaderWriter(Collections.singletonMap("key", "oldValue"));
    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(fakeWriter);

    ehcache.put("key", "value");
    verify(this.store).compute(eq("key"), getAnyBiFunction());
    verifyZeroInteractions(this.spiedResilienceStrategy);
    assertThat(fakeStore.getEntryMap().get("key"), equalTo("value"));
    assertThat(fakeWriter.getEntryMap().get("key"), equalTo("value"));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.PutOutcome.PUT));
  }

  /**
   * Tests the effect of a {@link EhcacheWithLoaderWriter#put(Object, Object)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   *   <li>{@code CacheLoaderWriter.write} throws</li>
   * </ul>
   */
  @Test
  public void testPutNoStoreEntryCacheLoaderWriterException() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.<String, String>emptyMap());
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(Collections.singletonMap("key", "oldValue"));
    this.cacheLoaderWriter = spy(fakeLoaderWriter);
    doThrow(new Exception()).when(this.cacheLoaderWriter).write("key", "value");
    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    try {
      ehcache.put("key", "value");
      fail();
    } catch (CacheWritingException e) {
      // Expected
    }
    verify(this.store).compute(eq("key"), getAnyBiFunction());
    verifyZeroInteractions(this.spiedResilienceStrategy);
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.PutOutcome.class));
  }

  /**
   * Tests the effect of a {@link EhcacheWithLoaderWriter#put(Object, Object)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   *   <li>{@code Store.compute} throws</li>
   * </ul>
   */
  @Test
  public void testPutNoStoreEntryStoreAccessException() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.<String, String>emptyMap());
    this.store = spy(fakeStore);
    doThrow(new StoreAccessException("")).when(this.store).compute(eq("key"), getAnyBiFunction());

    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    ehcache.put("key", "value");
    verify(this.store).compute(eq("key"), getAnyBiFunction());
    verify(this.spiedResilienceStrategy).putFailure(eq("key"), eq("value"), any(StoreAccessException.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.PutOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link EhcacheWithLoaderWriter#put(Object, Object)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   *   <li>{@code Store.compute} throws</li>
   *   <li>key not present via {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testPutNoStoreEntryStoreAccessExceptionNoCacheLoaderWriterEntry() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.<String, String>emptyMap());
    this.store = spy(fakeStore);
    doThrow(new StoreAccessException("")).when(this.store).compute(eq("key"), getAnyBiFunction());

    final FakeCacheLoaderWriter fakeWriter = new FakeCacheLoaderWriter(Collections.<String, String>emptyMap());
    this.cacheLoaderWriter = spy(fakeWriter);
    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    final InOrder ordered = inOrder(this.cacheLoaderWriter, this.spiedResilienceStrategy);

    ehcache.put("key", "value");
    verify(this.store).compute(eq("key"), getAnyBiFunction());
    ordered.verify(this.cacheLoaderWriter).write(eq("key"), eq("value"));
    ordered.verify(this.spiedResilienceStrategy).putFailure(eq("key"), eq("value"), any(StoreAccessException.class));
    assertThat(fakeWriter.getEntryMap().get("key"), equalTo("value"));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.PutOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link EhcacheWithLoaderWriter#put(Object, Object)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   *   <li>{@code Store.compute} throws</li>
   *   <li>key present via {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testPutNoStoreEntryStoreAccessExceptionHasCacheLoaderWriterEntry() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.<String, String>emptyMap());
    this.store = spy(fakeStore);
    doThrow(new StoreAccessException("")).when(this.store).compute(eq("key"), getAnyBiFunction());

    final FakeCacheLoaderWriter fakeWriter = new FakeCacheLoaderWriter(Collections.singletonMap("key", "oldValue"));
    this.cacheLoaderWriter = spy(fakeWriter);
    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    final InOrder ordered = inOrder(this.cacheLoaderWriter, this.spiedResilienceStrategy);

    ehcache.put("key", "value");
    verify(this.store).compute(eq("key"), getAnyBiFunction());
    ordered.verify(this.cacheLoaderWriter).write(eq("key"), eq("value"));
    ordered.verify(this.spiedResilienceStrategy).putFailure(eq("key"), eq("value"), any(StoreAccessException.class));
    assertThat(fakeWriter.getEntryMap().get("key"), equalTo("value"));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.PutOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link EhcacheWithLoaderWriter#put(Object, Object)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   *   <li>{@code Store.compute} throws</li>
   *   <li>{@code CacheLoaderWriter.write} throws</li>
   * </ul>
   */
  @Test
  public void testPutNoStoreEntryStoreAccessExceptionCacheLoaderWriterException() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.<String, String>emptyMap());
    this.store = spy(fakeStore);
    doThrow(new StoreAccessException("")).when(this.store).compute(eq("key"), getAnyBiFunction());

    final FakeCacheLoaderWriter fakeWriter = new FakeCacheLoaderWriter(Collections.singletonMap("key", "oldValue"));
    this.cacheLoaderWriter = spy(fakeWriter);
    doThrow(new Exception()).when(this.cacheLoaderWriter).write("key", "value");
    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    final InOrder ordered = inOrder(this.cacheLoaderWriter, this.spiedResilienceStrategy);

    try {
      ehcache.put("key", "value");
      fail();
    } catch (CacheWritingException e) {
      // Expected
    }
    verify(this.store).compute(eq("key"), getAnyBiFunction());
    ordered.verify(this.cacheLoaderWriter).write(eq("key"), eq("value"));
    ordered.verify(this.spiedResilienceStrategy)
        .putFailure(eq("key"), eq("value"), any(StoreAccessException.class), any(CacheWritingException.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.PutOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link EhcacheWithLoaderWriter#put(Object, Object)} for
   * <ul>
   *   <li>key present in {@code Store}</li>
   * </ul>
   */
  @Test
  public void testPutHasStoreEntry() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "oldValue"));
    this.store = spy(fakeStore);

    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    ehcache.put("key", "value");
    verify(this.store).compute(eq("key"), getAnyBiFunction());
    verifyZeroInteractions(this.spiedResilienceStrategy);
    assertThat(fakeStore.getEntryMap().get("key"), equalTo("value"));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.PutOutcome.UPDATED));
  }

  /**
   * Tests the effect of a {@link EhcacheWithLoaderWriter#put(Object, Object)} for
   * <ul>
   *   <li>key present in {@code Store}</li>
   *   <li>key not present via {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testPutHasStoreEntryNoCacheLoaderWriterEntry() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "oldValue"));
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeWriter = new FakeCacheLoaderWriter(Collections.<String, String>emptyMap());
    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(fakeWriter);

    ehcache.put("key", "value");
    verify(this.store).compute(eq("key"), getAnyBiFunction());
    verifyZeroInteractions(this.spiedResilienceStrategy);
    assertThat(fakeStore.getEntryMap().get("key"), equalTo("value"));
    assertThat(fakeWriter.getEntryMap().get("key"), equalTo("value"));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.PutOutcome.UPDATED));
  }

  /**
   * Tests the effect of a {@link EhcacheWithLoaderWriter#put(Object, Object)} for
   * <ul>
   *   <li>key present in {@code Store}</li>
   *   <li>key present via {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testPutHasStoreEntryHasCacheLoaderWriterEntry() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "oldValue"));
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeWriter = new FakeCacheLoaderWriter(Collections.singletonMap("key", "oldValue"));
    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(fakeWriter);

    ehcache.put("key", "value");
    verify(this.store).compute(eq("key"), getAnyBiFunction());
    verifyZeroInteractions(this.spiedResilienceStrategy);
    assertThat(fakeStore.getEntryMap().get("key"), equalTo("value"));
    assertThat(fakeWriter.getEntryMap().get("key"), equalTo("value"));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.PutOutcome.UPDATED));
  }

  /**
   * Tests the effect of a {@link EhcacheWithLoaderWriter#put(Object, Object)} for
   * <ul>
   *   <li>key present in {@code Store}</li>
   *   <li>{@code CacheLoaderWriter.write} throws</li>
   * </ul>
   */
  @Test
  public void testPutHasStoreEntryCacheLoaderWriterException() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "oldValue"));
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeWriter = new FakeCacheLoaderWriter(Collections.singletonMap("key", "oldValue"));
    this.cacheLoaderWriter = spy(fakeWriter);
    doThrow(new Exception()).when(this.cacheLoaderWriter).write("key", "value");
    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    try {
      ehcache.put("key", "value");
      fail();
    } catch (CacheWritingException e) {
      // Expected
    }
    verify(this.store).compute(eq("key"), getAnyBiFunction());
    verifyZeroInteractions(this.spiedResilienceStrategy);
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.PutOutcome.class));
  }

  /**
   * Tests the effect of a {@link EhcacheWithLoaderWriter#put(Object, Object)} for
   * <ul>
   *   <li>key present in {@code Store}</li>
   *   <li>{@code Store.compute} throws</li>
   * </ul>
   */
  @Test
  public void testPutHasStoreEntryStoreAccessException() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "oldValue"));
    this.store = spy(fakeStore);
    doThrow(new StoreAccessException("")).when(this.store).compute(eq("key"), getAnyBiFunction());

    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    ehcache.put("key", "value");
    verify(this.store).compute(eq("key"), getAnyBiFunction());
    verify(this.spiedResilienceStrategy).putFailure(eq("key"), eq("value"), any(StoreAccessException.class));
    assertThat(fakeStore.getEntryMap().containsKey("key"), is(false));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.PutOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link EhcacheWithLoaderWriter#put(Object, Object)} for
   * <ul>
   *   <li>key present in {@code Store}</li>
   *   <li>{@code Store.compute} throws</li>
   *   <li>key not present via {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testPutHasStoreEntryStoreAccessExceptionNoCacheLoaderWriterEntry() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "oldValue"));
    this.store = spy(fakeStore);
    doThrow(new StoreAccessException("")).when(this.store).compute(eq("key"), getAnyBiFunction());

    final FakeCacheLoaderWriter fakeWriter = new FakeCacheLoaderWriter(Collections.<String, String>emptyMap());
    this.cacheLoaderWriter = spy(fakeWriter);
    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    final InOrder ordered = inOrder(this.cacheLoaderWriter, this.spiedResilienceStrategy);

    ehcache.put("key", "value");
    verify(this.store).compute(eq("key"), getAnyBiFunction());
    ordered.verify(this.cacheLoaderWriter).write(eq("key"), eq("value"));
    ordered.verify(this.spiedResilienceStrategy).putFailure(eq("key"), eq("value"), any(StoreAccessException.class));
    assertThat(fakeWriter.getEntryMap().get("key"), equalTo("value"));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.PutOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link EhcacheWithLoaderWriter#put(Object, Object)} for
   * <ul>
   *   <li>key present in {@code Store}</li>
   *   <li>{@code Store.compute} throws</li>
   *   <li>key present via {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testPutHasStoreEntryStoreAccessExceptionHasCacheLoaderWriterEntry() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "oldValue"));
    this.store = spy(fakeStore);
    doThrow(new StoreAccessException("")).when(this.store).compute(eq("key"), getAnyBiFunction());

    final FakeCacheLoaderWriter fakeWriter = new FakeCacheLoaderWriter(Collections.singletonMap("key", "oldValue"));
    this.cacheLoaderWriter = spy(fakeWriter);
    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    final InOrder ordered = inOrder(this.cacheLoaderWriter, this.spiedResilienceStrategy);

    ehcache.put("key", "value");
    verify(this.store).compute(eq("key"), getAnyBiFunction());
    ordered.verify(this.cacheLoaderWriter).write(eq("key"), eq("value"));
    ordered.verify(this.spiedResilienceStrategy).putFailure(eq("key"), eq("value"), any(StoreAccessException.class));
    assertThat(fakeWriter.getEntryMap().get("key"), equalTo("value"));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.PutOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link EhcacheWithLoaderWriter#put(Object, Object)} for
   * <ul>
   *   <li>key present in {@code Store}</li>
   *   <li>{@code Store.compute} throws</li>
   *   <li>{@code CacheLoaderWriter.write} throws</li>
   * </ul>
   */
  @Test
  public void testPutHasStoreEntryStoreAccessExceptionCacheLoaderWriterException() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "oldValue"));
    this.store = spy(fakeStore);
    doThrow(new StoreAccessException("")).when(this.store).compute(eq("key"), getAnyBiFunction());

    final FakeCacheLoaderWriter fakeWriter = new FakeCacheLoaderWriter(Collections.singletonMap("key", "oldValue"));
    this.cacheLoaderWriter = spy(fakeWriter);
    doThrow(new Exception()).when(this.cacheLoaderWriter).write("key", "value");
    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    final InOrder ordered = inOrder(this.cacheLoaderWriter, this.spiedResilienceStrategy);

    try {
      ehcache.put("key", "value");
      fail();
    } catch (CacheWritingException e) {
      // Expected
    }
    verify(this.store).compute(eq("key"), getAnyBiFunction());
    ordered.verify(this.cacheLoaderWriter).write(eq("key"), eq("value"));
    ordered.verify(this.spiedResilienceStrategy)
        .putFailure(eq("key"), eq("value"), any(StoreAccessException.class), any(CacheWritingException.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.PutOutcome.FAILURE));
  }

  /**
   * Gets an initialized {@link EhcacheWithLoaderWriter Ehcache} instance using the
   * {@link org.ehcache.spi.loaderwriter.CacheLoaderWriter CacheLoaderWriter} provided.
   *
   * @param cacheLoaderWriter
   *    the {@code CacheLoaderWriter} to use; may be {@code null}
   *
   * @return a new {@code EhcacheWithLoaderWriter} instance
   */
  private EhcacheWithLoaderWriter<String, String> getEhcache(final CacheLoaderWriter<String, String> cacheLoaderWriter) {
    return getEhcache(cacheLoaderWriter, CACHE_CONFIGURATION);
  }

  private EhcacheWithLoaderWriter<String, String> getEhcache(CacheLoaderWriter<String, String> cacheLoaderWriter, CacheConfiguration<String, String> config) {
    final EhcacheWithLoaderWriter<String, String> ehcache = new EhcacheWithLoaderWriter<String, String>(config, this.store, cacheLoaderWriter, cacheEventDispatcher, LoggerFactory.getLogger(EhcacheWithLoaderWriter.class + "-" + "EhcacheWithLoaderWriterBasicPutTest"));
    ehcache.init();
    assertThat("cache not initialized", ehcache.getStatus(), CoreMatchers.is(Status.AVAILABLE));
    this.spiedResilienceStrategy = this.setResilienceStrategySpy(ehcache);
    return ehcache;
  }
}
