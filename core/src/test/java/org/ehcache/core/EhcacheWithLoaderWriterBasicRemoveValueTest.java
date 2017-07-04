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
import org.ehcache.core.spi.store.StoreAccessException;
import org.ehcache.spi.loaderwriter.CacheWritingException;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.ehcache.core.statistics.CacheOperationOutcomes;
import org.hamcrest.CoreMatchers;
import org.junit.Test;
import org.mockito.Mock;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.EnumSet;

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
 * Provides testing of basic REMOVE(key, value) operations on an {@code EhcacheWithLoaderWriter}.
 *
 * @author Clifford W. Johnson
 */
public class EhcacheWithLoaderWriterBasicRemoveValueTest extends EhcacheBasicCrudBase {

  @Mock
  protected CacheLoaderWriter<String, String> cacheLoaderWriter;

  @Test
  public void testRemoveNullNull() {
    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    try {
      ehcache.remove(null, null);
      fail();
    } catch (NullPointerException e) {
      // expected
    }
  }

  @Test
  public void testRemoveKeyNull() throws Exception {
    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    try {
      ehcache.remove("key", null);
      fail();
    } catch (NullPointerException e) {
      // expected
    }
  }

  @Test
  public void testRemoveNullValue() throws Exception {
    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    try {
      ehcache.remove(null, "value");
      fail();
    } catch (NullPointerException e) {
      // expected
    }
  }

  /**
   * Tests the effect of a {@link EhcacheWithLoaderWriter#remove(Object, Object)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   * </ul>
   */
  @Test
  public void testRemoveValueNoStoreEntry() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.<String, String>emptyMap());
    this.store = spy(fakeStore);

    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    assertFalse(ehcache.remove("key", "value"));
    verify(this.store).compute(eq("key"), getAnyBiFunction(), getBooleanNullaryFunction());
    verifyZeroInteractions(this.spiedResilienceStrategy);
    assertThat(fakeStore.getEntryMap().containsKey("key"), is(false));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ConditionalRemoveOutcome.FAILURE_KEY_MISSING));
  }

  /**
   * Tests the effect of a {@link EhcacheWithLoaderWriter#remove(Object, Object)} for
   * <ul>
   *   <li>key with unequal value in {@code Store}</li>
   * </ul>
   */
  @Test
  public void testRemoveValueUnequalStoreEntry() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "unequalValue"));
    this.store = spy(fakeStore);

    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    assertFalse(ehcache.remove("key", "value"));
    verify(this.store).compute(eq("key"), getAnyBiFunction(), getBooleanNullaryFunction());
    verifyZeroInteractions(this.spiedResilienceStrategy);
    assertThat(fakeStore.getEntryMap().get("key"), is(equalTo("unequalValue")));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ConditionalRemoveOutcome.FAILURE_KEY_PRESENT));
  }

  /**
   * Tests the effect of a {@link EhcacheWithLoaderWriter#remove(Object, Object)} for
   * <ul>
   *   <li>key with equal value in {@code Store}</li>
   * </ul>
   */
  @Test
  public void testRemoveValueEqualStoreEntry() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "value"));
    this.store = spy(fakeStore);

    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    assertTrue(ehcache.remove("key", "value"));
    verify(this.store).compute(eq("key"), getAnyBiFunction(), getBooleanNullaryFunction());
    verifyZeroInteractions(this.spiedResilienceStrategy);
    assertThat(fakeStore.getEntryMap().containsKey("key"), is(false));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ConditionalRemoveOutcome.SUCCESS));
  }

  /**
   * Tests the effect of a {@link EhcacheWithLoaderWriter#remove(Object, Object)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   *   <li>>{@code Store.compute} throws</li>
   * </ul>
   */
  @Test
  public void testRemoveValueNoStoreEntryStoreAccessException() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.<String, String>emptyMap());
    this.store = spy(fakeStore);
    doThrow(new StoreAccessException("")).when(this.store).compute(eq("key"), getAnyBiFunction(), getBooleanNullaryFunction());

    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    ehcache.remove("key", "value");
    verify(this.store).compute(eq("key"), getAnyBiFunction(), getBooleanNullaryFunction());
    verify(this.spiedResilienceStrategy)
        .removeFailure(eq("key"), eq("value"), any(StoreAccessException.class), eq(false));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ConditionalRemoveOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link EhcacheWithLoaderWriter#remove(Object, Object)} for
   * <ul>
   *   <li>key with unequal value present in {@code Store}</li>
   *   <li>>{@code Store.compute} throws</li>
   * </ul>
   */
  @Test
  public void testRemoveValueUnequalStoreEntryStoreAccessException() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "unequalValue"));
    this.store = spy(fakeStore);
    doThrow(new StoreAccessException("")).when(this.store).compute(eq("key"), getAnyBiFunction(), getBooleanNullaryFunction());

    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    ehcache.remove("key", "value");
    verify(this.store).compute(eq("key"), getAnyBiFunction(), getBooleanNullaryFunction());
    verify(this.spiedResilienceStrategy)
        .removeFailure(eq("key"), eq("value"), any(StoreAccessException.class), eq(false));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ConditionalRemoveOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link EhcacheWithLoaderWriter#remove(Object, Object)} for
   * <ul>
   *   <li>key with equal value present in {@code Store}</li>
   *   <li>>{@code Store.compute} throws</li>
   * </ul>
   */
  @Test
  public void testRemoveValueEqualStoreEntryStoreAccessException() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "value"));
    this.store = spy(fakeStore);
    doThrow(new StoreAccessException("")).when(this.store).compute(eq("key"), getAnyBiFunction(), getBooleanNullaryFunction());

    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    ehcache.remove("key", "value");
    verify(this.store).compute(eq("key"), getAnyBiFunction(), getBooleanNullaryFunction());
    verify(this.spiedResilienceStrategy)
        .removeFailure(eq("key"), eq("value"), any(StoreAccessException.class), eq(false));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ConditionalRemoveOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link EhcacheWithLoaderWriter#remove(Object, Object)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   *   <li>key not present via {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testRemoveValueNoStoreEntryNoCacheLoaderWriterEntry() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.<String, String>emptyMap());
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeWriter = new FakeCacheLoaderWriter(Collections.<String, String>emptyMap());
    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(fakeWriter);

    assertFalse(ehcache.remove("key", "value"));
    verify(this.store).compute(eq("key"), getAnyBiFunction(), getBooleanNullaryFunction());
    verifyZeroInteractions(this.spiedResilienceStrategy);
    assertThat(fakeStore.getEntryMap().containsKey("key"), is(false));
    assertThat(fakeWriter.getEntryMap().containsKey("key"), is(false));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ConditionalRemoveOutcome.FAILURE_KEY_MISSING));
  }

  /**
   * Tests the effect of a {@link EhcacheWithLoaderWriter#remove(Object, Object)} for
   * <ul>
   *   <li>key with unequal value present in {@code Store}</li>
   *   <li>key not present via {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testRemoveValueUnequalStoreEntryNoCacheLoaderWriterEntry() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "unequalValue"));
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeWriter = new FakeCacheLoaderWriter(Collections.<String, String>emptyMap());
    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(fakeWriter);

    assertFalse(ehcache.remove("key", "value"));
    verify(this.store).compute(eq("key"), getAnyBiFunction(), getBooleanNullaryFunction());
    verifyZeroInteractions(this.spiedResilienceStrategy);
    assertThat(fakeStore.getEntryMap().get("key"), is(equalTo("unequalValue")));
    // Broken initial state: CacheLoaderWriter check omitted
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ConditionalRemoveOutcome.FAILURE_KEY_PRESENT));
  }

  /**
   * Tests the effect of a {@link EhcacheWithLoaderWriter#remove(Object, Object)} for
   * <ul>
   *   <li>key with equal value present in {@code Store}</li>
   *   <li>key not present via {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testRemoveValueEqualStoreEntryNoCacheLoaderWriterEntry() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "value"));
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeWriter = new FakeCacheLoaderWriter(Collections.<String, String>emptyMap());
    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(fakeWriter);

    assertTrue(ehcache.remove("key", "value"));
    verify(this.store).compute(eq("key"), getAnyBiFunction(), getBooleanNullaryFunction());
    verifyZeroInteractions(this.spiedResilienceStrategy);
    assertThat(fakeStore.getEntryMap().containsKey("key"), is(false));
    // Broken initial state: CacheLoaderWriter check omitted
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ConditionalRemoveOutcome.SUCCESS));
  }

  /**
   * Tests the effect of a {@link EhcacheWithLoaderWriter#remove(Object, Object)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   *   <li>>{@code Store.compute} throws</li>
   *   <li>key not present via {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testRemoveValueNoStoreEntryStoreAccessExceptionNoCacheLoaderWriterEntry() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.<String, String>emptyMap());
    this.store = spy(fakeStore);
    doThrow(new StoreAccessException("")).when(this.store).compute(eq("key"), getAnyBiFunction(), getBooleanNullaryFunction());

    final FakeCacheLoaderWriter fakeWriter = new FakeCacheLoaderWriter(Collections.<String, String>emptyMap());
    this.cacheLoaderWriter = spy(fakeWriter);
    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    ehcache.remove("key", "value");
    verify(this.store).compute(eq("key"), getAnyBiFunction(), getBooleanNullaryFunction());
    verify(this.spiedResilienceStrategy)
        .removeFailure(eq("key"), eq("value"), any(StoreAccessException.class), eq(false));
    assertThat(fakeWriter.getEntryMap().containsKey("key"), is(false));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ConditionalRemoveOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link EhcacheWithLoaderWriter#remove(Object, Object)} for
   * <ul>
   *   <li>key with unequal value present in {@code Store}</li>
   *   <li>>{@code Store.compute} throws</li>
   *   <li>key not present via {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testRemoveValueUnequalStoreEntryStoreAccessExceptionNoCacheLoaderWriterEntry() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "unequalValue"));
    this.store = spy(fakeStore);
    doThrow(new StoreAccessException("")).when(this.store).compute(eq("key"), getAnyBiFunction(), getBooleanNullaryFunction());

    final FakeCacheLoaderWriter fakeWriter = new FakeCacheLoaderWriter(Collections.<String, String>emptyMap());
    this.cacheLoaderWriter = spy(fakeWriter);
    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    ehcache.remove("key", "value");
    verify(this.store).compute(eq("key"), getAnyBiFunction(), getBooleanNullaryFunction());
    verify(this.spiedResilienceStrategy)
        .removeFailure(eq("key"), eq("value"), any(StoreAccessException.class), eq(false));
    // Broken initial state: CacheLoaderWriter check omitted
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ConditionalRemoveOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link EhcacheWithLoaderWriter#remove(Object, Object)} for
   * <ul>
   *   <li>key with equal value present in {@code Store}</li>
   *   <li>>{@code Store.compute} throws</li>
   *   <li>key not present via {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testRemoveValueEqualStoreEntryStoreAccessExceptionNoCacheLoaderWriterEntry() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "value"));
    this.store = spy(fakeStore);
    doThrow(new StoreAccessException("")).when(this.store).compute(eq("key"), getAnyBiFunction(), getBooleanNullaryFunction());

    final FakeCacheLoaderWriter fakeWriter = new FakeCacheLoaderWriter(Collections.<String, String>emptyMap());
    this.cacheLoaderWriter = spy(fakeWriter);
    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    ehcache.remove("key", "value");
    verify(this.store).compute(eq("key"), getAnyBiFunction(), getBooleanNullaryFunction());
    verify(this.spiedResilienceStrategy)
        .removeFailure(eq("key"), eq("value"), any(StoreAccessException.class), eq(false));
    // Broken initial state: CacheLoaderWriter check omitted
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ConditionalRemoveOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link EhcacheWithLoaderWriter#remove(Object, Object)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   *   <li>key with unequal value present via {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testRemoveValueNoStoreEntryUnequalCacheLoaderWriterEntry() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.<String, String>emptyMap());
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeWriter = new FakeCacheLoaderWriter(Collections.singletonMap("key", "unequalValue"));
    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(fakeWriter);

    assertFalse(ehcache.remove("key", "value"));
    verify(this.store).compute(eq("key"), getAnyBiFunction(), getBooleanNullaryFunction());
    verifyZeroInteractions(this.spiedResilienceStrategy);
    assertThat(fakeStore.getEntryMap().get("key"), is("unequalValue"));
    assertThat(fakeWriter.getEntryMap().get("key"), is("unequalValue"));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ConditionalRemoveOutcome.FAILURE_KEY_PRESENT));
  }

  /**
   * Tests the effect of a {@link EhcacheWithLoaderWriter#remove(Object, Object)} for
   * <ul>
   *   <li>key with unequal value present in {@code Store}</li>
   *   <li>key with unequal value present via {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testRemoveValueUnequalStoreEntryUnequalCacheLoaderWriterEntry() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "unequalValue"));
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeWriter = new FakeCacheLoaderWriter(Collections.singletonMap("key", "unequalValue"));
    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(fakeWriter);

    assertFalse(ehcache.remove("key", "value"));
    verify(this.store).compute(eq("key"), getAnyBiFunction(), getBooleanNullaryFunction());
    verifyZeroInteractions(this.spiedResilienceStrategy);
    assertThat(fakeStore.getEntryMap().get("key"), is(equalTo("unequalValue")));
    assertThat(fakeWriter.getEntryMap().get("key"), is(equalTo("unequalValue")));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ConditionalRemoveOutcome.FAILURE_KEY_PRESENT));
  }

  /**
   * Tests the effect of a {@link EhcacheWithLoaderWriter#remove(Object, Object)} for
   * <ul>
   *   <li>key with equal value present in {@code Store}</li>
   *   <li>key with unequal value present via {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testRemoveValueEqualStoreEntryUnequalCacheLoaderWriterEntry() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "value"));
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeWriter = new FakeCacheLoaderWriter(Collections.singletonMap("key", "unequalValue"));
    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(fakeWriter);

    assertTrue(ehcache.remove("key", "value"));
    verify(this.store).compute(eq("key"), getAnyBiFunction(), getBooleanNullaryFunction());
    verifyZeroInteractions(this.spiedResilienceStrategy);
    assertThat(fakeStore.getEntryMap().containsKey("key"), is(false));
    // Broken initial state: CacheLoaderWriter check omitted
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ConditionalRemoveOutcome.SUCCESS));
  }

  /**
   * Tests the effect of a {@link EhcacheWithLoaderWriter#remove(Object, Object)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   *   <li>>{@code Store.compute} throws</li>
   *   <li>key with unequal value present via {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testRemoveValueNoStoreEntryStoreAccessExceptionUnequalCacheLoaderWriterEntry() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.<String, String>emptyMap());
    this.store = spy(fakeStore);
    doThrow(new StoreAccessException("")).when(this.store).compute(eq("key"), getAnyBiFunction(), getBooleanNullaryFunction());

    final FakeCacheLoaderWriter fakeWriter = new FakeCacheLoaderWriter(Collections.singletonMap("key", "unequalValue"));
    this.cacheLoaderWriter = spy(fakeWriter);
    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    ehcache.remove("key", "value");
    verify(this.store).compute(eq("key"), getAnyBiFunction(), getBooleanNullaryFunction());
    verify(this.spiedResilienceStrategy)
        .removeFailure(eq("key"), eq("value"), any(StoreAccessException.class), eq(false));
    assertThat(fakeWriter.getEntryMap().get("key"), is(equalTo("unequalValue")));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ConditionalRemoveOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link EhcacheWithLoaderWriter#remove(Object, Object)} for
   * <ul>
   *   <li>key with unequal value present in {@code Store}</li>
   *   <li>>{@code Store.compute} throws</li>
   *   <li>key with unequal value present via {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testRemoveValueUnequalStoreEntryStoreAccessExceptionUnequalCacheLoaderWriterEntry() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "unequalValue"));
    this.store = spy(fakeStore);
    doThrow(new StoreAccessException("")).when(this.store).compute(eq("key"), getAnyBiFunction(), getBooleanNullaryFunction());

    final FakeCacheLoaderWriter fakeWriter = new FakeCacheLoaderWriter(Collections.singletonMap("key", "unequalValue"));
    this.cacheLoaderWriter = spy(fakeWriter);
    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    assertThat(ehcache.remove("key", "value"), is(false));
    verify(this.store).compute(eq("key"), getAnyBiFunction(), getBooleanNullaryFunction());
    verify(this.spiedResilienceStrategy)
        .removeFailure(eq("key"), eq("value"), any(StoreAccessException.class), eq(false));
    assertThat(fakeWriter.getEntryMap().get("key"), is(equalTo("unequalValue")));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ConditionalRemoveOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link EhcacheWithLoaderWriter#remove(Object, Object)} for
   * <ul>
   *   <li>key with equal value present in {@code Store}</li>
   *   <li>>{@code Store.compute} throws</li>
   *   <li>key with unequal value present via {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testRemoveValueEqualStoreEntryStoreAccessExceptionUnequalCacheLoaderWriterEntry() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "value"));
    this.store = spy(fakeStore);
    doThrow(new StoreAccessException("")).when(this.store).compute(eq("key"), getAnyBiFunction(), getBooleanNullaryFunction());

    final FakeCacheLoaderWriter fakeWriter = new FakeCacheLoaderWriter(Collections.singletonMap("key", "unequalValue"));
    this.cacheLoaderWriter = spy(fakeWriter);
    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    ehcache.remove("key", "value");
    verify(this.store).compute(eq("key"), getAnyBiFunction(), getBooleanNullaryFunction());
    verify(this.spiedResilienceStrategy)
        .removeFailure(eq("key"), eq("value"), any(StoreAccessException.class), eq(false));
    // Broken initial state: CacheLoaderWriter check omitted
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ConditionalRemoveOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link EhcacheWithLoaderWriter#remove(Object, Object)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   *   <li>key with equal value present via {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testRemoveValueNoStoreEntryEqualCacheLoaderWriterEntry() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.<String, String>emptyMap());
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeWriter = new FakeCacheLoaderWriter(Collections.singletonMap("key", "value"));
    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(fakeWriter);

    assertThat(ehcache.remove("key", "value"), is(true));
    verify(this.store).compute(eq("key"), getAnyBiFunction(), getBooleanNullaryFunction());
    verifyZeroInteractions(this.spiedResilienceStrategy);
    assertThat(fakeStore.getEntryMap().containsKey("key"), is(false));
    assertThat(fakeWriter.getEntryMap().containsKey("key"), is(false));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ConditionalRemoveOutcome.SUCCESS));
  }

  /**
   * Tests the effect of a {@link EhcacheWithLoaderWriter#remove(Object, Object)} for
   * <ul>
   *   <li>key with unequal value present in {@code Store}</li>
   *   <li>key with equal value present via {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testRemoveValueUnequalStoreEntryEqualCacheLoaderWriterEntry() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "unequalValue"));
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeWriter = new FakeCacheLoaderWriter(Collections.singletonMap("key", "value"));
    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(fakeWriter);

    assertFalse(ehcache.remove("key", "value"));
    verify(this.store).compute(eq("key"), getAnyBiFunction(), getBooleanNullaryFunction());
    verifyZeroInteractions(this.spiedResilienceStrategy);
    assertThat(fakeStore.getEntryMap().get("key"), is(equalTo("unequalValue")));
    // Broken initial state: CacheLoaderWriter check omitted
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ConditionalRemoveOutcome.FAILURE_KEY_PRESENT));
  }

  /**
   * Tests the effect of a {@link EhcacheWithLoaderWriter#remove(Object, Object)} for
   * <ul>
   *   <li>key with equal value present in {@code Store}</li>
   *   <li>key with equal value present via {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testRemoveValueEqualStoreEntryEqualCacheLoaderWriterEntry() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "value"));
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeWriter = new FakeCacheLoaderWriter(Collections.singletonMap("key", "value"));
    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(fakeWriter);

    assertTrue(ehcache.remove("key", "value"));
    verify(this.store).compute(eq("key"), getAnyBiFunction(), getBooleanNullaryFunction());
    verifyZeroInteractions(this.spiedResilienceStrategy);
    assertThat(fakeStore.getEntryMap().containsKey("key"), is(false));
    assertThat(fakeWriter.getEntryMap().containsKey("key"), is(false));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ConditionalRemoveOutcome.SUCCESS));
  }

  /**
   * Tests the effect of a {@link EhcacheWithLoaderWriter#remove(Object, Object)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   *   <li>>{@code Store.compute} throws</li>
   *   <li>key with equal value present via {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testRemoveValueNoStoreEntryStoreAccessExceptionEqualCacheLoaderWriterEntry() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.<String, String>emptyMap());
    this.store = spy(fakeStore);
    doThrow(new StoreAccessException("")).when(this.store).compute(eq("key"), getAnyBiFunction(), getBooleanNullaryFunction());

    final FakeCacheLoaderWriter fakeWriter = new FakeCacheLoaderWriter(Collections.singletonMap("key", "value"));
    this.cacheLoaderWriter = spy(fakeWriter);
    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    assertThat(ehcache.remove("key", "value"), is(true));
    verify(this.store).compute(eq("key"), getAnyBiFunction(), getBooleanNullaryFunction());
    verify(this.spiedResilienceStrategy)
        .removeFailure(eq("key"), eq("value"), any(StoreAccessException.class), eq(true));
    assertThat(fakeWriter.getEntryMap().containsKey("key"), is(false));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ConditionalRemoveOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link EhcacheWithLoaderWriter#remove(Object, Object)} for
   * <ul>
   *   <li>key with unequal value present in {@code Store}</li>
   *   <li>>{@code Store.compute} throws</li>
   *   <li>key with equal value present via {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testRemoveValueUnequalStoreEntryStoreAccessExceptionEqualCacheLoaderWriterEntry() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "unequalValue"));
    this.store = spy(fakeStore);
    doThrow(new StoreAccessException("")).when(this.store).compute(eq("key"), getAnyBiFunction(), getBooleanNullaryFunction());

    final FakeCacheLoaderWriter fakeWriter = new FakeCacheLoaderWriter(Collections.singletonMap("key", "value"));
    this.cacheLoaderWriter = spy(fakeWriter);
    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    assertThat(ehcache.remove("key", "value"), is(true));
    verify(this.store).compute(eq("key"), getAnyBiFunction(), getBooleanNullaryFunction());
    verify(this.spiedResilienceStrategy)
        .removeFailure(eq("key"), eq("value"), any(StoreAccessException.class), eq(true));
    // Broken initial state: CacheLoaderWriter check omitted
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ConditionalRemoveOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link EhcacheWithLoaderWriter#remove(Object, Object)} for
   * <ul>
   *   <li>key with equal value present in {@code Store}</li>
   *   <li>>{@code Store.compute} throws</li>
   *   <li>key with equal value present via {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testRemoveValueEqualStoreEntryStoreAccessExceptionEqualCacheLoaderWriterEntry() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "value"));
    this.store = spy(fakeStore);
    doThrow(new StoreAccessException("")).when(this.store).compute(eq("key"), getAnyBiFunction(), getBooleanNullaryFunction());

    final FakeCacheLoaderWriter fakeWriter = new FakeCacheLoaderWriter(Collections.singletonMap("key", "value"));
    this.cacheLoaderWriter = spy(fakeWriter);
    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    assertThat(ehcache.remove("key", "value"), is(true));
    verify(this.store).compute(eq("key"), getAnyBiFunction(), getBooleanNullaryFunction());
    verify(this.spiedResilienceStrategy)
        .removeFailure(eq("key"), eq("value"), any(StoreAccessException.class), eq(true));
    assertThat(fakeWriter.getEntryMap().containsKey("key"), is(false));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ConditionalRemoveOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link EhcacheWithLoaderWriter#remove(Object, Object)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   *   <li>{@code CacheLoaderWriter.delete} throws</li>
   * </ul>
   */
  @Test
  public void testRemoveValueNoStoreEntryCacheWritingException() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.<String, String>emptyMap());
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeWriter = new FakeCacheLoaderWriter(Collections.singletonMap("key", "value"));
    this.cacheLoaderWriter = spy(fakeWriter);
    doThrow(new Exception()).when(this.cacheLoaderWriter).delete("key");
    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    try {
      ehcache.remove("key", "value");
      fail();
    } catch (CacheWritingException e) {
      // expected
    }
    verify(this.store).compute(eq("key"), getAnyBiFunction(), getBooleanNullaryFunction());
    verifyZeroInteractions(this.spiedResilienceStrategy);
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.ConditionalRemoveOutcome.class));
  }

  /**
   * Tests the effect of a {@link EhcacheWithLoaderWriter#remove(Object, Object)} for
   * <ul>
   *   <li>key with unequal value present in {@code Store}</li>
   *   <li>{@code CacheLoaderWriter.delete} throws</li>
   * </ul>
   */
  @Test
  public void testRemoveValueUnequalStoreEntryCacheWritingException() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "unequalValue"));
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeWriter = new FakeCacheLoaderWriter(Collections.singletonMap("key", "unequalValue"));
    this.cacheLoaderWriter = spy(fakeWriter);
    doThrow(new Exception()).when(this.cacheLoaderWriter).delete("key");
    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    assertFalse(ehcache.remove("key", "value"));
    verify(this.store).compute(eq("key"), getAnyBiFunction(), getBooleanNullaryFunction());
    verifyZeroInteractions(this.spiedResilienceStrategy);
    assertThat(fakeStore.getEntryMap().get("key"), is(equalTo("unequalValue")));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ConditionalRemoveOutcome.FAILURE_KEY_PRESENT));
  }

  /**
   * Tests the effect of a {@link EhcacheWithLoaderWriter#remove(Object, Object)} for
   * <ul>
   *   <li>key with equal value present in {@code Store}</li>
   *   <li>{@code CacheLoaderWriter.delete} throws</li>
   * </ul>
   */
  @Test
  public void testRemoveValueEqualStoreEntryCacheWritingException() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "value"));
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeWriter = new FakeCacheLoaderWriter(Collections.singletonMap("key", "value"));
    this.cacheLoaderWriter = spy(fakeWriter);
    doThrow(new Exception()).when(this.cacheLoaderWriter).delete("key");
    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    try {
      ehcache.remove("key", "value");
      fail();
    } catch (CacheWritingException e) {
      // Expected
    }
    verify(this.store).compute(eq("key"), getAnyBiFunction(), getBooleanNullaryFunction());
    verifyZeroInteractions(this.spiedResilienceStrategy);
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.RemoveOutcome.class));
  }

  /**
   * Tests the effect of a {@link EhcacheWithLoaderWriter#remove(Object, Object)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   *   <li>>{@code Store.compute} throws</li>
   *   <li>{@code CacheLoaderWriter.delete} throws</li>
   * </ul>
   */
  @Test
  public void testRemoveValueNoStoreEntryStoreAccessExceptionCacheWritingException() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.<String, String>emptyMap());
    this.store = spy(fakeStore);
    doThrow(new StoreAccessException("")).when(this.store).compute(eq("key"), getAnyBiFunction(), getBooleanNullaryFunction());

    final FakeCacheLoaderWriter fakeWriter = new FakeCacheLoaderWriter(Collections.singletonMap("key", "value"));
    this.cacheLoaderWriter = spy(fakeWriter);
    doThrow(new Exception()).when(this.cacheLoaderWriter).delete("key");
    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    try {
      ehcache.remove("key", "value");
      fail();
    } catch (CacheWritingException e) {
      // expected
    }
    verify(this.store).compute(eq("key"), getAnyBiFunction(), getBooleanNullaryFunction());
    verify(this.spiedResilienceStrategy)
        .removeFailure(eq("key"), eq("value"), any(StoreAccessException.class), any(CacheWritingException.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ConditionalRemoveOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link EhcacheWithLoaderWriter#remove(Object, Object)} for
   * <ul>
   *   <li>key with unequal value present in {@code Store}</li>
   *   <li>>{@code Store.compute} throws</li>
   *   <li>{@code CacheLoaderWriter.delete} throws</li>
   * </ul>
   */
  @Test
  public void testRemoveValueUnequalStoreEntryStoreAccessExceptionCacheWritingException() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "unequalValue"));
    this.store = spy(fakeStore);
    doThrow(new StoreAccessException("")).when(this.store).compute(eq("key"), getAnyBiFunction(), getBooleanNullaryFunction());

    final FakeCacheLoaderWriter fakeWriter = new FakeCacheLoaderWriter(Collections.singletonMap("key", "unequalValue"));
    this.cacheLoaderWriter = spy(fakeWriter);
    doThrow(new Exception()).when(this.cacheLoaderWriter).delete("key");
    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    ehcache.remove("key", "value");
    verify(this.store).compute(eq("key"), getAnyBiFunction(), getBooleanNullaryFunction());
    verify(this.spiedResilienceStrategy)
        .removeFailure(eq("key"), eq("value"), any(StoreAccessException.class), eq(false));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ConditionalRemoveOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link EhcacheWithLoaderWriter#remove(Object, Object)} for
   * <ul>
   *   <li>key with equal value present in {@code Store}</li>
   *   <li>>{@code Store.compute} throws</li>
   *   <li>{@code CacheLoaderWriter.delete} throws</li>
   * </ul>
   */
  @Test
  public void testRemoveValueEqualStoreEntryStoreAccessExceptionCacheWritingException() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "value"));
    this.store = spy(fakeStore);
    doThrow(new StoreAccessException("")).when(this.store).compute(eq("key"), getAnyBiFunction(), getBooleanNullaryFunction());

    final FakeCacheLoaderWriter fakeWriter = new FakeCacheLoaderWriter(Collections.singletonMap("key", "value"));
    this.cacheLoaderWriter = spy(fakeWriter);
    doThrow(new Exception()).when(this.cacheLoaderWriter).delete("key");
    final EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    try {
      ehcache.remove("key", "value");
      fail();
    } catch (CacheWritingException e) {
      // expected
    }
    verify(this.store).compute(eq("key"), getAnyBiFunction(), getBooleanNullaryFunction());
    verify(this.spiedResilienceStrategy)
        .removeFailure(eq("key"), eq("value"), any(StoreAccessException.class), any(CacheWritingException.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ConditionalRemoveOutcome.FAILURE));
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
    final EhcacheWithLoaderWriter<String, String> ehcache = new EhcacheWithLoaderWriter<String, String>(CACHE_CONFIGURATION, this.store, cacheLoaderWriter, cacheEventDispatcher, LoggerFactory.getLogger(EhcacheWithLoaderWriter.class + "-" + "EhcacheWithLoaderWriterBasicRemoveValueTest"));
    ehcache.init();
    assertThat("cache not initialized", ehcache.getStatus(), CoreMatchers.is(Status.AVAILABLE));
    this.spiedResilienceStrategy = this.setResilienceStrategySpy(ehcache);
    return ehcache;
  }
}
