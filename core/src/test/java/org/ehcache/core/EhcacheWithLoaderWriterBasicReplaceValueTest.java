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
import org.ehcache.core.internal.resilience.RobustLoaderWriterResilienceStrategy;
import org.ehcache.core.resilience.DefaultRecoveryStore;
import org.ehcache.core.statistics.CacheOperationOutcomes;
import org.ehcache.expiry.ExpiryPolicy;
import org.ehcache.spi.loaderwriter.CacheWritingException;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.ehcache.spi.resilience.StoreAccessException;
import org.hamcrest.CoreMatchers;
import org.junit.Test;
import org.mockito.Mock;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.EnumSet;

import static org.ehcache.core.util.Matchers.holding;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;
import static org.mockito.hamcrest.MockitoHamcrest.argThat;

/**
 * Provides testing of basic REPLACE(key, newValue, oldValue) operations on an {@code EhcacheWithLoaderWriter}.
 *
 * @author Clifford W. Johnson
 */
public class EhcacheWithLoaderWriterBasicReplaceValueTest extends EhcacheBasicCrudBase {

  @Mock
  protected CacheLoaderWriter<String, String> cacheLoaderWriter;


  @Test
  public void testReplaceValueNullNullNull() {
    EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    try {
      ehcache.replace(null, null, null);
      fail();
    } catch (NullPointerException e) {
      // expected
    }
  }

  @Test
  public void testReplaceKeyNullNull() {
    EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    try {
      ehcache.replace("key", null, null);
      fail();
    } catch (NullPointerException e) {
      // expected
    }
  }

  @Test
  public void testReplaceKeyValueNull() {
    EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    try {
      ehcache.replace("key", "oldValue", null);
      fail();
    } catch (NullPointerException e) {
      // expected
    }
  }

  @Test
  public void testReplaceKeyNullValue() {
    EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    try {
      ehcache.replace("key", null, "newValue");
      fail();
    } catch (NullPointerException e) {
      // expected
    }
  }

  @Test
  public void testReplaceNullValueNull() {
    EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    try {
      ehcache.replace(null, "oldValue", null);
      fail();
    } catch (NullPointerException e) {
      // expected
    }
  }

  @Test
  public void testReplaceNullValueValue() {
    EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    try {
      ehcache.replace(null, "oldValue", "newValue");
      fail();
    } catch (NullPointerException e) {
      // expected
    }
  }

  @Test
  public void testReplaceNullNullValue() {
    EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    try {
      ehcache.replace(null, null, "newValue");
      fail();
    } catch (NullPointerException e) {
      // expected
    }
  }


  /**
   * Tests the effect of a {@link EhcacheWithLoaderWriter#replace(Object, Object, Object)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   * </ul>
   */
  @Test
  public void testReplaceValueNoStoreEntry() throws Exception {
    FakeStore fakeStore = new FakeStore(Collections.emptyMap());
    this.store = spy(fakeStore);

    EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    assertFalse(ehcache.replace("key", "oldValue", "newValue"));
    verify(this.store).compute(eq("key"), getAnyBiFunction(), getBooleanSupplier());
    verifyZeroInteractions(this.resilienceStrategy);
    assertThat(fakeStore.getEntryMap().containsKey("key"), is(false));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ReplaceOutcome.MISS_NOT_PRESENT));
  }

  /**
   * Tests the effect of a {@link EhcacheWithLoaderWriter#replace(Object, Object, Object)} for
   * <ul>
   *   <li>key with unequal value in {@code Store}</li>
   * </ul>
   */
  @Test
  public void testReplaceValueUnequalStoreEntry() throws Exception {
    FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "unequalValue"));
    this.store = spy(fakeStore);

    EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    assertFalse(ehcache.replace("key", "oldValue", "newValue"));
    verify(this.store).compute(eq("key"), getAnyBiFunction(), getBooleanSupplier());
    verifyZeroInteractions(this.resilienceStrategy);
    assertThat(fakeStore.getEntryMap().get("key"), is(equalTo("unequalValue")));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ReplaceOutcome.MISS_PRESENT));
  }

  /**
   * Tests the effect of a {@link EhcacheWithLoaderWriter#replace(Object, Object, Object)} for
   * <ul>
   *   <li>key with equal value in {@code Store}</li>
   * </ul>
   */
  @Test
  public void testReplaceValueEqualStoreEntry() throws Exception {
    FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "oldValue"));
    this.store = spy(fakeStore);

    EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    assertTrue(ehcache.replace("key", "oldValue", "newValue"));
    verify(this.store).compute(eq("key"), getAnyBiFunction(), getBooleanSupplier());
    verifyZeroInteractions(this.resilienceStrategy);
    assertThat(fakeStore.getEntryMap().get("key"), is(equalTo("newValue")));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ReplaceOutcome.HIT));
  }

  /**
   * Tests the effect of a {@link EhcacheWithLoaderWriter#replace(Object, Object, Object)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   *   <li>>{@code Store.compute} throws</li>
   * </ul>
   */
  @Test
  public void testReplaceValueNoStoreEntryStoreAccessException() throws Exception {
    FakeStore fakeStore = new FakeStore(Collections.emptyMap());
    this.store = spy(fakeStore);
    doThrow(new StoreAccessException("")).when(this.store).compute(eq("key"), getAnyBiFunction(), getBooleanSupplier());

    EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    ehcache.replace("key", "oldValue", "newValue");
    verify(this.store).compute(eq("key"), getAnyBiFunction(), getBooleanSupplier());
    verify(this.resilienceStrategy).replaceFailure(eq("key"), eq("oldValue"), eq("newValue"), any(StoreAccessException.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ReplaceOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link EhcacheWithLoaderWriter#replace(Object, Object, Object)} for
   * <ul>
   *   <li>key with unequal value present in {@code Store}</li>
   *   <li>>{@code Store.compute} throws</li>
   * </ul>
   */
  @Test
  public void testReplaceValueUnequalStoreEntryStoreAccessException() throws Exception {
    FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "unequalValue"));
    this.store = spy(fakeStore);
    doThrow(new StoreAccessException("")).when(this.store).compute(eq("key"), getAnyBiFunction(), getBooleanSupplier());

    EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    ehcache.replace("key", "oldValue", "newValue");
    verify(this.store).compute(eq("key"), getAnyBiFunction(), getBooleanSupplier());
    verify(this.resilienceStrategy)
        .replaceFailure(eq("key"), eq("oldValue"), eq("newValue"), any(StoreAccessException.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ReplaceOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link EhcacheWithLoaderWriter#replace(Object, Object, Object)} for
   * <ul>
   *   <li>key with equal value present in {@code Store}</li>
   *   <li>>{@code Store.compute} throws</li>
   * </ul>
   */
  @Test
  public void testReplaceValueEqualStoreEntryStoreAccessException() throws Exception {
    FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "oldValue"));
    this.store = spy(fakeStore);
    doThrow(new StoreAccessException("")).when(this.store).compute(eq("key"), getAnyBiFunction(), getBooleanSupplier());

    EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    ehcache.replace("key", "oldValue", "newValue");
    verify(this.store).compute(eq("key"), getAnyBiFunction(), getBooleanSupplier());
    verify(this.resilienceStrategy)
        .replaceFailure(eq("key"), eq("oldValue"), eq("newValue"), any(StoreAccessException.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ReplaceOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link EhcacheWithLoaderWriter#replace(Object, Object, Object)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   *   <li>key not present via {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testReplaceValueNoStoreEntryNoCacheLoaderWriterEntry() throws Exception {
    FakeStore fakeStore = new FakeStore(Collections.emptyMap());
    this.store = spy(fakeStore);

    FakeCacheLoaderWriter fakeWriter = new FakeCacheLoaderWriter(Collections.emptyMap());
    EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(fakeWriter);

    assertFalse(ehcache.replace("key", "oldValue", "newValue"));
    verify(this.store).compute(eq("key"), getAnyBiFunction(), getBooleanSupplier());
    verifyZeroInteractions(this.resilienceStrategy);
    assertThat(fakeStore.getEntryMap().containsKey("key"), is(false));
    assertThat(fakeWriter.getEntryMap().containsKey("key"), is(false));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ReplaceOutcome.MISS_NOT_PRESENT));
  }

  /**
   * Tests the effect of a {@link EhcacheWithLoaderWriter#replace(Object, Object, Object)} for
   * <ul>
   *   <li>key with unequal value present in {@code Store}</li>
   *   <li>key not present via {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testReplaceValueUnequalStoreEntryNoCacheLoaderWriterEntry() throws Exception {
    FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "unequalValue"));
    this.store = spy(fakeStore);

    FakeCacheLoaderWriter fakeWriter = new FakeCacheLoaderWriter(Collections.emptyMap());
    EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(fakeWriter);

    assertFalse(ehcache.replace("key", "oldValue", "newValue"));
    verify(this.store).compute(eq("key"), getAnyBiFunction(), getBooleanSupplier());
    verifyZeroInteractions(this.resilienceStrategy);
    assertThat(fakeStore.getEntryMap().get("key"), is(equalTo("unequalValue")));
    // Broken initial state: CacheLoaderWriter check omitted
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ReplaceOutcome.MISS_PRESENT));
  }

  /**
   * Tests the effect of a {@link EhcacheWithLoaderWriter#replace(Object, Object, Object)} for
   * <ul>
   *   <li>key with equal value present in {@code Store}</li>
   *   <li>key not present via {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testReplaceValueEqualStoreEntryNoCacheLoaderWriterEntry() throws Exception {
    FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "oldValue"));
    this.store = spy(fakeStore);

    FakeCacheLoaderWriter fakeWriter = new FakeCacheLoaderWriter(Collections.emptyMap());
    EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(fakeWriter);

    assertTrue(ehcache.replace("key", "oldValue", "newValue"));
    verify(this.store).compute(eq("key"), getAnyBiFunction(), getBooleanSupplier());
    verifyZeroInteractions(this.resilienceStrategy);
    assertThat(fakeStore.getEntryMap().get("key"), is(equalTo("newValue")));
    // Broken initial state: CacheLoaderWriter check omitted
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ReplaceOutcome.HIT));
  }

  /**
   * Tests the effect of a {@link EhcacheWithLoaderWriter#replace(Object, Object, Object)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   *   <li>>{@code Store.compute} throws</li>
   *   <li>key not present via {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testReplaceValueNoStoreEntryStoreAccessExceptionNoCacheLoaderWriterEntry() throws Exception {
    FakeStore fakeStore = new FakeStore(Collections.emptyMap());
    this.store = spy(fakeStore);
    doThrow(new StoreAccessException("")).when(this.store).compute(eq("key"), getAnyBiFunction(), getBooleanSupplier());

    FakeCacheLoaderWriter fakeWriter = new FakeCacheLoaderWriter(Collections.emptyMap());
    this.cacheLoaderWriter = spy(fakeWriter);
    EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    ehcache.replace("key", "oldValue", "newValue");
    verify(this.store).compute(eq("key"), getAnyBiFunction(), getBooleanSupplier());
    verify(this.resilienceStrategy)
        .replaceFailure(eq("key"), eq("oldValue"), eq("newValue"), any(StoreAccessException.class));
    assertThat(fakeWriter.getEntryMap().containsKey("key"), is(false));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ReplaceOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link EhcacheWithLoaderWriter#replace(Object, Object, Object)} for
   * <ul>
   *   <li>key with unequal value present in {@code Store}</li>
   *   <li>>{@code Store.compute} throws</li>
   *   <li>key not present via {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testReplaceValueUnequalStoreEntryStoreAccessExceptionNoCacheLoaderWriterEntry() throws Exception {
    FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "unequalValue"));
    this.store = spy(fakeStore);
    doThrow(new StoreAccessException("")).when(this.store).compute(eq("key"), getAnyBiFunction(), getBooleanSupplier());

    FakeCacheLoaderWriter fakeWriter = new FakeCacheLoaderWriter(Collections.emptyMap());
    this.cacheLoaderWriter = spy(fakeWriter);
    EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    ehcache.replace("key", "oldValue", "newValue");
    verify(this.store).compute(eq("key"), getAnyBiFunction(), getBooleanSupplier());
    verify(this.resilienceStrategy)
        .replaceFailure(eq("key"), eq("oldValue"), eq("newValue"), any(StoreAccessException.class));
    // Broken initial state: CacheLoaderWriter check omitted
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ReplaceOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link EhcacheWithLoaderWriter#replace(Object, Object, Object)} for
   * <ul>
   *   <li>key with equal value present in {@code Store}</li>
   *   <li>>{@code Store.compute} throws</li>
   *   <li>key not present via {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testReplaceValueEqualStoreEntryStoreAccessExceptionNoCacheLoaderWriterEntry() throws Exception {
    FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "oldValue"));
    this.store = spy(fakeStore);
    doThrow(new StoreAccessException("")).when(this.store).compute(eq("key"), getAnyBiFunction(), getBooleanSupplier());

    FakeCacheLoaderWriter fakeWriter = new FakeCacheLoaderWriter(Collections.emptyMap());
    this.cacheLoaderWriter = spy(fakeWriter);
    EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    ehcache.replace("key", "oldValue", "newValue");
    verify(this.store).compute(eq("key"), getAnyBiFunction(), getBooleanSupplier());
    verify(this.resilienceStrategy)
        .replaceFailure(eq("key"), eq("oldValue"), eq("newValue"), any(StoreAccessException.class));
    // Broken initial state: CacheLoaderWriter check omitted
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ReplaceOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link EhcacheWithLoaderWriter#replace(Object, Object, Object)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   *   <li>key with unequal value present via {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testReplaceValueNoStoreEntryUnequalCacheLoaderWriterEntry() throws Exception {
    FakeStore fakeStore = new FakeStore(Collections.emptyMap());
    this.store = spy(fakeStore);

    FakeCacheLoaderWriter fakeWriter = new FakeCacheLoaderWriter(Collections.singletonMap("key", "unequalValue"));
    EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(fakeWriter);

    assertFalse(ehcache.replace("key", "oldValue", "newValue"));
    verify(this.store).compute(eq("key"), getAnyBiFunction(), getBooleanSupplier());
    verifyZeroInteractions(this.resilienceStrategy);
    assertThat(fakeStore.getEntryMap().get("key"), is("unequalValue"));
    assertThat(fakeWriter.getEntryMap().get("key"), is(equalTo("unequalValue")));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ReplaceOutcome.MISS_PRESENT));
  }

  /**
   * Tests the effect of a {@link EhcacheWithLoaderWriter#replace(Object, Object, Object)} for
   * <ul>
   *   <li>key with unequal value present in {@code Store}</li>
   *   <li>key with unequal value present via {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testReplaceValueUnequalStoreEntryUnequalCacheLoaderWriterEntry() throws Exception {
    FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "unequalValue"));
    this.store = spy(fakeStore);

    FakeCacheLoaderWriter fakeWriter = new FakeCacheLoaderWriter(Collections.singletonMap("key", "unequalValue"));
    EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(fakeWriter);

    assertFalse(ehcache.replace("key", "oldValue", "newValue"));
    verify(this.store).compute(eq("key"), getAnyBiFunction(), getBooleanSupplier());
    verifyZeroInteractions(this.resilienceStrategy);
    assertThat(fakeStore.getEntryMap().get("key"), is(equalTo("unequalValue")));
    assertThat(fakeWriter.getEntryMap().get("key"), is(equalTo("unequalValue")));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ReplaceOutcome.MISS_PRESENT));
  }

  /**
   * Tests the effect of a {@link EhcacheWithLoaderWriter#replace(Object, Object, Object)} for
   * <ul>
   *   <li>key with equal value present in {@code Store}</li>
   *   <li>key with unequal value present via {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testReplaceValueEqualStoreEntryUnequalCacheLoaderWriterEntry() throws Exception {
    FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "oldValue"));
    this.store = spy(fakeStore);

    FakeCacheLoaderWriter fakeWriter = new FakeCacheLoaderWriter(Collections.singletonMap("key", "unequalValue"));
    EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(fakeWriter);

    assertTrue(ehcache.replace("key", "oldValue", "newValue"));
    verify(this.store).compute(eq("key"), getAnyBiFunction(), getBooleanSupplier());
    verifyZeroInteractions(this.resilienceStrategy);
    assertThat(fakeStore.getEntryMap().get("key"), is(equalTo("newValue")));
    // Broken initial state: CacheLoaderWriter check omitted
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ReplaceOutcome.HIT));
  }

  /**
   * Tests the effect of a {@link EhcacheWithLoaderWriter#replace(Object, Object, Object)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   *   <li>>{@code Store.compute} throws</li>
   *   <li>key with unequal value present via {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testReplaceValueNoStoreEntryStoreAccessExceptionUnequalCacheLoaderWriterEntry() throws Exception {
    FakeStore fakeStore = new FakeStore(Collections.emptyMap());
    this.store = spy(fakeStore);
    doThrow(new StoreAccessException("")).when(this.store).compute(eq("key"), getAnyBiFunction(), getBooleanSupplier());

    FakeCacheLoaderWriter fakeWriter = new FakeCacheLoaderWriter(Collections.singletonMap("key", "unequalValue"));
    this.cacheLoaderWriter = spy(fakeWriter);
    EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    ehcache.replace("key", "oldValue", "newValue");
    verify(this.store).compute(eq("key"), getAnyBiFunction(), getBooleanSupplier());
    verify(this.resilienceStrategy)
        .replaceFailure(eq("key"), eq("oldValue"), eq("newValue"), any(StoreAccessException.class));
    assertThat(fakeWriter.getEntryMap().get("key"), is(equalTo("unequalValue")));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ReplaceOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link EhcacheWithLoaderWriter#replace(Object, Object, Object)} for
   * <ul>
   *   <li>key with unequal value present in {@code Store}</li>
   *   <li>>{@code Store.compute} throws</li>
   *   <li>key with unequal value present via {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testReplaceValueUnequalStoreEntryStoreAccessExceptionUnequalCacheLoaderWriterEntry() throws Exception {
    FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "unequalValue"));
    this.store = spy(fakeStore);
    doThrow(new StoreAccessException("")).when(this.store).compute(eq("key"), getAnyBiFunction(), getBooleanSupplier());

    FakeCacheLoaderWriter fakeWriter = new FakeCacheLoaderWriter(Collections.singletonMap("key", "unequalValue"));
    this.cacheLoaderWriter = spy(fakeWriter);
    EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    ehcache.replace("key", "oldValue", "newValue");
    verify(this.store).compute(eq("key"), getAnyBiFunction(), getBooleanSupplier());
    verify(this.resilienceStrategy)
        .replaceFailure(eq("key"), eq("oldValue"), eq("newValue"), any(StoreAccessException.class));
    assertThat(fakeWriter.getEntryMap().get("key"), is(equalTo("unequalValue")));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ReplaceOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link EhcacheWithLoaderWriter#replace(Object, Object, Object)} for
   * <ul>
   *   <li>key with equal value present in {@code Store}</li>
   *   <li>>{@code Store.compute} throws</li>
   *   <li>key with unequal value present via {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testReplaceValueEqualStoreEntryStoreAccessExceptionUnequalCacheLoaderWriterEntry() throws Exception {
    FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "oldValue"));
    this.store = spy(fakeStore);
    doThrow(new StoreAccessException("")).when(this.store).compute(eq("key"), getAnyBiFunction(), getBooleanSupplier());

    FakeCacheLoaderWriter fakeWriter = new FakeCacheLoaderWriter(Collections.singletonMap("key", "unequalValue"));
    this.cacheLoaderWriter = spy(fakeWriter);
    EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    ehcache.replace("key", "oldValue", "newValue");
    verify(this.store).compute(eq("key"), getAnyBiFunction(), getBooleanSupplier());
    verify(this.resilienceStrategy)
        .replaceFailure(eq("key"), eq("oldValue"), eq("newValue"), any(StoreAccessException.class));
    // Broken initial state: CacheLoaderWriter check omitted
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ReplaceOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link EhcacheWithLoaderWriter#replace(Object, Object, Object)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   *   <li>key with equal value present via {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testReplaceValueNoStoreEntryEqualCacheLoaderWriterEntry() throws Exception {
    FakeStore fakeStore = new FakeStore(Collections.emptyMap());
    this.store = spy(fakeStore);

    FakeCacheLoaderWriter fakeWriter = new FakeCacheLoaderWriter(Collections.singletonMap("key", "oldValue"));
    EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(fakeWriter);

    assertThat(ehcache.replace("key", "oldValue", "newValue"), is(true));
    verify(this.store).compute(eq("key"), getAnyBiFunction(), getBooleanSupplier());
    verifyZeroInteractions(this.resilienceStrategy);
    assertThat(fakeStore.getEntryMap().get("key"), is("newValue"));
    assertThat(fakeWriter.getEntryMap().get("key"), is(equalTo("newValue")));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ReplaceOutcome.HIT));
  }

  /**
   * Tests the effect of a {@link EhcacheWithLoaderWriter#replace(Object, Object, Object)} for
   * <ul>
   *   <li>key with unequal value present in {@code Store}</li>
   *   <li>key with equal value present via {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testReplaceValueUnequalStoreEntryEqualCacheLoaderWriterEntry() throws Exception {
    FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "unequalValue"));
    this.store = spy(fakeStore);

    FakeCacheLoaderWriter fakeWriter = new FakeCacheLoaderWriter(Collections.singletonMap("key", "oldValue"));
    EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(fakeWriter);

    assertFalse(ehcache.replace("key", "oldValue", "newValue"));
    verify(this.store).compute(eq("key"), getAnyBiFunction(), getBooleanSupplier());
    verifyZeroInteractions(this.resilienceStrategy);
    assertThat(fakeStore.getEntryMap().get("key"), is(equalTo("unequalValue")));
    // Broken initial state: CacheLoaderWriter check omitted
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ReplaceOutcome.MISS_PRESENT));
  }

  /**
   * Tests the effect of a {@link EhcacheWithLoaderWriter#replace(Object, Object, Object)} for
   * <ul>
   *   <li>key with equal value present in {@code Store}</li>
   *   <li>key with equal value present via {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testReplaceValueEqualStoreEntryEqualCacheLoaderWriterEntry() throws Exception {
    FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "oldValue"));
    this.store = spy(fakeStore);

    FakeCacheLoaderWriter fakeWriter = new FakeCacheLoaderWriter(Collections.singletonMap("key", "oldValue"));
    EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(fakeWriter);

    assertTrue(ehcache.replace("key", "oldValue", "newValue"));
    verify(this.store).compute(eq("key"), getAnyBiFunction(), getBooleanSupplier());
    verifyZeroInteractions(this.resilienceStrategy);
    assertThat(fakeStore.getEntryMap().get("key"), is(equalTo("newValue")));
    assertThat(fakeWriter.getEntryMap().get("key"), is(equalTo("newValue")));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ReplaceOutcome.HIT));
  }

  /**
   * Tests the effect of a {@link EhcacheWithLoaderWriter#replace(Object, Object, Object)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   *   <li>>{@code Store.compute} throws</li>
   *   <li>key with equal value present via {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testReplaceValueNoStoreEntryStoreAccessExceptionEqualCacheLoaderWriterEntry() throws Exception {
    FakeStore fakeStore = new FakeStore(Collections.emptyMap());
    this.store = spy(fakeStore);
    doThrow(new StoreAccessException("")).when(this.store).compute(eq("key"), getAnyBiFunction(), getBooleanSupplier());

    FakeCacheLoaderWriter fakeWriter = new FakeCacheLoaderWriter(Collections.singletonMap("key", "oldValue"));
    this.cacheLoaderWriter = spy(fakeWriter);
    EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    ehcache.replace("key", "oldValue", "newValue");
    verify(this.store).compute(eq("key"), getAnyBiFunction(), getBooleanSupplier());
    verify(this.resilienceStrategy).replaceFailure(eq("key"), eq("oldValue"), eq("newValue"), any(StoreAccessException.class));
    assertThat(fakeWriter.getEntryMap().get("key"), is(equalTo("newValue")));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ReplaceOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link EhcacheWithLoaderWriter#replace(Object, Object, Object)} for
   * <ul>
   *   <li>key with unequal value present in {@code Store}</li>
   *   <li>>{@code Store.compute} throws</li>
   *   <li>key with equal value present via {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testReplaceValueUnequalStoreEntryStoreAccessExceptionEqualCacheLoaderWriterEntry() throws Exception {
    FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "unequalValue"));
    this.store = spy(fakeStore);
    doThrow(new StoreAccessException("")).when(this.store).compute(eq("key"), getAnyBiFunction(), getBooleanSupplier());

    FakeCacheLoaderWriter fakeWriter = new FakeCacheLoaderWriter(Collections.singletonMap("key", "oldValue"));
    this.cacheLoaderWriter = spy(fakeWriter);
    EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    ehcache.replace("key", "oldValue", "newValue");
    verify(this.store).compute(eq("key"), getAnyBiFunction(), getBooleanSupplier());
    verify(this.resilienceStrategy).replaceFailure(eq("key"), eq("oldValue"), eq("newValue"), any(StoreAccessException.class));
    // Broken initial state: CacheLoaderWriter check omitted
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ReplaceOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link EhcacheWithLoaderWriter#replace(Object, Object, Object)} for
   * <ul>
   *   <li>key with equal value present in {@code Store}</li>
   *   <li>>{@code Store.compute} throws</li>
   *   <li>key with equal value present via {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testReplaceValueEqualStoreEntryStoreAccessExceptionEqualCacheLoaderWriterEntry() throws Exception {
    FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "oldValue"));
    this.store = spy(fakeStore);
    doThrow(new StoreAccessException("")).when(this.store).compute(eq("key"), getAnyBiFunction(), getBooleanSupplier());

    FakeCacheLoaderWriter fakeWriter = new FakeCacheLoaderWriter(Collections.singletonMap("key", "oldValue"));
    this.cacheLoaderWriter = spy(fakeWriter);
    EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    ehcache.replace("key", "oldValue", "newValue");
    verify(this.store).compute(eq("key"), getAnyBiFunction(), getBooleanSupplier());
    verify(this.resilienceStrategy).replaceFailure(eq("key"), eq("oldValue"), eq("newValue"), any(StoreAccessException.class));
    assertThat(fakeWriter.getEntryMap().get("key"), is(equalTo("newValue")));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ReplaceOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link EhcacheWithLoaderWriter#replace(Object, Object, Object)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   *   <li>{@code CacheLoaderWriter.write} throws</li>
   * </ul>
   */
  @Test
  public void testReplaceValueNoStoreEntryCacheLoaderWriterException() throws Exception {
    FakeStore fakeStore = new FakeStore(Collections.emptyMap());
    this.store = spy(fakeStore);

    FakeCacheLoaderWriter fakeWriter = new FakeCacheLoaderWriter(Collections.singletonMap("key", "oldValue"));
    this.cacheLoaderWriter = spy(fakeWriter);
    doThrow(new Exception()).when(this.cacheLoaderWriter).write("key", "newValue");
    EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    try {
      ehcache.replace("key", "oldValue", "newValue");
      fail();
    } catch (CacheWritingException e) {
      // expected
    }
    verify(this.store).compute(eq("key"), getAnyBiFunction(), getBooleanSupplier());
    verifyZeroInteractions(this.resilienceStrategy);
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ReplaceOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link EhcacheWithLoaderWriter#replace(Object, Object, Object)} for
   * <ul>
   *   <li>key with unequal value present in {@code Store}</li>
   *   <li>{@code CacheLoaderWriter.write} throws</li>
   * </ul>
   */
  @Test
  public void testReplaceValueUnequalStoreEntryCacheLoaderWriterException() throws Exception {
    FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "unequalValue"));
    this.store = spy(fakeStore);

    FakeCacheLoaderWriter fakeWriter = new FakeCacheLoaderWriter(Collections.singletonMap("key", "oldValue"));
    this.cacheLoaderWriter = spy(fakeWriter);
    doThrow(new Exception()).when(this.cacheLoaderWriter).write("key", "newValue");
    EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    assertFalse(ehcache.replace("key", "oldValue", "newValue"));
    verify(this.store).compute(eq("key"), getAnyBiFunction(), getBooleanSupplier());
    verifyZeroInteractions(this.resilienceStrategy);
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ReplaceOutcome.MISS_PRESENT));
  }

  /**
   * Tests the effect of a {@link EhcacheWithLoaderWriter#replace(Object, Object, Object)} for
   * <ul>
   *   <li>key with equal value present in {@code Store}</li>
   *   <li>{@code CacheLoaderWriter.write} throws</li>
   * </ul>
   */
  @Test
  public void testReplaceValueEqualStoreEntryCacheLoaderWriterException() throws Exception {
    FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "oldValue"));
    this.store = spy(fakeStore);

    FakeCacheLoaderWriter fakeWriter = new FakeCacheLoaderWriter(Collections.singletonMap("key", "oldValue"));
    this.cacheLoaderWriter = spy(fakeWriter);
    doThrow(new Exception()).when(this.cacheLoaderWriter).write("key", "newValue");
    EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    try {
      ehcache.replace("key", "oldValue", "newValue");
      fail();
    } catch (CacheWritingException e) {
      // Expected
    }
    verify(this.store).compute(eq("key"), getAnyBiFunction(), getBooleanSupplier());
    verifyZeroInteractions(this.resilienceStrategy);
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ReplaceOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link EhcacheWithLoaderWriter#replace(Object, Object, Object)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   *   <li>>{@code Store.compute} throws</li>
   *   <li>{@code CacheLoaderWriter.write} throws</li>
   * </ul>
   */
  @Test
  public void testReplaceValueNoStoreEntryStoreAccessExceptionCacheLoaderWriterException() throws Exception {
    FakeStore fakeStore = new FakeStore(Collections.emptyMap());
    this.store = spy(fakeStore);
    doThrow(new StoreAccessException("")).when(this.store).compute(eq("key"), getAnyBiFunction(), getBooleanSupplier());

    FakeCacheLoaderWriter fakeWriter = new FakeCacheLoaderWriter(Collections.singletonMap("key", "oldValue"));
    this.cacheLoaderWriter = spy(fakeWriter);
    doThrow(new Exception()).when(this.cacheLoaderWriter).write("key", "newValue");
    EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    try {
      ehcache.replace("key", "oldValue", "newValue");
      fail();
    } catch (CacheWritingException e) {
      // expected
    }
    verify(this.store).compute(eq("key"), getAnyBiFunction(), getBooleanSupplier());
    verify(this.resilienceStrategy).replaceFailure(eq("key"), eq("oldValue"), eq("newValue"), any(StoreAccessException.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ReplaceOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link EhcacheWithLoaderWriter#replace(Object, Object, Object)} for
   * <ul>
   *   <li>key with unequal value present in {@code Store}</li>
   *   <li>>{@code Store.compute} throws</li>
   *   <li>{@code CacheLoaderWriter.write} throws</li>
   * </ul>
   */
  @Test
  public void testReplaceValueUnequalStoreEntryStoreAccessExceptionCacheLoaderWriterException() throws Exception {
    FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "unequalValue"));
    this.store = spy(fakeStore);
    doThrow(new StoreAccessException("")).when(this.store).compute(eq("key"), getAnyBiFunction(), getBooleanSupplier());

    FakeCacheLoaderWriter fakeWriter = new FakeCacheLoaderWriter(Collections.singletonMap("key", "oldValue"));
    this.cacheLoaderWriter = spy(fakeWriter);
    doThrow(new Exception()).when(this.cacheLoaderWriter).write("key", "newValue");
    EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    try {
      ehcache.replace("key", "oldValue", "newValue");
      fail();
    } catch (CacheWritingException e) {
      // expected
    }
    verify(this.store).compute(eq("key"), getAnyBiFunction(), getBooleanSupplier());
    verify(this.resilienceStrategy)
        .replaceFailure(eq("key"), eq("oldValue"), eq("newValue"), any(StoreAccessException.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ReplaceOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link EhcacheWithLoaderWriter#replace(Object, Object, Object)} for
   * <ul>
   *   <li>key with equal value present in {@code Store}</li>
   *   <li>>{@code Store.compute} throws</li>
   *   <li>{@code CacheLoaderWriter.write} throws</li>
   * </ul>
   */
  @Test
  public void testReplaceValueEqualStoreEntryStoreAccessExceptionCacheLoaderWriterException() throws Exception {
    FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "oldValue"));
    this.store = spy(fakeStore);
    doThrow(new StoreAccessException("")).when(this.store).compute(eq("key"), getAnyBiFunction(), getBooleanSupplier());

    FakeCacheLoaderWriter fakeWriter = new FakeCacheLoaderWriter(Collections.singletonMap("key", "oldValue"));
    this.cacheLoaderWriter = spy(fakeWriter);
    doThrow(new Exception()).when(this.cacheLoaderWriter).write("key", "newValue");
    EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    try {
      ehcache.replace("key", "oldValue", "newValue");
      fail();
    } catch (CacheWritingException e) {
      // Expected
    }
    verify(this.store).compute(eq("key"), getAnyBiFunction(), getBooleanSupplier());
    verify(this.resilienceStrategy).replaceFailure(eq("key"), eq("oldValue"), eq("newValue"), any(StoreAccessException.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ReplaceOutcome.FAILURE));
  }

  @Test
  public void testReplaceWithImmediatelyExpiredEntry() throws Exception {
    FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "old-value"));
    this.store = spy(fakeStore);

    FakeCacheLoaderWriter fakeWriter = new FakeCacheLoaderWriter(Collections.singletonMap("key", "old-value"));

    @SuppressWarnings("unchecked")
    ExpiryPolicy<String, String> expiry = mock(ExpiryPolicy.class);
    when(expiry.getExpiryForUpdate(eq("key"), argThat(holding("old-value")), eq("value"))).thenReturn(Duration.ZERO);

    EhcacheWithLoaderWriter<String, String> ehcache = this.getEhcache(fakeWriter, expiry);

    ehcache.replace("key", "old-value", "value");
    verify(this.store).compute(eq("key"), getAnyBiFunction(), getBooleanSupplier());
    verifyZeroInteractions(this.resilienceStrategy);
    assertThat(fakeStore.getEntryMap().get("key"), nullValue());
    assertThat(fakeWriter.getEntryMap().get("key"), equalTo("value"));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ReplaceOutcome.HIT));
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
  private EhcacheWithLoaderWriter<String, String> getEhcache(CacheLoaderWriter<String, String> cacheLoaderWriter) {
    return getEhcache(cacheLoaderWriter, ExpiryPolicy.NO_EXPIRY);
  }

  private EhcacheWithLoaderWriter<String, String> getEhcache(CacheLoaderWriter<String, String> cacheLoaderWriter, ExpiryPolicy<? super String, ? super String> expiry) {
    CacheConfiguration<String, String> config = new BaseCacheConfiguration<>(String.class, String.class, null, null,
      expiry, ResourcePoolsHelper.createHeapOnlyPools());
    this.resilienceStrategy = spy(new RobustLoaderWriterResilienceStrategy<>(new DefaultRecoveryStore<>(this.store), cacheLoaderWriter));
    EhcacheWithLoaderWriter<String, String> ehcache = new EhcacheWithLoaderWriter<>(config, this.store, resilienceStrategy, cacheLoaderWriter, cacheEventDispatcher, LoggerFactory
      .getLogger(EhcacheWithLoaderWriter.class + "-" + "EhcacheWithLoaderWriterBasicReplaceValueTest"));
    ehcache.init();
    assertThat("cache not initialized", ehcache.getStatus(), CoreMatchers.is(Status.AVAILABLE));
    return ehcache;
  }
}
