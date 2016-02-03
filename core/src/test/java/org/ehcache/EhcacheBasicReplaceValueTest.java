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

import org.ehcache.config.BaseCacheConfiguration;
import org.ehcache.config.ResourcePoolsHelper;
import org.ehcache.exceptions.CacheAccessException;
import org.ehcache.exceptions.CacheWritingException;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.ehcache.statistics.CacheOperationOutcomes;
import org.junit.Test;
import org.mockito.Mock;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.EnumSet;

import org.ehcache.config.CacheConfiguration;
import org.ehcache.expiry.Duration;
import org.ehcache.expiry.Expirations;
import org.ehcache.expiry.Expiry;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

/**
 * Provides testing of basic REPLACE(key, newValue, oldValue) operations on an {@code Ehcache}.
 *
 * @author Clifford W. Johnson
 */
public class EhcacheBasicReplaceValueTest extends EhcacheBasicCrudBase {

  @Mock
  protected CacheLoaderWriter<String, String> cacheLoaderWriter;


  @Test
  public void testReplaceValueNullNullNull() {
    final Ehcache<String, String> ehcache = this.getEhcache(null);

    try {
      ehcache.replace(null, null, null);
      fail();
    } catch (NullPointerException e) {
      // expected
    }
  }

  @Test
  public void testReplaceKeyNullNull() {
    final Ehcache<String, String> ehcache = this.getEhcache(null);

    try {
      ehcache.replace("key", null, null);
      fail();
    } catch (NullPointerException e) {
      // expected
    }
  }

  @Test
  public void testReplaceKeyValueNull() {
    final Ehcache<String, String> ehcache = this.getEhcache(null);

    try {
      ehcache.replace("key", "oldValue", null);
      fail();
    } catch (NullPointerException e) {
      // expected
    }
  }

  @Test
  public void testReplaceKeyNullValue() {
    final Ehcache<String, String> ehcache = this.getEhcache(null);

    try {
      ehcache.replace("key", null, "newValue");
      fail();
    } catch (NullPointerException e) {
      // expected
    }
  }

  @Test
  public void testReplaceNullValueNull() {
    final Ehcache<String, String> ehcache = this.getEhcache(null);

    try {
      ehcache.replace(null, "oldValue", null);
      fail();
    } catch (NullPointerException e) {
      // expected
    }
  }

  @Test
  public void testReplaceNullValueValue() {
    final Ehcache<String, String> ehcache = this.getEhcache(null);

    try {
      ehcache.replace(null, "oldValue", "newValue");
      fail();
    } catch (NullPointerException e) {
      // expected
    }
  }

  @Test
  public void testReplaceNullNullValue() {
    final Ehcache<String, String> ehcache = this.getEhcache(null);

    try {
      ehcache.replace(null, null, "newValue");
      fail();
    } catch (NullPointerException e) {
      // expected
    }
  }


  /**
   * Tests the effect of a {@link org.ehcache.Ehcache#replace(Object, Object, Object)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   *   <li>no {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testReplaceValueNoStoreEntryNoCacheLoaderWriter() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.<String, String>emptyMap());
    this.store = spy(fakeStore);

    final Ehcache<String, String> ehcache = this.getEhcache(null);

    assertFalse(ehcache.replace("key", "oldValue", "newValue"));
    verify(this.store).compute(eq("key"), getAnyBiFunction(), getBooleanNullaryFunction());
    verifyZeroInteractions(this.spiedResilienceStrategy);
    assertThat(fakeStore.getEntryMap().containsKey("key"), is(false));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ReplaceOutcome.MISS_NOT_PRESENT));
  }

  /**
   * Tests the effect of a {@link org.ehcache.Ehcache#replace(Object, Object, Object)} for
   * <ul>
   *   <li>key with unequal value in {@code Store}</li>
   *   <li>no {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testReplaceValueUnequalStoreEntryNoCacheLoaderWriter() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "unequalValue"));
    this.store = spy(fakeStore);

    final Ehcache<String, String> ehcache = this.getEhcache(null);

    assertFalse(ehcache.replace("key", "oldValue", "newValue"));
    verify(this.store).compute(eq("key"), getAnyBiFunction(), getBooleanNullaryFunction());
    verifyZeroInteractions(this.spiedResilienceStrategy);
    assertThat(fakeStore.getEntryMap().get("key"), is(equalTo("unequalValue")));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ReplaceOutcome.MISS_PRESENT));
  }

  /**
   * Tests the effect of a {@link org.ehcache.Ehcache#replace(Object, Object, Object)} for
   * <ul>
   *   <li>key with equal value in {@code Store}</li>
   *   <li>no {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testReplaceValueEqualStoreEntryNoCacheLoaderWriter() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "oldValue"));
    this.store = spy(fakeStore);

    final Ehcache<String, String> ehcache = this.getEhcache(null);

    assertTrue(ehcache.replace("key", "oldValue", "newValue"));
    verify(this.store).compute(eq("key"), getAnyBiFunction(), getBooleanNullaryFunction());
    verifyZeroInteractions(this.spiedResilienceStrategy);
    assertThat(fakeStore.getEntryMap().get("key"), is(equalTo("newValue")));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ReplaceOutcome.HIT));
  }

  /**
   * Tests the effect of a {@link org.ehcache.Ehcache#replace(Object, Object, Object)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   *   <li>>{@code Store.compute} throws</li>
   *   <li>no {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testReplaceValueNoStoreEntryCacheAccessExceptionNoCacheLoaderWriter() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.<String, String>emptyMap());
    this.store = spy(fakeStore);
    doThrow(new CacheAccessException("")).when(this.store).compute(eq("key"), getAnyBiFunction(), getBooleanNullaryFunction());

    final Ehcache<String, String> ehcache = this.getEhcache(null);

    ehcache.replace("key", "oldValue", "newValue");
    verify(this.store).compute(eq("key"), getAnyBiFunction(), getBooleanNullaryFunction());
    verify(this.spiedResilienceStrategy)
        .replaceFailure(eq("key"), eq("oldValue"), eq("newValue"), any(CacheAccessException.class), eq(false));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ReplaceOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link org.ehcache.Ehcache#replace(Object, Object, Object)} for
   * <ul>
   *   <li>key with unequal value present in {@code Store}</li>
   *   <li>>{@code Store.compute} throws</li>
   *   <li>no {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testReplaceValueUnequalStoreEntryCacheAccessExceptionNoCacheLoaderWriter() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "unequalValue"));
    this.store = spy(fakeStore);
    doThrow(new CacheAccessException("")).when(this.store).compute(eq("key"), getAnyBiFunction(), getBooleanNullaryFunction());

    final Ehcache<String, String> ehcache = this.getEhcache(null);

    ehcache.replace("key", "oldValue", "newValue");
    verify(this.store).compute(eq("key"), getAnyBiFunction(), getBooleanNullaryFunction());
    verify(this.spiedResilienceStrategy)
        .replaceFailure(eq("key"), eq("oldValue"), eq("newValue"), any(CacheAccessException.class), eq(false));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ReplaceOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link org.ehcache.Ehcache#replace(Object, Object, Object)} for
   * <ul>
   *   <li>key with equal value present in {@code Store}</li>
   *   <li>>{@code Store.compute} throws</li>
   *   <li>no {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testReplaceValueEqualStoreEntryCacheAccessExceptionNoCacheLoaderWriter() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "oldValue"));
    this.store = spy(fakeStore);
    doThrow(new CacheAccessException("")).when(this.store).compute(eq("key"), getAnyBiFunction(), getBooleanNullaryFunction());

    final Ehcache<String, String> ehcache = this.getEhcache(null);

    ehcache.replace("key", "oldValue", "newValue");
    verify(this.store).compute(eq("key"), getAnyBiFunction(), getBooleanNullaryFunction());
    verify(this.spiedResilienceStrategy)
        .replaceFailure(eq("key"), eq("oldValue"), eq("newValue"), any(CacheAccessException.class), eq(false));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ReplaceOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link org.ehcache.Ehcache#replace(Object, Object, Object)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   *   <li>key not present via {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testReplaceValueNoStoreEntryNoCacheLoaderWriterEntry() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.<String, String>emptyMap());
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeWriter = new FakeCacheLoaderWriter(Collections.<String, String>emptyMap());
    final Ehcache<String, String> ehcache = this.getEhcache(fakeWriter);

    assertFalse(ehcache.replace("key", "oldValue", "newValue"));
    verify(this.store).compute(eq("key"), getAnyBiFunction(), getBooleanNullaryFunction());
    verifyZeroInteractions(this.spiedResilienceStrategy);
    assertThat(fakeStore.getEntryMap().containsKey("key"), is(false));
    assertThat(fakeWriter.getEntryMap().containsKey("key"), is(false));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ReplaceOutcome.MISS_NOT_PRESENT));
  }

  /**
   * Tests the effect of a {@link org.ehcache.Ehcache#replace(Object, Object, Object)} for
   * <ul>
   *   <li>key with unequal value present in {@code Store}</li>
   *   <li>key not present via {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testReplaceValueUnequalStoreEntryNoCacheLoaderWriterEntry() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "unequalValue"));
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeWriter = new FakeCacheLoaderWriter(Collections.<String, String>emptyMap());
    final Ehcache<String, String> ehcache = this.getEhcache(fakeWriter);

    assertFalse(ehcache.replace("key", "oldValue", "newValue"));
    verify(this.store).compute(eq("key"), getAnyBiFunction(), getBooleanNullaryFunction());
    verifyZeroInteractions(this.spiedResilienceStrategy);
    assertThat(fakeStore.getEntryMap().get("key"), is(equalTo("unequalValue")));
    // Broken initial state: CacheLoaderWriter check omitted
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ReplaceOutcome.MISS_PRESENT));
  }

  /**
   * Tests the effect of a {@link org.ehcache.Ehcache#replace(Object, Object, Object)} for
   * <ul>
   *   <li>key with equal value present in {@code Store}</li>
   *   <li>key not present via {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testReplaceValueEqualStoreEntryNoCacheLoaderWriterEntry() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "oldValue"));
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeWriter = new FakeCacheLoaderWriter(Collections.<String, String>emptyMap());
    final Ehcache<String, String> ehcache = this.getEhcache(fakeWriter);

    assertTrue(ehcache.replace("key", "oldValue", "newValue"));
    verify(this.store).compute(eq("key"), getAnyBiFunction(), getBooleanNullaryFunction());
    verifyZeroInteractions(this.spiedResilienceStrategy);
    assertThat(fakeStore.getEntryMap().get("key"), is(equalTo("newValue")));
    // Broken initial state: CacheLoaderWriter check omitted
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ReplaceOutcome.HIT));
  }

  /**
   * Tests the effect of a {@link org.ehcache.Ehcache#replace(Object, Object, Object)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   *   <li>>{@code Store.compute} throws</li>
   *   <li>key not present via {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testReplaceValueNoStoreEntryCacheAccessExceptionNoCacheLoaderWriterEntry() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.<String, String>emptyMap());
    this.store = spy(fakeStore);
    doThrow(new CacheAccessException("")).when(this.store).compute(eq("key"), getAnyBiFunction(), getBooleanNullaryFunction());

    final FakeCacheLoaderWriter fakeWriter = new FakeCacheLoaderWriter(Collections.<String, String>emptyMap());
    this.cacheLoaderWriter = spy(fakeWriter);
    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    ehcache.replace("key", "oldValue", "newValue");
    verify(this.store).compute(eq("key"), getAnyBiFunction(), getBooleanNullaryFunction());
    verify(this.spiedResilienceStrategy)
        .replaceFailure(eq("key"), eq("oldValue"), eq("newValue"), any(CacheAccessException.class), eq(false));
    assertThat(fakeWriter.getEntryMap().containsKey("key"), is(false));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ReplaceOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link org.ehcache.Ehcache#replace(Object, Object, Object)} for
   * <ul>
   *   <li>key with unequal value present in {@code Store}</li>
   *   <li>>{@code Store.compute} throws</li>
   *   <li>key not present via {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testReplaceValueUnequalStoreEntryCacheAccessExceptionNoCacheLoaderWriterEntry() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "unequalValue"));
    this.store = spy(fakeStore);
    doThrow(new CacheAccessException("")).when(this.store).compute(eq("key"), getAnyBiFunction(), getBooleanNullaryFunction());

    final FakeCacheLoaderWriter fakeWriter = new FakeCacheLoaderWriter(Collections.<String, String>emptyMap());
    this.cacheLoaderWriter = spy(fakeWriter);
    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    ehcache.replace("key", "oldValue", "newValue");
    verify(this.store).compute(eq("key"), getAnyBiFunction(), getBooleanNullaryFunction());
    verify(this.spiedResilienceStrategy)
        .replaceFailure(eq("key"), eq("oldValue"), eq("newValue"), any(CacheAccessException.class), eq(false));
    // Broken initial state: CacheLoaderWriter check omitted
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ReplaceOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link org.ehcache.Ehcache#replace(Object, Object, Object)} for
   * <ul>
   *   <li>key with equal value present in {@code Store}</li>
   *   <li>>{@code Store.compute} throws</li>
   *   <li>key not present via {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testReplaceValueEqualStoreEntryCacheAccessExceptionNoCacheLoaderWriterEntry() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "oldValue"));
    this.store = spy(fakeStore);
    doThrow(new CacheAccessException("")).when(this.store).compute(eq("key"), getAnyBiFunction(), getBooleanNullaryFunction());

    final FakeCacheLoaderWriter fakeWriter = new FakeCacheLoaderWriter(Collections.<String, String>emptyMap());
    this.cacheLoaderWriter = spy(fakeWriter);
    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    ehcache.replace("key", "oldValue", "newValue");
    verify(this.store).compute(eq("key"), getAnyBiFunction(), getBooleanNullaryFunction());
    verify(this.spiedResilienceStrategy)
        .replaceFailure(eq("key"), eq("oldValue"), eq("newValue"), any(CacheAccessException.class), eq(false));
    // Broken initial state: CacheLoaderWriter check omitted
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ReplaceOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link org.ehcache.Ehcache#replace(Object, Object, Object)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   *   <li>key with unequal value present via {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testReplaceValueNoStoreEntryUnequalCacheLoaderWriterEntry() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.<String, String>emptyMap());
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeWriter = new FakeCacheLoaderWriter(Collections.singletonMap("key", "unequalValue"));
    final Ehcache<String, String> ehcache = this.getEhcache(fakeWriter);

    assertFalse(ehcache.replace("key", "oldValue", "newValue"));
    verify(this.store).compute(eq("key"), getAnyBiFunction(), getBooleanNullaryFunction());
    verifyZeroInteractions(this.spiedResilienceStrategy);
    assertThat(fakeStore.getEntryMap().get("key"), is("unequalValue"));
    assertThat(fakeWriter.getEntryMap().get("key"), is(equalTo("unequalValue")));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ReplaceOutcome.MISS_PRESENT));
  }

  /**
   * Tests the effect of a {@link org.ehcache.Ehcache#replace(Object, Object, Object)} for
   * <ul>
   *   <li>key with unequal value present in {@code Store}</li>
   *   <li>key with unequal value present via {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testReplaceValueUnequalStoreEntryUnequalCacheLoaderWriterEntry() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "unequalValue"));
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeWriter = new FakeCacheLoaderWriter(Collections.singletonMap("key", "unequalValue"));
    final Ehcache<String, String> ehcache = this.getEhcache(fakeWriter);

    assertFalse(ehcache.replace("key", "oldValue", "newValue"));
    verify(this.store).compute(eq("key"), getAnyBiFunction(), getBooleanNullaryFunction());
    verifyZeroInteractions(this.spiedResilienceStrategy);
    assertThat(fakeStore.getEntryMap().get("key"), is(equalTo("unequalValue")));
    assertThat(fakeWriter.getEntryMap().get("key"), is(equalTo("unequalValue")));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ReplaceOutcome.MISS_PRESENT));
  }

  /**
   * Tests the effect of a {@link org.ehcache.Ehcache#replace(Object, Object, Object)} for
   * <ul>
   *   <li>key with equal value present in {@code Store}</li>
   *   <li>key with unequal value present via {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testReplaceValueEqualStoreEntryUnequalCacheLoaderWriterEntry() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "oldValue"));
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeWriter = new FakeCacheLoaderWriter(Collections.singletonMap("key", "unequalValue"));
    final Ehcache<String, String> ehcache = this.getEhcache(fakeWriter);

    assertTrue(ehcache.replace("key", "oldValue", "newValue"));
    verify(this.store).compute(eq("key"), getAnyBiFunction(), getBooleanNullaryFunction());
    verifyZeroInteractions(this.spiedResilienceStrategy);
    assertThat(fakeStore.getEntryMap().get("key"), is(equalTo("newValue")));
    // Broken initial state: CacheLoaderWriter check omitted
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ReplaceOutcome.HIT));
  }

  /**
   * Tests the effect of a {@link org.ehcache.Ehcache#replace(Object, Object, Object)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   *   <li>>{@code Store.compute} throws</li>
   *   <li>key with unequal value present via {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testReplaceValueNoStoreEntryCacheAccessExceptionUnequalCacheLoaderWriterEntry() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.<String, String>emptyMap());
    this.store = spy(fakeStore);
    doThrow(new CacheAccessException("")).when(this.store).compute(eq("key"), getAnyBiFunction(), getBooleanNullaryFunction());

    final FakeCacheLoaderWriter fakeWriter = new FakeCacheLoaderWriter(Collections.singletonMap("key", "unequalValue"));
    this.cacheLoaderWriter = spy(fakeWriter);
    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    ehcache.replace("key", "oldValue", "newValue");
    verify(this.store).compute(eq("key"), getAnyBiFunction(), getBooleanNullaryFunction());
    verify(this.spiedResilienceStrategy)
        .replaceFailure(eq("key"), eq("oldValue"), eq("newValue"), any(CacheAccessException.class), eq(false));
    assertThat(fakeWriter.getEntryMap().get("key"), is(equalTo("unequalValue")));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ReplaceOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link org.ehcache.Ehcache#replace(Object, Object, Object)} for
   * <ul>
   *   <li>key with unequal value present in {@code Store}</li>
   *   <li>>{@code Store.compute} throws</li>
   *   <li>key with unequal value present via {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testReplaceValueUnequalStoreEntryCacheAccessExceptionUnequalCacheLoaderWriterEntry() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "unequalValue"));
    this.store = spy(fakeStore);
    doThrow(new CacheAccessException("")).when(this.store).compute(eq("key"), getAnyBiFunction(), getBooleanNullaryFunction());

    final FakeCacheLoaderWriter fakeWriter = new FakeCacheLoaderWriter(Collections.singletonMap("key", "unequalValue"));
    this.cacheLoaderWriter = spy(fakeWriter);
    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    ehcache.replace("key", "oldValue", "newValue");
    verify(this.store).compute(eq("key"), getAnyBiFunction(), getBooleanNullaryFunction());
    verify(this.spiedResilienceStrategy)
        .replaceFailure(eq("key"), eq("oldValue"), eq("newValue"), any(CacheAccessException.class), eq(false));
    assertThat(fakeWriter.getEntryMap().get("key"), is(equalTo("unequalValue")));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ReplaceOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link org.ehcache.Ehcache#replace(Object, Object, Object)} for
   * <ul>
   *   <li>key with equal value present in {@code Store}</li>
   *   <li>>{@code Store.compute} throws</li>
   *   <li>key with unequal value present via {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testReplaceValueEqualStoreEntryCacheAccessExceptionUnequalCacheLoaderWriterEntry() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "oldValue"));
    this.store = spy(fakeStore);
    doThrow(new CacheAccessException("")).when(this.store).compute(eq("key"), getAnyBiFunction(), getBooleanNullaryFunction());

    final FakeCacheLoaderWriter fakeWriter = new FakeCacheLoaderWriter(Collections.singletonMap("key", "unequalValue"));
    this.cacheLoaderWriter = spy(fakeWriter);
    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    ehcache.replace("key", "oldValue", "newValue");
    verify(this.store).compute(eq("key"), getAnyBiFunction(), getBooleanNullaryFunction());
    verify(this.spiedResilienceStrategy)
        .replaceFailure(eq("key"), eq("oldValue"), eq("newValue"), any(CacheAccessException.class), eq(false));
    // Broken initial state: CacheLoaderWriter check omitted
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ReplaceOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link org.ehcache.Ehcache#replace(Object, Object, Object)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   *   <li>key with equal value present via {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testReplaceValueNoStoreEntryEqualCacheLoaderWriterEntry() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.<String, String>emptyMap());
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeWriter = new FakeCacheLoaderWriter(Collections.singletonMap("key", "oldValue"));
    final Ehcache<String, String> ehcache = this.getEhcache(fakeWriter);

    assertThat(ehcache.replace("key", "oldValue", "newValue"), is(true));
    verify(this.store).compute(eq("key"), getAnyBiFunction(), getBooleanNullaryFunction());
    verifyZeroInteractions(this.spiedResilienceStrategy);
    assertThat(fakeStore.getEntryMap().get("key"), is("newValue"));
    assertThat(fakeWriter.getEntryMap().get("key"), is(equalTo("newValue")));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ReplaceOutcome.HIT));
  }

  /**
   * Tests the effect of a {@link org.ehcache.Ehcache#replace(Object, Object, Object)} for
   * <ul>
   *   <li>key with unequal value present in {@code Store}</li>
   *   <li>key with equal value present via {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testReplaceValueUnequalStoreEntryEqualCacheLoaderWriterEntry() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "unequalValue"));
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeWriter = new FakeCacheLoaderWriter(Collections.singletonMap("key", "oldValue"));
    final Ehcache<String, String> ehcache = this.getEhcache(fakeWriter);

    assertFalse(ehcache.replace("key", "oldValue", "newValue"));
    verify(this.store).compute(eq("key"), getAnyBiFunction(), getBooleanNullaryFunction());
    verifyZeroInteractions(this.spiedResilienceStrategy);
    assertThat(fakeStore.getEntryMap().get("key"), is(equalTo("unequalValue")));
    // Broken initial state: CacheLoaderWriter check omitted
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ReplaceOutcome.MISS_PRESENT));
  }

  /**
   * Tests the effect of a {@link org.ehcache.Ehcache#replace(Object, Object, Object)} for
   * <ul>
   *   <li>key with equal value present in {@code Store}</li>
   *   <li>key with equal value present via {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testReplaceValueEqualStoreEntryEqualCacheLoaderWriterEntry() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "oldValue"));
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeWriter = new FakeCacheLoaderWriter(Collections.singletonMap("key", "oldValue"));
    final Ehcache<String, String> ehcache = this.getEhcache(fakeWriter);

    assertTrue(ehcache.replace("key", "oldValue", "newValue"));
    verify(this.store).compute(eq("key"), getAnyBiFunction(), getBooleanNullaryFunction());
    verifyZeroInteractions(this.spiedResilienceStrategy);
    assertThat(fakeStore.getEntryMap().get("key"), is(equalTo("newValue")));
    assertThat(fakeWriter.getEntryMap().get("key"), is(equalTo("newValue")));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ReplaceOutcome.HIT));
  }

  /**
   * Tests the effect of a {@link org.ehcache.Ehcache#replace(Object, Object, Object)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   *   <li>>{@code Store.compute} throws</li>
   *   <li>key with equal value present via {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testReplaceValueNoStoreEntryCacheAccessExceptionEqualCacheLoaderWriterEntry() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.<String, String>emptyMap());
    this.store = spy(fakeStore);
    doThrow(new CacheAccessException("")).when(this.store).compute(eq("key"), getAnyBiFunction(), getBooleanNullaryFunction());

    final FakeCacheLoaderWriter fakeWriter = new FakeCacheLoaderWriter(Collections.singletonMap("key", "oldValue"));
    this.cacheLoaderWriter = spy(fakeWriter);
    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    ehcache.replace("key", "oldValue", "newValue");
    verify(this.store).compute(eq("key"), getAnyBiFunction(), getBooleanNullaryFunction());
    verify(this.spiedResilienceStrategy)
        .replaceFailure(eq("key"), eq("oldValue"), eq("newValue"), any(CacheAccessException.class), eq(true));
    assertThat(fakeWriter.getEntryMap().get("key"), is(equalTo("newValue")));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ReplaceOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link org.ehcache.Ehcache#replace(Object, Object, Object)} for
   * <ul>
   *   <li>key with unequal value present in {@code Store}</li>
   *   <li>>{@code Store.compute} throws</li>
   *   <li>key with equal value present via {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testReplaceValueUnequalStoreEntryCacheAccessExceptionEqualCacheLoaderWriterEntry() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "unequalValue"));
    this.store = spy(fakeStore);
    doThrow(new CacheAccessException("")).when(this.store).compute(eq("key"), getAnyBiFunction(), getBooleanNullaryFunction());

    final FakeCacheLoaderWriter fakeWriter = new FakeCacheLoaderWriter(Collections.singletonMap("key", "oldValue"));
    this.cacheLoaderWriter = spy(fakeWriter);
    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    ehcache.replace("key", "oldValue", "newValue");
    verify(this.store).compute(eq("key"), getAnyBiFunction(), getBooleanNullaryFunction());
    verify(this.spiedResilienceStrategy)
        .replaceFailure(eq("key"), eq("oldValue"), eq("newValue"), any(CacheAccessException.class), eq(true));
    // Broken initial state: CacheLoaderWriter check omitted
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ReplaceOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link org.ehcache.Ehcache#replace(Object, Object, Object)} for
   * <ul>
   *   <li>key with equal value present in {@code Store}</li>
   *   <li>>{@code Store.compute} throws</li>
   *   <li>key with equal value present via {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testReplaceValueEqualStoreEntryCacheAccessExceptionEqualCacheLoaderWriterEntry() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "oldValue"));
    this.store = spy(fakeStore);
    doThrow(new CacheAccessException("")).when(this.store).compute(eq("key"), getAnyBiFunction(), getBooleanNullaryFunction());

    final FakeCacheLoaderWriter fakeWriter = new FakeCacheLoaderWriter(Collections.singletonMap("key", "oldValue"));
    this.cacheLoaderWriter = spy(fakeWriter);
    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    ehcache.replace("key", "oldValue", "newValue");
    verify(this.store).compute(eq("key"), getAnyBiFunction(), getBooleanNullaryFunction());
    verify(this.spiedResilienceStrategy)
        .replaceFailure(eq("key"), eq("oldValue"), eq("newValue"), any(CacheAccessException.class), eq(true));
    assertThat(fakeWriter.getEntryMap().get("key"), is(equalTo("newValue")));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ReplaceOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link org.ehcache.Ehcache#replace(Object, Object, Object)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   *   <li>{@code CacheLoaderWriter.write} throws</li>
   * </ul>
   */
  @Test
  public void testReplaceValueNoStoreEntryCacheLoaderWriterException() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.<String, String>emptyMap());
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeWriter = new FakeCacheLoaderWriter(Collections.singletonMap("key", "oldValue"));
    this.cacheLoaderWriter = spy(fakeWriter);
    doThrow(new Exception()).when(this.cacheLoaderWriter).write("key", "newValue");
    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    try {
      ehcache.replace("key", "oldValue", "newValue");
      fail();
    } catch (CacheWritingException e) {
      // expected
    }
    verify(this.store).compute(eq("key"), getAnyBiFunction(), getBooleanNullaryFunction());
    verifyZeroInteractions(this.spiedResilienceStrategy);
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.ReplaceOutcome.class));
  }

  /**
   * Tests the effect of a {@link org.ehcache.Ehcache#replace(Object, Object, Object)} for
   * <ul>
   *   <li>key with unequal value present in {@code Store}</li>
   *   <li>{@code CacheLoaderWriter.write} throws</li>
   * </ul>
   */
  @Test
  public void testReplaceValueUnequalStoreEntryCacheLoaderWriterException() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "unequalValue"));
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeWriter = new FakeCacheLoaderWriter(Collections.singletonMap("key", "oldValue"));
    this.cacheLoaderWriter = spy(fakeWriter);
    doThrow(new Exception()).when(this.cacheLoaderWriter).write("key", "newValue");
    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    assertFalse(ehcache.replace("key", "oldValue", "newValue"));
    verify(this.store).compute(eq("key"), getAnyBiFunction(), getBooleanNullaryFunction());
    verifyZeroInteractions(this.spiedResilienceStrategy);
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ReplaceOutcome.MISS_PRESENT));
  }

  /**
   * Tests the effect of a {@link org.ehcache.Ehcache#replace(Object, Object, Object)} for
   * <ul>
   *   <li>key with equal value present in {@code Store}</li>
   *   <li>{@code CacheLoaderWriter.write} throws</li>
   * </ul>
   */
  @Test
  public void testReplaceValueEqualStoreEntryCacheLoaderWriterException() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "oldValue"));
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeWriter = new FakeCacheLoaderWriter(Collections.singletonMap("key", "oldValue"));
    this.cacheLoaderWriter = spy(fakeWriter);
    doThrow(new Exception()).when(this.cacheLoaderWriter).write("key", "newValue");
    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    try {
      ehcache.replace("key", "oldValue", "newValue");
      fail();
    } catch (CacheWritingException e) {
      // Expected
    }
    verify(this.store).compute(eq("key"), getAnyBiFunction(), getBooleanNullaryFunction());
    verifyZeroInteractions(this.spiedResilienceStrategy);
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.ReplaceOutcome.class));    // TODO: Confirm correctness
  }

  /**
   * Tests the effect of a {@link org.ehcache.Ehcache#replace(Object, Object, Object)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   *   <li>>{@code Store.compute} throws</li>
   *   <li>{@code CacheLoaderWriter.write} throws</li>
   * </ul>
   */
  @Test
  public void testReplaceValueNoStoreEntryCacheAccessExceptionCacheLoaderWriterException() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.<String, String>emptyMap());
    this.store = spy(fakeStore);
    doThrow(new CacheAccessException("")).when(this.store).compute(eq("key"), getAnyBiFunction(), getBooleanNullaryFunction());

    final FakeCacheLoaderWriter fakeWriter = new FakeCacheLoaderWriter(Collections.singletonMap("key", "oldValue"));
    this.cacheLoaderWriter = spy(fakeWriter);
    doThrow(new Exception()).when(this.cacheLoaderWriter).write("key", "newValue");
    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    try {
      ehcache.replace("key", "oldValue", "newValue");
      fail();
    } catch (CacheWritingException e) {
      // expected
    }
    verify(this.store).compute(eq("key"), getAnyBiFunction(), getBooleanNullaryFunction());
    verify(this.spiedResilienceStrategy)
        .replaceFailure(eq("key"), eq("oldValue"), eq("newValue"), any(CacheAccessException.class), any(CacheWritingException.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ReplaceOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link org.ehcache.Ehcache#replace(Object, Object, Object)} for
   * <ul>
   *   <li>key with unequal value present in {@code Store}</li>
   *   <li>>{@code Store.compute} throws</li>
   *   <li>{@code CacheLoaderWriter.write} throws</li>
   * </ul>
   */
  @Test
  public void testReplaceValueUnequalStoreEntryCacheAccessExceptionCacheLoaderWriterException() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "unequalValue"));
    this.store = spy(fakeStore);
    doThrow(new CacheAccessException("")).when(this.store).compute(eq("key"), getAnyBiFunction(), getBooleanNullaryFunction());

    final FakeCacheLoaderWriter fakeWriter = new FakeCacheLoaderWriter(Collections.singletonMap("key", "oldValue"));
    this.cacheLoaderWriter = spy(fakeWriter);
    doThrow(new Exception()).when(this.cacheLoaderWriter).write("key", "newValue");
    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    try {
      ehcache.replace("key", "oldValue", "newValue");
      fail();
    } catch (CacheWritingException e) {
      // expected
    }
    verify(this.store).compute(eq("key"), getAnyBiFunction(), getBooleanNullaryFunction());
    verify(this.spiedResilienceStrategy)
        .replaceFailure(eq("key"), eq("oldValue"), eq("newValue"), any(CacheAccessException.class), any(CacheWritingException.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ReplaceOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link org.ehcache.Ehcache#replace(Object, Object, Object)} for
   * <ul>
   *   <li>key with equal value present in {@code Store}</li>
   *   <li>>{@code Store.compute} throws</li>
   *   <li>{@code CacheLoaderWriter.write} throws</li>
   * </ul>
   */
  @Test
  public void testReplaceValueEqualStoreEntryCacheAccessExceptionCacheLoaderWriterException() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "oldValue"));
    this.store = spy(fakeStore);
    doThrow(new CacheAccessException("")).when(this.store).compute(eq("key"), getAnyBiFunction(), getBooleanNullaryFunction());

    final FakeCacheLoaderWriter fakeWriter = new FakeCacheLoaderWriter(Collections.singletonMap("key", "oldValue"));
    this.cacheLoaderWriter = spy(fakeWriter);
    doThrow(new Exception()).when(this.cacheLoaderWriter).write("key", "newValue");
    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    try {
      ehcache.replace("key", "oldValue", "newValue");
      fail();
    } catch (CacheWritingException e) {
      // Expected
    }
    verify(this.store).compute(eq("key"), getAnyBiFunction(), getBooleanNullaryFunction());
    verify(this.spiedResilienceStrategy)
        .replaceFailure(eq("key"), eq("oldValue"), eq("newValue"), any(CacheAccessException.class), any(CacheWritingException.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ReplaceOutcome.FAILURE));
  }

  @Test
  public void testReplaceWithImmediatelyExpiredEntry() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.<String, String>singletonMap("key", "old-value"));
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeWriter = new FakeCacheLoaderWriter(Collections.<String, String>singletonMap("key", "old-value"));
    
    final Expiry<String, String> expiry = mock(Expiry.class);
    when(expiry.getExpiryForUpdate("key", "old-value", "value")).thenReturn(Duration.ZERO);
    
    final Ehcache<String, String> ehcache = this.getEhcache(fakeWriter, expiry);

    ehcache.replace("key", "old-value", "value");
    verify(this.store).compute(eq("key"), getAnyBiFunction(), getBooleanNullaryFunction());
    verifyZeroInteractions(this.spiedResilienceStrategy);
    assertThat(fakeStore.getEntryMap().get("key"), nullValue());
    assertThat(fakeWriter.getEntryMap().get("key"), equalTo("value"));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ReplaceOutcome.HIT));
  }

  /**
   * Gets an initialized {@link Ehcache Ehcache} instance using the
   * {@link org.ehcache.spi.loaderwriter.CacheLoaderWriter CacheLoaderWriter} provided.
   *
   * @param cacheLoaderWriter
   *    the {@code CacheLoaderWriter} to use; may be {@code null}
   *
   * @return a new {@code Ehcache} instance
   */
  private Ehcache<String, String> getEhcache(final CacheLoaderWriter<String, String> cacheLoaderWriter) {
    return getEhcache(cacheLoaderWriter, Expirations.noExpiration());
  }

  private Ehcache<String, String> getEhcache(final CacheLoaderWriter<String, String> cacheLoaderWriter, Expiry<? super String, ? super String> expiry) {
    CacheConfiguration<String, String> config = new BaseCacheConfiguration<String, String>(String.class, String.class, null, null, expiry, ResourcePoolsHelper.createHeapOnlyPools());
    final Ehcache<String, String> ehcache = new Ehcache<String, String>(config, this.store, cacheLoaderWriter, cacheEventDispatcher, LoggerFactory.getLogger(Ehcache.class + "-" + "EhcacheBasicReplaceValueTest"));
    ehcache.init();
    assertThat("cache not initialized", ehcache.getStatus(), is(Status.AVAILABLE));
    this.spiedResilienceStrategy = this.setResilienceStrategySpy(ehcache);
    return ehcache;
  }
}
