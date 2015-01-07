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
import org.ehcache.exceptions.CacheWriterException;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.ehcache.statistics.CacheOperationOutcomes;
import org.junit.Test;
import org.mockito.Mock;

import java.util.Collections;
import java.util.EnumSet;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

/**
 * Provides testing of basic REMOVE(key, value) operations on an {@code Ehcache}.
 *
 * @author Clifford W. Johnson
 */
public class EhcacheBasicRemoveValueTest extends EhcacheBasicCrudBase {

  @Mock
  protected CacheLoaderWriter<String, String> cacheLoaderWriter;

  @Test
  public void testRemoveNullNull() {
    final Ehcache<String, String> ehcache = this.getEhcache(null);

    try {
      ehcache.remove(null, null);
      fail();
    } catch (NullPointerException e) {
      // expected
    }
  }

  @Test
  public void testRemoveKeyNull() throws Exception {
    final Ehcache<String, String> ehcache = this.getEhcache(null);

    try {
      ehcache.remove("key", null);
      fail();
    } catch (NullPointerException e) {
      // expected
    }
  }

  @Test
  public void testRemoveNullValue() throws Exception {
    final Ehcache<String, String> ehcache = this.getEhcache(null);

    try {
      ehcache.remove(null, "value");
      fail();
    } catch (NullPointerException e) {
      // expected
    }
  }

  /**
   * Tests the effect of a {@link org.ehcache.Ehcache#remove(Object, Object)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   *   <li>no {@code CacheWriter}</li>
   * </ul>
   */
  @Test
  public void testRemoveValueNoStoreEntryNoCacheWriter() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.<String, String>emptyMap());
    this.store = spy(fakeStore);

    final Ehcache<String, String> ehcache = this.getEhcache(null);

    assertFalse(ehcache.remove("key", "value"));
    verify(this.store).computeIfPresent(eq("key"), getAnyBiFunction(), getBooleanNullaryFunction());
    verifyZeroInteractions(this.spiedResilienceStrategy);
    assertThat(fakeStore.getEntryMap().containsKey("key"), is(false));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ConditionalRemoveOutcome.FAILURE_KEY_MISSING));
  }

  /**
   * Tests the effect of a {@link org.ehcache.Ehcache#remove(Object, Object)} for
   * <ul>
   *   <li>key with unequal value in {@code Store}</li>
   *   <li>no {@code CacheWriter}</li>
   * </ul>
   */
  @Test
  public void testRemoveValueUnequalStoreEntryNoCacheWriter() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "unequalValue"));
    this.store = spy(fakeStore);

    final Ehcache<String, String> ehcache = this.getEhcache(null);

    assertFalse(ehcache.remove("key", "value"));
    verify(this.store).computeIfPresent(eq("key"), getAnyBiFunction(), getBooleanNullaryFunction());
    verifyZeroInteractions(this.spiedResilienceStrategy);
    assertThat(fakeStore.getEntryMap().get("key"), is(equalTo("unequalValue")));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ConditionalRemoveOutcome.FAILURE_KEY_PRESENT));
  }

  /**
   * Tests the effect of a {@link org.ehcache.Ehcache#remove(Object, Object)} for
   * <ul>
   *   <li>key with equal value in {@code Store}</li>
   *   <li>no {@code CacheWriter}</li>
   * </ul>
   */
  @Test
  public void testRemoveValueEqualStoreEntryNoCacheWriter() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "value"));
    this.store = spy(fakeStore);

    final Ehcache<String, String> ehcache = this.getEhcache(null);

    assertTrue(ehcache.remove("key", "value"));
    verify(this.store).computeIfPresent(eq("key"), getAnyBiFunction(), getBooleanNullaryFunction());
    verifyZeroInteractions(this.spiedResilienceStrategy);
    assertThat(fakeStore.getEntryMap().containsKey("key"), is(false));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ConditionalRemoveOutcome.SUCCESS));
  }

  /**
   * Tests the effect of a {@link org.ehcache.Ehcache#remove(Object, Object)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   *   <li>>{@code Store.computeIfPresent} throws</li>
   *   <li>no {@code CacheWriter}</li>
   * </ul>
   */
  @Test
  public void testRemoveValueNoStoreEntryCacheAccessExceptionNoCacheWriter() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.<String, String>emptyMap());
    this.store = spy(fakeStore);
    doThrow(new CacheAccessException("")).when(this.store).computeIfPresent(eq("key"), getAnyBiFunction(), getBooleanNullaryFunction());

    final Ehcache<String, String> ehcache = this.getEhcache(null);

    ehcache.remove("key", "value");
    verify(this.store).computeIfPresent(eq("key"), getAnyBiFunction(), getBooleanNullaryFunction());
    verify(this.spiedResilienceStrategy)
        .removeFailure(eq("key"), eq("value"), any(CacheAccessException.class), eq(false));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ConditionalRemoveOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link org.ehcache.Ehcache#remove(Object, Object)} for
   * <ul>
   *   <li>key with unequal value present in {@code Store}</li>
   *   <li>>{@code Store.computeIfPresent} throws</li>
   *   <li>no {@code CacheWriter}</li>
   * </ul>
   */
  @Test
  public void testRemoveValueUnequalStoreEntryCacheAccessExceptionNoCacheWriter() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "unequalValue"));
    this.store = spy(fakeStore);
    doThrow(new CacheAccessException("")).when(this.store).computeIfPresent(eq("key"), getAnyBiFunction(), getBooleanNullaryFunction());

    final Ehcache<String, String> ehcache = this.getEhcache(null);

    ehcache.remove("key", "value");
    verify(this.store).computeIfPresent(eq("key"), getAnyBiFunction(), getBooleanNullaryFunction());
    verify(this.spiedResilienceStrategy)
        .removeFailure(eq("key"), eq("value"), any(CacheAccessException.class), eq(false));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ConditionalRemoveOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link org.ehcache.Ehcache#remove(Object, Object)} for
   * <ul>
   *   <li>key with equal value present in {@code Store}</li>
   *   <li>>{@code Store.computeIfPresent} throws</li>
   *   <li>no {@code CacheWriter}</li>
   * </ul>
   */
  @Test
  public void testRemoveValueEqualStoreEntryCacheAccessExceptionNoCacheWriter() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "value"));
    this.store = spy(fakeStore);
    doThrow(new CacheAccessException("")).when(this.store).computeIfPresent(eq("key"), getAnyBiFunction(), getBooleanNullaryFunction());

    final Ehcache<String, String> ehcache = this.getEhcache(null);

    ehcache.remove("key", "value");
    verify(this.store).computeIfPresent(eq("key"), getAnyBiFunction(), getBooleanNullaryFunction());
    verify(this.spiedResilienceStrategy)
        .removeFailure(eq("key"), eq("value"), any(CacheAccessException.class), eq(false));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ConditionalRemoveOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link org.ehcache.Ehcache#remove(Object, Object)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   *   <li>key not present via {@code CacheWriter}</li>
   * </ul>
   */
  @Test
  public void testRemoveValueNoStoreEntryNoCacheWriterEntry() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.<String, String>emptyMap());
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeWriter = new FakeCacheLoaderWriter(Collections.<String, String>emptyMap());
    final Ehcache<String, String> ehcache = this.getEhcache(fakeWriter);

    assertFalse(ehcache.remove("key", "value"));
    verify(this.store).computeIfPresent(eq("key"), getAnyBiFunction(), getBooleanNullaryFunction());
    verifyZeroInteractions(this.spiedResilienceStrategy);
    assertThat(fakeStore.getEntryMap().containsKey("key"), is(false));
    assertThat(fakeWriter.getEntryMap().containsKey("key"), is(false));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ConditionalRemoveOutcome.FAILURE_KEY_MISSING));
  }

  /**
   * Tests the effect of a {@link org.ehcache.Ehcache#remove(Object, Object)} for
   * <ul>
   *   <li>key with unequal value present in {@code Store}</li>
   *   <li>key not present via {@code CacheWriter}</li>
   * </ul>
   */
  @Test
  public void testRemoveValueUnequalStoreEntryNoCacheWriterEntry() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "unequalValue"));
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeWriter = new FakeCacheLoaderWriter(Collections.<String, String>emptyMap());
    final Ehcache<String, String> ehcache = this.getEhcache(fakeWriter);

    assertFalse(ehcache.remove("key", "value"));
    verify(this.store).computeIfPresent(eq("key"), getAnyBiFunction(), getBooleanNullaryFunction());
    verifyZeroInteractions(this.spiedResilienceStrategy);
    assertThat(fakeStore.getEntryMap().get("key"), is(equalTo("unequalValue")));
    // Broken initial state: CacheWriter check omitted
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ConditionalRemoveOutcome.FAILURE_KEY_PRESENT));
  }

  /**
   * Tests the effect of a {@link org.ehcache.Ehcache#remove(Object, Object)} for
   * <ul>
   *   <li>key with equal value present in {@code Store}</li>
   *   <li>key not present via {@code CacheWriter}</li>
   * </ul>
   */
  @Test
  public void testRemoveValueEqualStoreEntryNoCacheWriterEntry() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "value"));
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeWriter = new FakeCacheLoaderWriter(Collections.<String, String>emptyMap());
    final Ehcache<String, String> ehcache = this.getEhcache(fakeWriter);

    assertTrue(ehcache.remove("key", "value"));
    verify(this.store).computeIfPresent(eq("key"), getAnyBiFunction(), getBooleanNullaryFunction());
    verifyZeroInteractions(this.spiedResilienceStrategy);
    assertThat(fakeStore.getEntryMap().containsKey("key"), is(false));
    // Broken initial state: CacheWriter check omitted
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ConditionalRemoveOutcome.SUCCESS));
  }

  /**
   * Tests the effect of a {@link org.ehcache.Ehcache#remove(Object, Object)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   *   <li>>{@code Store.computeIfPresent} throws</li>
   *   <li>key not present via {@code CacheWriter}</li>
   * </ul>
   */
  @Test
  public void testRemoveValueNoStoreEntryCacheAccessExceptionNoCacheWriterEntry() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.<String, String>emptyMap());
    this.store = spy(fakeStore);
    doThrow(new CacheAccessException("")).when(this.store).computeIfPresent(eq("key"), getAnyBiFunction(), getBooleanNullaryFunction());

    final FakeCacheLoaderWriter fakeWriter = new FakeCacheLoaderWriter(Collections.<String, String>emptyMap());
    this.cacheLoaderWriter = spy(fakeWriter);
    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    ehcache.remove("key", "value");
    verify(this.store).computeIfPresent(eq("key"), getAnyBiFunction(), getBooleanNullaryFunction());
    verify(this.spiedResilienceStrategy)
        .removeFailure(eq("key"), eq("value"), any(CacheAccessException.class), eq(false));
    assertThat(fakeWriter.getEntryMap().containsKey("key"), is(false));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ConditionalRemoveOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link org.ehcache.Ehcache#remove(Object, Object)} for
   * <ul>
   *   <li>key with unequal value present in {@code Store}</li>
   *   <li>>{@code Store.computeIfPresent} throws</li>
   *   <li>key not present via {@code CacheWriter}</li>
   * </ul>
   */
  @Test
  public void testRemoveValueUnequalStoreEntryCacheAccessExceptionNoCacheWriterEntry() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "unequalValue"));
    this.store = spy(fakeStore);
    doThrow(new CacheAccessException("")).when(this.store).computeIfPresent(eq("key"), getAnyBiFunction(), getBooleanNullaryFunction());

    final FakeCacheLoaderWriter fakeWriter = new FakeCacheLoaderWriter(Collections.<String, String>emptyMap());
    this.cacheLoaderWriter = spy(fakeWriter);
    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    ehcache.remove("key", "value");
    verify(this.store).computeIfPresent(eq("key"), getAnyBiFunction(), getBooleanNullaryFunction());
    verify(this.spiedResilienceStrategy)
        .removeFailure(eq("key"), eq("value"), any(CacheAccessException.class), eq(false));
    // Broken initial state: CacheWriter check omitted
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ConditionalRemoveOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link org.ehcache.Ehcache#remove(Object, Object)} for
   * <ul>
   *   <li>key with equal value present in {@code Store}</li>
   *   <li>>{@code Store.computeIfPresent} throws</li>
   *   <li>key not present via {@code CacheWriter}</li>
   * </ul>
   */
  @Test
  public void testRemoveValueEqualStoreEntryCacheAccessExceptionNoCacheWriterEntry() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "value"));
    this.store = spy(fakeStore);
    doThrow(new CacheAccessException("")).when(this.store).computeIfPresent(eq("key"), getAnyBiFunction(), getBooleanNullaryFunction());

    final FakeCacheLoaderWriter fakeWriter = new FakeCacheLoaderWriter(Collections.<String, String>emptyMap());
    this.cacheLoaderWriter = spy(fakeWriter);
    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    ehcache.remove("key", "value");
    verify(this.store).computeIfPresent(eq("key"), getAnyBiFunction(), getBooleanNullaryFunction());
    verify(this.spiedResilienceStrategy)
        .removeFailure(eq("key"), eq("value"), any(CacheAccessException.class), eq(false));
    // Broken initial state: CacheWriter check omitted
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ConditionalRemoveOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link org.ehcache.Ehcache#remove(Object, Object)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   *   <li>key with unequal value present via {@code CacheWriter}</li>
   * </ul>
   */
  @Test
  public void testRemoveValueNoStoreEntryUnequalCacheWriterEntry() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.<String, String>emptyMap());
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeWriter = new FakeCacheLoaderWriter(Collections.singletonMap("key", "unequalValue"));
    final Ehcache<String, String> ehcache = this.getEhcache(fakeWriter);

    assertFalse(ehcache.remove("key", "value"));
    verify(this.store).computeIfPresent(eq("key"), getAnyBiFunction(), getBooleanNullaryFunction());
    verifyZeroInteractions(this.spiedResilienceStrategy);
    assertThat(fakeStore.getEntryMap().containsKey("key"), is(false));
    assertThat(fakeWriter.getEntryMap().get("key"), is(equalTo("unequalValue")));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ConditionalRemoveOutcome.FAILURE_KEY_MISSING));
  }

  /**
   * Tests the effect of a {@link org.ehcache.Ehcache#remove(Object, Object)} for
   * <ul>
   *   <li>key with unequal value present in {@code Store}</li>
   *   <li>key with unequal value present via {@code CacheWriter}</li>
   * </ul>
   */
  @Test
  public void testRemoveValueUnequalStoreEntryUnequalCacheWriterEntry() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "unequalValue"));
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeWriter = new FakeCacheLoaderWriter(Collections.singletonMap("key", "unequalValue"));
    final Ehcache<String, String> ehcache = this.getEhcache(fakeWriter);

    assertFalse(ehcache.remove("key", "value"));
    verify(this.store).computeIfPresent(eq("key"), getAnyBiFunction(), getBooleanNullaryFunction());
    verifyZeroInteractions(this.spiedResilienceStrategy);
    assertThat(fakeStore.getEntryMap().get("key"), is(equalTo("unequalValue")));
    assertThat(fakeWriter.getEntryMap().get("key"), is(equalTo("unequalValue")));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ConditionalRemoveOutcome.FAILURE_KEY_PRESENT));
  }

  /**
   * Tests the effect of a {@link org.ehcache.Ehcache#remove(Object, Object)} for
   * <ul>
   *   <li>key with equal value present in {@code Store}</li>
   *   <li>key with unequal value present via {@code CacheWriter}</li>
   * </ul>
   */
  @Test
  public void testRemoveValueEqualStoreEntryUnequalCacheWriterEntry() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "value"));
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeWriter = new FakeCacheLoaderWriter(Collections.singletonMap("key", "unequalValue"));
    final Ehcache<String, String> ehcache = this.getEhcache(fakeWriter);

    assertTrue(ehcache.remove("key", "value"));
    verify(this.store).computeIfPresent(eq("key"), getAnyBiFunction(), getBooleanNullaryFunction());
    verifyZeroInteractions(this.spiedResilienceStrategy);
    assertThat(fakeStore.getEntryMap().containsKey("key"), is(false));
    // Broken initial state: CacheWriter check omitted
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ConditionalRemoveOutcome.SUCCESS));
  }

  /**
   * Tests the effect of a {@link org.ehcache.Ehcache#remove(Object, Object)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   *   <li>>{@code Store.computeIfPresent} throws</li>
   *   <li>key with unequal value present via {@code CacheWriter}</li>
   * </ul>
   */
  @Test
  public void testRemoveValueNoStoreEntryCacheAccessExceptionUnequalCacheWriterEntry() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.<String, String>emptyMap());
    this.store = spy(fakeStore);
    doThrow(new CacheAccessException("")).when(this.store).computeIfPresent(eq("key"), getAnyBiFunction(), getBooleanNullaryFunction());

    final FakeCacheLoaderWriter fakeWriter = new FakeCacheLoaderWriter(Collections.singletonMap("key", "unequalValue"));
    this.cacheLoaderWriter = spy(fakeWriter);
    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    ehcache.remove("key", "value");
    verify(this.store).computeIfPresent(eq("key"), getAnyBiFunction(), getBooleanNullaryFunction());
    verify(this.spiedResilienceStrategy)
        .removeFailure(eq("key"), eq("value"), any(CacheAccessException.class), eq(false));
    assertThat(fakeWriter.getEntryMap().get("key"), is(equalTo("unequalValue")));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ConditionalRemoveOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link org.ehcache.Ehcache#remove(Object, Object)} for
   * <ul>
   *   <li>key with unequal value present in {@code Store}</li>
   *   <li>>{@code Store.computeIfPresent} throws</li>
   *   <li>key with unequal value present via {@code CacheWriter}</li>
   * </ul>
   */
  @Test
  public void testRemoveValueUnequalStoreEntryCacheAccessExceptionUnequalCacheWriterEntry() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "unequalValue"));
    this.store = spy(fakeStore);
    doThrow(new CacheAccessException("")).when(this.store).computeIfPresent(eq("key"), getAnyBiFunction(), getBooleanNullaryFunction());

    final FakeCacheLoaderWriter fakeWriter = new FakeCacheLoaderWriter(Collections.singletonMap("key", "unequalValue"));
    this.cacheLoaderWriter = spy(fakeWriter);
    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    ehcache.remove("key", "value");
    verify(this.store).computeIfPresent(eq("key"), getAnyBiFunction(), getBooleanNullaryFunction());
    verify(this.spiedResilienceStrategy)
        .removeFailure(eq("key"), eq("value"), any(CacheAccessException.class), eq(false));
    assertThat(fakeWriter.getEntryMap().get("key"), is(equalTo("unequalValue")));   // TODO: Confirm correctness
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ConditionalRemoveOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link org.ehcache.Ehcache#remove(Object, Object)} for
   * <ul>
   *   <li>key with equal value present in {@code Store}</li>
   *   <li>>{@code Store.computeIfPresent} throws</li>
   *   <li>key with unequal value present via {@code CacheWriter}</li>
   * </ul>
   */
  @Test
  public void testRemoveValueEqualStoreEntryCacheAccessExceptionUnequalCacheWriterEntry() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "value"));
    this.store = spy(fakeStore);
    doThrow(new CacheAccessException("")).when(this.store).computeIfPresent(eq("key"), getAnyBiFunction(), getBooleanNullaryFunction());

    final FakeCacheLoaderWriter fakeWriter = new FakeCacheLoaderWriter(Collections.singletonMap("key", "unequalValue"));
    this.cacheLoaderWriter = spy(fakeWriter);
    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    ehcache.remove("key", "value");
    verify(this.store).computeIfPresent(eq("key"), getAnyBiFunction(), getBooleanNullaryFunction());
    verify(this.spiedResilienceStrategy)
        .removeFailure(eq("key"), eq("value"), any(CacheAccessException.class), eq(false));
    // Broken initial state: CacheWriter check omitted
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ConditionalRemoveOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link org.ehcache.Ehcache#remove(Object, Object)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   *   <li>key with equal value present via {@code CacheWriter}</li>
   * </ul>
   */
  @Test
  public void testRemoveValueNoStoreEntryEqualCacheWriterEntry() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.<String, String>emptyMap());
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeWriter = new FakeCacheLoaderWriter(Collections.singletonMap("key", "value"));
    final Ehcache<String, String> ehcache = this.getEhcache(fakeWriter);

    assertFalse(ehcache.remove("key", "value"));
    verify(this.store).computeIfPresent(eq("key"), getAnyBiFunction(), getBooleanNullaryFunction());
    verifyZeroInteractions(this.spiedResilienceStrategy);
    assertThat(fakeStore.getEntryMap().containsKey("key"), is(false));
    assertThat(fakeWriter.getEntryMap().containsKey("key"), is(true));    // TODO: Confirm correctness
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ConditionalRemoveOutcome.FAILURE_KEY_MISSING));
  }

  /**
   * Tests the effect of a {@link org.ehcache.Ehcache#remove(Object, Object)} for
   * <ul>
   *   <li>key with unequal value present in {@code Store}</li>
   *   <li>key with equal value present via {@code CacheWriter}</li>
   * </ul>
   */
  @Test
  public void testRemoveValueUnequalStoreEntryEqualCacheWriterEntry() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "unequalValue"));
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeWriter = new FakeCacheLoaderWriter(Collections.singletonMap("key", "value"));
    final Ehcache<String, String> ehcache = this.getEhcache(fakeWriter);

    assertFalse(ehcache.remove("key", "value"));
    verify(this.store).computeIfPresent(eq("key"), getAnyBiFunction(), getBooleanNullaryFunction());
    verifyZeroInteractions(this.spiedResilienceStrategy);
    assertThat(fakeStore.getEntryMap().get("key"), is(equalTo("unequalValue")));
    // Broken initial state: CacheWriter check omitted
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ConditionalRemoveOutcome.FAILURE_KEY_PRESENT));
  }

  /**
   * Tests the effect of a {@link org.ehcache.Ehcache#remove(Object, Object)} for
   * <ul>
   *   <li>key with equal value present in {@code Store}</li>
   *   <li>key with equal value present via {@code CacheWriter}</li>
   * </ul>
   */
  @Test
  public void testRemoveValueEqualStoreEntryEqualCacheWriterEntry() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "value"));
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeWriter = new FakeCacheLoaderWriter(Collections.singletonMap("key", "value"));
    final Ehcache<String, String> ehcache = this.getEhcache(fakeWriter);

    assertTrue(ehcache.remove("key", "value"));
    verify(this.store).computeIfPresent(eq("key"), getAnyBiFunction(), getBooleanNullaryFunction());
    verifyZeroInteractions(this.spiedResilienceStrategy);
    assertThat(fakeStore.getEntryMap().containsKey("key"), is(false));
    assertThat(fakeWriter.getEntryMap().containsKey("key"), is(false));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ConditionalRemoveOutcome.SUCCESS));
  }

  /**
   * Tests the effect of a {@link org.ehcache.Ehcache#remove(Object, Object)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   *   <li>>{@code Store.computeIfPresent} throws</li>
   *   <li>key with equal value present via {@code CacheWriter}</li>
   * </ul>
   */
  @Test
  public void testRemoveValueNoStoreEntryCacheAccessExceptionEqualCacheWriterEntry() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.<String, String>emptyMap());
    this.store = spy(fakeStore);
    doThrow(new CacheAccessException("")).when(this.store).computeIfPresent(eq("key"), getAnyBiFunction(), getBooleanNullaryFunction());

    final FakeCacheLoaderWriter fakeWriter = new FakeCacheLoaderWriter(Collections.singletonMap("key", "value"));
    this.cacheLoaderWriter = spy(fakeWriter);
    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    ehcache.remove("key", "value");
    verify(this.store).computeIfPresent(eq("key"), getAnyBiFunction(), getBooleanNullaryFunction());
    verify(this.spiedResilienceStrategy)
        .removeFailure(eq("key"), eq("value"), any(CacheAccessException.class), eq(false));
    assertThat(fakeWriter.getEntryMap().containsKey("key"), is(true));    // TODO: Confirm correctness
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ConditionalRemoveOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link org.ehcache.Ehcache#remove(Object, Object)} for
   * <ul>
   *   <li>key with unequal value present in {@code Store}</li>
   *   <li>>{@code Store.computeIfPresent} throws</li>
   *   <li>key with equal value present via {@code CacheWriter}</li>
   * </ul>
   */
  @Test
  public void testRemoveValueUnequalStoreEntryCacheAccessExceptionEqualCacheWriterEntry() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "unequalValue"));
    this.store = spy(fakeStore);
    doThrow(new CacheAccessException("")).when(this.store).computeIfPresent(eq("key"), getAnyBiFunction(), getBooleanNullaryFunction());

    final FakeCacheLoaderWriter fakeWriter = new FakeCacheLoaderWriter(Collections.singletonMap("key", "value"));
    this.cacheLoaderWriter = spy(fakeWriter);
    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    ehcache.remove("key", "value");
    verify(this.store).computeIfPresent(eq("key"), getAnyBiFunction(), getBooleanNullaryFunction());
    verify(this.spiedResilienceStrategy)
        .removeFailure(eq("key"), eq("value"), any(CacheAccessException.class), eq(false));
    // Broken initial state: CacheWriter check omitted
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ConditionalRemoveOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link org.ehcache.Ehcache#remove(Object, Object)} for
   * <ul>
   *   <li>key with equal value present in {@code Store}</li>
   *   <li>>{@code Store.computeIfPresent} throws</li>
   *   <li>key with equal value present via {@code CacheWriter}</li>
   * </ul>
   */
  @Test
  public void testRemoveValueEqualStoreEntryCacheAccessExceptionEqualCacheWriterEntry() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "value"));
    this.store = spy(fakeStore);
    doThrow(new CacheAccessException("")).when(this.store).computeIfPresent(eq("key"), getAnyBiFunction(), getBooleanNullaryFunction());

    final FakeCacheLoaderWriter fakeWriter = new FakeCacheLoaderWriter(Collections.singletonMap("key", "value"));
    this.cacheLoaderWriter = spy(fakeWriter);
    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    ehcache.remove("key", "value");
    verify(this.store).computeIfPresent(eq("key"), getAnyBiFunction(), getBooleanNullaryFunction());
    verify(this.spiedResilienceStrategy)
        .removeFailure(eq("key"), eq("value"), any(CacheAccessException.class), eq(false));
    assertThat(fakeWriter.getEntryMap().containsKey("key"), is(true));    // TODO: Confirm correctness
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ConditionalRemoveOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link org.ehcache.Ehcache#remove(Object, Object)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   *   <li>{@code CacheWriter.delete} throws</li>
   * </ul>
   */
  @Test
  public void testRemoveValueNoStoreEntryCacheWriterException() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.<String, String>emptyMap());
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeWriter = new FakeCacheLoaderWriter(Collections.singletonMap("key", "value"));
    this.cacheLoaderWriter = spy(fakeWriter);
    doThrow(new Exception()).when(this.cacheLoaderWriter).delete("key");
    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    assertFalse(ehcache.remove("key", "value"));
    verify(this.store).computeIfPresent(eq("key"), getAnyBiFunction(), getBooleanNullaryFunction());
    verifyZeroInteractions(this.spiedResilienceStrategy);
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ConditionalRemoveOutcome.FAILURE_KEY_MISSING));
  }

  /**
   * Tests the effect of a {@link org.ehcache.Ehcache#remove(Object, Object)} for
   * <ul>
   *   <li>key with unequal value present in {@code Store}</li>
   *   <li>{@code CacheWriter.delete} throws</li>
   * </ul>
   */
  @Test
  public void testRemoveValueUnequalStoreEntryCacheWriterException() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "unequalValue"));
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeWriter = new FakeCacheLoaderWriter(Collections.singletonMap("key", "unequalValue"));
    this.cacheLoaderWriter = spy(fakeWriter);
    doThrow(new Exception()).when(this.cacheLoaderWriter).delete("key");
    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    assertFalse(ehcache.remove("key", "value"));
    verify(this.store).computeIfPresent(eq("key"), getAnyBiFunction(), getBooleanNullaryFunction());
    verifyZeroInteractions(this.spiedResilienceStrategy);
    assertThat(fakeStore.getEntryMap().get("key"), is(equalTo("unequalValue")));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ConditionalRemoveOutcome.FAILURE_KEY_PRESENT));
  }

  /**
   * Tests the effect of a {@link org.ehcache.Ehcache#remove(Object, Object)} for
   * <ul>
   *   <li>key with equal value present in {@code Store}</li>
   *   <li>{@code CacheWriter.delete} throws</li>
   * </ul>
   */
  @Test
  public void testRemoveValueEqualStoreEntryCacheWriterException() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "value"));
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeWriter = new FakeCacheLoaderWriter(Collections.singletonMap("key", "value"));
    this.cacheLoaderWriter = spy(fakeWriter);
    doThrow(new Exception()).when(this.cacheLoaderWriter).delete("key");
    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    try {
      ehcache.remove("key", "value");
      fail();
    } catch (CacheWriterException e) {
      // Expected
    }
    verify(this.store).computeIfPresent(eq("key"), getAnyBiFunction(), getBooleanNullaryFunction());
    verifyZeroInteractions(this.spiedResilienceStrategy);
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.RemoveOutcome.class));
  }

  /**
   * Tests the effect of a {@link org.ehcache.Ehcache#remove(Object, Object)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   *   <li>>{@code Store.computeIfPresent} throws</li>
   *   <li>{@code CacheWriter.delete} throws</li>
   * </ul>
   */
  @Test
  public void testRemoveValueNoStoreEntryCacheAccessExceptionCacheWriterException() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.<String, String>emptyMap());
    this.store = spy(fakeStore);
    doThrow(new CacheAccessException("")).when(this.store).computeIfPresent(eq("key"), getAnyBiFunction(), getBooleanNullaryFunction());

    final FakeCacheLoaderWriter fakeWriter = new FakeCacheLoaderWriter(Collections.singletonMap("key", "value"));
    this.cacheLoaderWriter = spy(fakeWriter);
    doThrow(new Exception()).when(this.cacheLoaderWriter).delete("key");
    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    ehcache.remove("key", "value");
    verify(this.store).computeIfPresent(eq("key"), getAnyBiFunction(), getBooleanNullaryFunction());
    verify(this.spiedResilienceStrategy)
        .removeFailure(eq("key"), eq("value"), any(CacheAccessException.class), eq(false));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ConditionalRemoveOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link org.ehcache.Ehcache#remove(Object, Object)} for
   * <ul>
   *   <li>key with unequal value present in {@code Store}</li>
   *   <li>>{@code Store.computeIfPresent} throws</li>
   *   <li>{@code CacheWriter.delete} throws</li>
   * </ul>
   */
  @Test
  public void testRemoveValueUnequalStoreEntryCacheAccessExceptionCacheWriterException() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "unequalValue"));
    this.store = spy(fakeStore);
    doThrow(new CacheAccessException("")).when(this.store).computeIfPresent(eq("key"), getAnyBiFunction(), getBooleanNullaryFunction());

    final FakeCacheLoaderWriter fakeWriter = new FakeCacheLoaderWriter(Collections.singletonMap("key", "unequalValue"));
    this.cacheLoaderWriter = spy(fakeWriter);
    doThrow(new Exception()).when(this.cacheLoaderWriter).delete("key");
    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    ehcache.remove("key", "value");
    verify(this.store).computeIfPresent(eq("key"), getAnyBiFunction(), getBooleanNullaryFunction());
    verify(this.spiedResilienceStrategy)
        .removeFailure(eq("key"), eq("value"), any(CacheAccessException.class), eq(false));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ConditionalRemoveOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link org.ehcache.Ehcache#remove(Object, Object)} for
   * <ul>
   *   <li>key with equal value present in {@code Store}</li>
   *   <li>>{@code Store.computeIfPresent} throws</li>
   *   <li>{@code CacheWriter.delete} throws</li>
   * </ul>
   */
  @Test
  public void testRemoveValueEqualStoreEntryCacheAccessExceptionCacheWriterException() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "value"));
    this.store = spy(fakeStore);
    doThrow(new CacheAccessException("")).when(this.store).computeIfPresent(eq("key"), getAnyBiFunction(), getBooleanNullaryFunction());

    final FakeCacheLoaderWriter fakeWriter = new FakeCacheLoaderWriter(Collections.singletonMap("key", "value"));
    this.cacheLoaderWriter = spy(fakeWriter);
    doThrow(new Exception()).when(this.cacheLoaderWriter).delete("key");
    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    ehcache.remove("key", "value");
    verify(this.store).computeIfPresent(eq("key"), getAnyBiFunction(), getBooleanNullaryFunction());
    verify(this.spiedResilienceStrategy)
        .removeFailure(eq("key"), eq("value"), any(CacheAccessException.class), eq(false));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ConditionalRemoveOutcome.FAILURE));
  }

  /**
   * Gets an initialized {@link org.ehcache.Ehcache Ehcache} instance using the
   * {@link org.ehcache.spi.writer.CacheWriter CacheWriter} provided.
   *
   * @param cacheLoaderWriter
   *    the {@code CacheWriter} to use; may be {@code null}
   *
   * @return a new {@code Ehcache} instance
   */
  private Ehcache<String, String> getEhcache(final CacheLoaderWriter<String, String> cacheLoaderWriter) {
    final Ehcache<String, String> ehcache = new Ehcache<String, String>(CACHE_CONFIGURATION, this.store, cacheLoaderWriter);
    ehcache.init();
    assertThat("cache not initialized", ehcache.getStatus(), is(Status.AVAILABLE));
    this.spiedResilienceStrategy = this.setResilienceStrategySpy(ehcache);
    return ehcache;
  }
}
