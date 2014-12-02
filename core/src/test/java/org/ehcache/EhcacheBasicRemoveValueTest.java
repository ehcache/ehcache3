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
import org.ehcache.spi.writer.CacheWriter;
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
  protected CacheWriter<String, String> cacheWriter;

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
    final MockStore realStore = new MockStore(Collections.<String, String>emptyMap());
    this.store = spy(realStore);

    final Ehcache<String, String> ehcache = this.getEhcache(null);

    assertFalse(ehcache.remove("key", "value"));
    verify(this.store).computeIfPresent(eq("key"), getAnyBiFunction());
    verifyZeroInteractions(this.spiedResilienceStrategy);
    assertThat(realStore.getMap().containsKey("key"), is(false));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.RemoveOutcome.FAILURE));   // TODO: Confirm correctness
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
    final MockStore realStore = new MockStore(Collections.singletonMap("key", "unequalValue"));
    this.store = spy(realStore);

    final Ehcache<String, String> ehcache = this.getEhcache(null);

    assertFalse(ehcache.remove("key", "value"));
    verify(this.store).computeIfPresent(eq("key"), getAnyBiFunction());
    verifyZeroInteractions(this.spiedResilienceStrategy);
    assertThat(realStore.getMap().get("key"), is(equalTo("unequalValue")));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.RemoveOutcome.FAILURE));   // TODO: Confirm correctness
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
    final MockStore realStore = new MockStore(Collections.singletonMap("key", "value"));
    this.store = spy(realStore);

    final Ehcache<String, String> ehcache = this.getEhcache(null);

    assertTrue(ehcache.remove("key", "value"));
    verify(this.store).computeIfPresent(eq("key"), getAnyBiFunction());
    verifyZeroInteractions(this.spiedResilienceStrategy);
    assertThat(realStore.getMap().containsKey("key"), is(false));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.RemoveOutcome.SUCCESS));
  }

  /**
   * Tests the effect of a {@link org.ehcache.Ehcache#remove(Object, Object)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   *   <li>>{@code Store.compute} throws</li>
   *   <li>no {@code CacheWriter}</li>
   * </ul>
   */
  @Test
  public void testRemoveValueNoStoreEntryCacheAccessExceptionNoCacheWriter() throws Exception {
    final MockStore realStore = new MockStore(Collections.<String, String>emptyMap());
    this.store = spy(realStore);
    doThrow(new CacheAccessException("")).when(this.store).computeIfPresent(eq("key"), getAnyBiFunction());

    final Ehcache<String, String> ehcache = this.getEhcache(null);

    assertFalse(ehcache.remove("key", "value"));
    verify(this.store).computeIfPresent(eq("key"), getAnyBiFunction());
    verify(this.spiedResilienceStrategy)
        .removeFailure(eq("key"), eq("value"), any(CacheAccessException.class), eq(false));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.RemoveOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link org.ehcache.Ehcache#remove(Object, Object)} for
   * <ul>
   *   <li>key with unequal value present in {@code Store}</li>
   *   <li>>{@code Store.compute} throws</li>
   *   <li>no {@code CacheWriter}</li>
   * </ul>
   */
  @Test
  public void testRemoveValueUnequalStoreEntryCacheAccessExceptionNoCacheWriter() throws Exception {
    final MockStore realStore = new MockStore(Collections.singletonMap("key", "unequalValue"));
    this.store = spy(realStore);
    doThrow(new CacheAccessException("")).when(this.store).computeIfPresent(eq("key"), getAnyBiFunction());

    final Ehcache<String, String> ehcache = this.getEhcache(null);

    assertFalse(ehcache.remove("key", "value"));      // TODO: Confirm correctness
    verify(this.store).computeIfPresent(eq("key"), getAnyBiFunction());
    verify(this.spiedResilienceStrategy)
        .removeFailure(eq("key"), eq("value"), any(CacheAccessException.class), eq(false));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.RemoveOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link org.ehcache.Ehcache#remove(Object, Object)} for
   * <ul>
   *   <li>key with equal value present in {@code Store}</li>
   *   <li>>{@code Store.compute} throws</li>
   *   <li>no {@code CacheWriter}</li>
   * </ul>
   */
  @Test
  public void testRemoveValueEqualStoreEntryCacheAccessExceptionNoCacheWriter() throws Exception {
    final MockStore realStore = new MockStore(Collections.singletonMap("key", "value"));
    this.store = spy(realStore);
    doThrow(new CacheAccessException("")).when(this.store).computeIfPresent(eq("key"), getAnyBiFunction());

    final Ehcache<String, String> ehcache = this.getEhcache(null);

    assertFalse(ehcache.remove("key", "value"));      // TODO: Confirm correctness
    verify(this.store).computeIfPresent(eq("key"), getAnyBiFunction());
    verify(this.spiedResilienceStrategy)
        .removeFailure(eq("key"), eq("value"), any(CacheAccessException.class), eq(false));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.RemoveOutcome.FAILURE));
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
    final MockStore realStore = new MockStore(Collections.<String, String>emptyMap());
    this.store = spy(realStore);

    final MockCacheWriter realCache = new MockCacheWriter(Collections.<String, String>emptyMap());
    final Ehcache<String, String> ehcache = this.getEhcache(realCache);

    assertFalse(ehcache.remove("key", "value"));
    verify(this.store).computeIfPresent(eq("key"), getAnyBiFunction());
    verifyZeroInteractions(this.spiedResilienceStrategy);
    assertThat(realStore.getMap().containsKey("key"), is(false));
    assertThat(realCache.getEntries().containsKey("key"), is(false));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.RemoveOutcome.FAILURE));   // TODO: Confirm correctness
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
    final MockStore realStore = new MockStore(Collections.singletonMap("key", "unequalValue"));
    this.store = spy(realStore);

    final MockCacheWriter realCache = new MockCacheWriter(Collections.<String, String>emptyMap());
    final Ehcache<String, String> ehcache = this.getEhcache(realCache);

    assertFalse(ehcache.remove("key", "value"));
    verify(this.store).computeIfPresent(eq("key"), getAnyBiFunction());
    verifyZeroInteractions(this.spiedResilienceStrategy);
    assertThat(realStore.getMap().get("key"), is(equalTo("unequalValue")));
    assertThat(realCache.getEntries().containsKey("key"), is(false));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.RemoveOutcome.FAILURE));   // TODO: Confirm correctness
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
    final MockStore realStore = new MockStore(Collections.singletonMap("key", "value"));
    this.store = spy(realStore);

    final MockCacheWriter realCache = new MockCacheWriter(Collections.<String, String>emptyMap());
    final Ehcache<String, String> ehcache = this.getEhcache(realCache);

    assertTrue(ehcache.remove("key", "value"));
    verify(this.store).computeIfPresent(eq("key"), getAnyBiFunction());
    verifyZeroInteractions(this.spiedResilienceStrategy);
    assertThat(realStore.getMap().containsKey("key"), is(false));
    assertThat(realCache.getEntries().containsKey("key"), is(false));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.RemoveOutcome.SUCCESS));
  }

  /**
   * Tests the effect of a {@link org.ehcache.Ehcache#remove(Object, Object)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   *   <li>>{@code Store.compute} throws</li>
   *   <li>key not present via {@code CacheWriter}</li>
   * </ul>
   */
  @Test
  public void testRemoveValueNoStoreEntryCacheAccessExceptionNoCacheWriterEntry() throws Exception {
    final MockStore realStore = new MockStore(Collections.<String, String>emptyMap());
    this.store = spy(realStore);
    doThrow(new CacheAccessException("")).when(this.store).computeIfPresent(eq("key"), getAnyBiFunction());

    final MockCacheWriter realCache = new MockCacheWriter(Collections.<String, String>emptyMap());
    this.cacheWriter = spy(realCache);
    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheWriter);

    assertFalse(ehcache.remove("key", "value"));
    verify(this.store).computeIfPresent(eq("key"), getAnyBiFunction());
    verify(this.spiedResilienceStrategy)
        .removeFailure(eq("key"), eq("value"), any(CacheAccessException.class), eq(false));
    assertThat(realCache.getEntries().containsKey("key"), is(false));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.RemoveOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link org.ehcache.Ehcache#remove(Object, Object)} for
   * <ul>
   *   <li>key with unequal value present in {@code Store}</li>
   *   <li>>{@code Store.compute} throws</li>
   *   <li>key not present via {@code CacheWriter}</li>
   * </ul>
   */
  @Test
  public void testRemoveValueUnequalStoreEntryCacheAccessExceptionNoCacheWriterEntry() throws Exception {
    final MockStore realStore = new MockStore(Collections.singletonMap("key", "unequalValue"));
    this.store = spy(realStore);
    doThrow(new CacheAccessException("")).when(this.store).computeIfPresent(eq("key"), getAnyBiFunction());

    final MockCacheWriter realCache = new MockCacheWriter(Collections.<String, String>emptyMap());
    this.cacheWriter = spy(realCache);
    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheWriter);

    assertFalse(ehcache.remove("key", "value"));    // TODO: Confirm correctness
    verify(this.store).computeIfPresent(eq("key"), getAnyBiFunction());
    verify(this.spiedResilienceStrategy)
        .removeFailure(eq("key"), eq("value"), any(CacheAccessException.class), eq(false));
    assertThat(realCache.getEntries().containsKey("key"), is(false));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.RemoveOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link org.ehcache.Ehcache#remove(Object, Object)} for
   * <ul>
   *   <li>key with equal value present in {@code Store}</li>
   *   <li>>{@code Store.compute} throws</li>
   *   <li>key not present via {@code CacheWriter}</li>
   * </ul>
   */
  @Test
  public void testRemoveValueEqualStoreEntryCacheAccessExceptionNoCacheWriterEntry() throws Exception {
    final MockStore realStore = new MockStore(Collections.singletonMap("key", "value"));
    this.store = spy(realStore);
    doThrow(new CacheAccessException("")).when(this.store).computeIfPresent(eq("key"), getAnyBiFunction());

    final MockCacheWriter realCache = new MockCacheWriter(Collections.<String, String>emptyMap());
    this.cacheWriter = spy(realCache);
    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheWriter);

    assertFalse(ehcache.remove("key", "value"));    // TODO: Confirm correctness
    verify(this.store).computeIfPresent(eq("key"), getAnyBiFunction());
    verify(this.spiedResilienceStrategy)
        .removeFailure(eq("key"), eq("value"), any(CacheAccessException.class), eq(false));
    assertThat(realCache.getEntries().containsKey("key"), is(false));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.RemoveOutcome.FAILURE));
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
    final MockStore realStore = new MockStore(Collections.<String, String>emptyMap());
    this.store = spy(realStore);

    final MockCacheWriter realCache = new MockCacheWriter(Collections.singletonMap("key", "unequalValue"));
    final Ehcache<String, String> ehcache = this.getEhcache(realCache);

    assertFalse(ehcache.remove("key", "value"));
    verify(this.store).computeIfPresent(eq("key"), getAnyBiFunction());
    verifyZeroInteractions(this.spiedResilienceStrategy);
    assertThat(realStore.getMap().containsKey("key"), is(false));
    assertThat(realCache.getEntries().get("key"), is(equalTo("unequalValue")));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.RemoveOutcome.FAILURE));   // TODO: Confirm correctness
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
    final MockStore realStore = new MockStore(Collections.singletonMap("key", "unequalValue"));
    this.store = spy(realStore);

    final MockCacheWriter realCache = new MockCacheWriter(Collections.singletonMap("key", "unequalValue"));
    final Ehcache<String, String> ehcache = this.getEhcache(realCache);

    assertFalse(ehcache.remove("key", "value"));
    verify(this.store).computeIfPresent(eq("key"), getAnyBiFunction());
    verifyZeroInteractions(this.spiedResilienceStrategy);
    assertThat(realStore.getMap().get("key"), is(equalTo("unequalValue")));
    assertThat(realCache.getEntries().get("key"), is(equalTo("unequalValue")));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.RemoveOutcome.FAILURE));   // TODO: Confirm correctness
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
    final MockStore realStore = new MockStore(Collections.singletonMap("key", "value"));
    this.store = spy(realStore);

    final MockCacheWriter realCache = new MockCacheWriter(Collections.singletonMap("key", "unequalValue"));
    final Ehcache<String, String> ehcache = this.getEhcache(realCache);

    assertTrue(ehcache.remove("key", "value"));
    verify(this.store).computeIfPresent(eq("key"), getAnyBiFunction());
    verifyZeroInteractions(this.spiedResilienceStrategy);
    assertThat(realStore.getMap().containsKey("key"), is(false));
    assertThat(realCache.getEntries().containsKey("key"), is(false));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.RemoveOutcome.SUCCESS));
  }

  /**
   * Tests the effect of a {@link org.ehcache.Ehcache#remove(Object, Object)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   *   <li>>{@code Store.compute} throws</li>
   *   <li>key with unequal value present via {@code CacheWriter}</li>
   * </ul>
   */
  @Test
  public void testRemoveValueNoStoreEntryCacheAccessExceptionUnequalCacheWriterEntry() throws Exception {
    final MockStore realStore = new MockStore(Collections.<String, String>emptyMap());
    this.store = spy(realStore);
    doThrow(new CacheAccessException("")).when(this.store).computeIfPresent(eq("key"), getAnyBiFunction());

    final MockCacheWriter realCache = new MockCacheWriter(Collections.singletonMap("key", "unequalValue"));
    this.cacheWriter = spy(realCache);
    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheWriter);

    assertFalse(ehcache.remove("key", "value"));
    verify(this.store).computeIfPresent(eq("key"), getAnyBiFunction());
    verify(this.spiedResilienceStrategy)
        .removeFailure(eq("key"), eq("value"), any(CacheAccessException.class), eq(false));
    assertThat(realCache.getEntries().get("key"), is(equalTo("unequalValue")));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.RemoveOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link org.ehcache.Ehcache#remove(Object, Object)} for
   * <ul>
   *   <li>key with unequal value present in {@code Store}</li>
   *   <li>>{@code Store.compute} throws</li>
   *   <li>key with unequal value present via {@code CacheWriter}</li>
   * </ul>
   */
  @Test
  public void testRemoveValueUnequalStoreEntryCacheAccessExceptionUnequalCacheWriterEntry() throws Exception {
    final MockStore realStore = new MockStore(Collections.singletonMap("key", "unequalValue"));
    this.store = spy(realStore);
    doThrow(new CacheAccessException("")).when(this.store).computeIfPresent(eq("key"), getAnyBiFunction());

    final MockCacheWriter realCache = new MockCacheWriter(Collections.singletonMap("key", "unequalValue"));
    this.cacheWriter = spy(realCache);
    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheWriter);

    assertFalse(ehcache.remove("key", "value"));    // TODO: Confirm correctness
    verify(this.store).computeIfPresent(eq("key"), getAnyBiFunction());
    verify(this.spiedResilienceStrategy)
        .removeFailure(eq("key"), eq("value"), any(CacheAccessException.class), eq(false));
    assertThat(realCache.getEntries().get("key"), is(equalTo("unequalValue")));   // TODO: Confirm correctness
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.RemoveOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link org.ehcache.Ehcache#remove(Object, Object)} for
   * <ul>
   *   <li>key with equal value present in {@code Store}</li>
   *   <li>>{@code Store.compute} throws</li>
   *   <li>key with unequal value present via {@code CacheWriter}</li>
   * </ul>
   */
  @Test
  public void testRemoveValueEqualStoreEntryCacheAccessExceptionUnequalCacheWriterEntry() throws Exception {
    final MockStore realStore = new MockStore(Collections.singletonMap("key", "value"));
    this.store = spy(realStore);
    doThrow(new CacheAccessException("")).when(this.store).computeIfPresent(eq("key"), getAnyBiFunction());

    final MockCacheWriter realCache = new MockCacheWriter(Collections.singletonMap("key", "unequalValue"));
    this.cacheWriter = spy(realCache);
    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheWriter);

    assertFalse(ehcache.remove("key", "value"));    // TODO: Confirm correctness
    verify(this.store).computeIfPresent(eq("key"), getAnyBiFunction());
    verify(this.spiedResilienceStrategy)
        .removeFailure(eq("key"), eq("value"), any(CacheAccessException.class), eq(false));
    assertThat(realCache.getEntries().get("key"), is(equalTo("unequalValue")));   // TODO: Confirm correctness
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.RemoveOutcome.FAILURE));
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
    final MockStore realStore = new MockStore(Collections.<String, String>emptyMap());
    this.store = spy(realStore);

    final MockCacheWriter realCache = new MockCacheWriter(Collections.singletonMap("key", "value"));
    final Ehcache<String, String> ehcache = this.getEhcache(realCache);

    assertFalse(ehcache.remove("key", "value"));
    verify(this.store).computeIfPresent(eq("key"), getAnyBiFunction());
    verifyZeroInteractions(this.spiedResilienceStrategy);
    assertThat(realStore.getMap().containsKey("key"), is(false));
    assertThat(realCache.getEntries().containsKey("key"), is(true));    // TODO: Confirm correctness
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.RemoveOutcome.FAILURE));
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
    final MockStore realStore = new MockStore(Collections.singletonMap("key", "unequalValue"));
    this.store = spy(realStore);

    final MockCacheWriter realCache = new MockCacheWriter(Collections.singletonMap("key", "value"));
    final Ehcache<String, String> ehcache = this.getEhcache(realCache);

    assertFalse(ehcache.remove("key", "value"));
    verify(this.store).computeIfPresent(eq("key"), getAnyBiFunction());
    verifyZeroInteractions(this.spiedResilienceStrategy);
    assertThat(realStore.getMap().get("key"), is(equalTo("unequalValue")));
    assertThat(realCache.getEntries().get("key"), is(equalTo("value")));    // TODO: Confirm correctness
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.RemoveOutcome.FAILURE));     // TODO: Confirm correctness
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
    final MockStore realStore = new MockStore(Collections.singletonMap("key", "value"));
    this.store = spy(realStore);

    final MockCacheWriter realCache = new MockCacheWriter(Collections.singletonMap("key", "value"));
    final Ehcache<String, String> ehcache = this.getEhcache(realCache);

    assertTrue(ehcache.remove("key", "value"));
    verify(this.store).computeIfPresent(eq("key"), getAnyBiFunction());
    verifyZeroInteractions(this.spiedResilienceStrategy);
    assertThat(realStore.getMap().containsKey("key"), is(false));
    assertThat(realCache.getEntries().containsKey("key"), is(false));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.RemoveOutcome.SUCCESS));
  }

  /**
   * Tests the effect of a {@link org.ehcache.Ehcache#remove(Object, Object)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   *   <li>>{@code Store.compute} throws</li>
   *   <li>key with equal value present via {@code CacheWriter}</li>
   * </ul>
   */
  @Test
  public void testRemoveValueNoStoreEntryCacheAccessExceptionEqualCacheWriterEntry() throws Exception {
    final MockStore realStore = new MockStore(Collections.<String, String>emptyMap());
    this.store = spy(realStore);
    doThrow(new CacheAccessException("")).when(this.store).computeIfPresent(eq("key"), getAnyBiFunction());

    final MockCacheWriter realCache = new MockCacheWriter(Collections.singletonMap("key", "value"));
    this.cacheWriter = spy(realCache);
    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheWriter);

    assertFalse(ehcache.remove("key", "value"));
    verify(this.store).computeIfPresent(eq("key"), getAnyBiFunction());
    verify(this.spiedResilienceStrategy)
        .removeFailure(eq("key"), eq("value"), any(CacheAccessException.class), eq(false));
    assertThat(realCache.getEntries().containsKey("key"), is(true));    // TODO: Confirm correctness
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.RemoveOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link org.ehcache.Ehcache#remove(Object, Object)} for
   * <ul>
   *   <li>key with unequal value present in {@code Store}</li>
   *   <li>>{@code Store.compute} throws</li>
   *   <li>key with equal value present via {@code CacheWriter}</li>
   * </ul>
   */
  @Test
  public void testRemoveValueUnequalStoreEntryCacheAccessExceptionEqualCacheWriterEntry() throws Exception {
    final MockStore realStore = new MockStore(Collections.singletonMap("key", "unequalValue"));
    this.store = spy(realStore);
    doThrow(new CacheAccessException("")).when(this.store).computeIfPresent(eq("key"), getAnyBiFunction());

    final MockCacheWriter realCache = new MockCacheWriter(Collections.singletonMap("key", "value"));
    this.cacheWriter = spy(realCache);
    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheWriter);

    assertFalse(ehcache.remove("key", "value"));
    verify(this.store).computeIfPresent(eq("key"), getAnyBiFunction());
    verify(this.spiedResilienceStrategy)
        .removeFailure(eq("key"), eq("value"), any(CacheAccessException.class), eq(false));
    assertThat(realCache.getEntries().containsKey("key"), is(true));    // TODO: Confirm correctness
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.RemoveOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link org.ehcache.Ehcache#remove(Object, Object)} for
   * <ul>
   *   <li>key with equal value present in {@code Store}</li>
   *   <li>>{@code Store.compute} throws</li>
   *   <li>key with equal value present via {@code CacheWriter}</li>
   * </ul>
   */
  @Test
  public void testRemoveValueEqualStoreEntryCacheAccessExceptionEqualCacheWriterEntry() throws Exception {
    final MockStore realStore = new MockStore(Collections.singletonMap("key", "value"));
    this.store = spy(realStore);
    doThrow(new CacheAccessException("")).when(this.store).computeIfPresent(eq("key"), getAnyBiFunction());

    final MockCacheWriter realCache = new MockCacheWriter(Collections.singletonMap("key", "value"));
    this.cacheWriter = spy(realCache);
    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheWriter);

    assertFalse(ehcache.remove("key", "value"));
    verify(this.store).computeIfPresent(eq("key"), getAnyBiFunction());
    verify(this.spiedResilienceStrategy)
        .removeFailure(eq("key"), eq("value"), any(CacheAccessException.class), eq(false));
    assertThat(realCache.getEntries().containsKey("key"), is(true));    // TODO: Confirm correctness
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.RemoveOutcome.FAILURE));
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
    final MockStore realStore = new MockStore(Collections.<String, String>emptyMap());
    this.store = spy(realStore);

    final MockCacheWriter realCache = new MockCacheWriter(Collections.singletonMap("key", "value"));
    this.cacheWriter = spy(realCache);
    doThrow(new Exception()).when(this.cacheWriter).delete("key");
    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheWriter);

    assertFalse(ehcache.remove("key", "value"));
    verify(this.store).computeIfPresent(eq("key"), getAnyBiFunction());
    verifyZeroInteractions(this.spiedResilienceStrategy);
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.RemoveOutcome.FAILURE));
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
    final MockStore realStore = new MockStore(Collections.singletonMap("key", "unequalValue"));
    this.store = spy(realStore);

    final MockCacheWriter realCache = new MockCacheWriter(Collections.singletonMap("key", "value"));
    this.cacheWriter = spy(realCache);
    doThrow(new Exception()).when(this.cacheWriter).delete("key");
    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheWriter);

    assertFalse(ehcache.remove("key", "value"));
    verify(this.store).computeIfPresent(eq("key"), getAnyBiFunction());
    verifyZeroInteractions(this.spiedResilienceStrategy);
    assertThat(realStore.getMap().get("key"), is(equalTo("unequalValue")));
    // With the current Ehcache.remove(key,value) implementation, CacheWriter.delete(key, value)
    // is not called when the current Store value is not equal to the value specified on
    // Ehcache.remove -- the Store-of-Record (accessed via CacheWriter) is left untouched.
    assertThat(realCache.getEntries().get("key"), is(equalTo("value")));    // TODO: Confirm correctness
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.RemoveOutcome.FAILURE));
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
    final MockStore realStore = new MockStore(Collections.singletonMap("key", "value"));
    this.store = spy(realStore);

    final MockCacheWriter realCache = new MockCacheWriter(Collections.singletonMap("key", "value"));
    this.cacheWriter = spy(realCache);
    doThrow(new Exception()).when(this.cacheWriter).delete("key");
    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheWriter);

    try {
      ehcache.remove("key", "value");
      fail();
    } catch (CacheWriterException e) {
      // Expected
    }
    verify(this.store).computeIfPresent(eq("key"), getAnyBiFunction());
    verifyZeroInteractions(this.spiedResilienceStrategy);
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.RemoveOutcome.class));
  }

  /**
   * Tests the effect of a {@link org.ehcache.Ehcache#remove(Object, Object)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   *   <li>>{@code Store.compute} throws</li>
   *   <li>{@code CacheWriter.delete} throws</li>
   * </ul>
   */
  @Test
  public void testRemoveValueNoStoreEntryCacheAccessExceptionCacheWriterException() throws Exception {
    final MockStore realStore = new MockStore(Collections.<String, String>emptyMap());
    this.store = spy(realStore);
    doThrow(new CacheAccessException("")).when(this.store).computeIfPresent(eq("key"), getAnyBiFunction());

    final MockCacheWriter realCache = new MockCacheWriter(Collections.singletonMap("key", "value"));
    this.cacheWriter = spy(realCache);
    doThrow(new Exception()).when(this.cacheWriter).delete("key");
    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheWriter);

    assertFalse(ehcache.remove("key", "value"));
    verify(this.store).computeIfPresent(eq("key"), getAnyBiFunction());
    verify(this.spiedResilienceStrategy)
        .removeFailure(eq("key"), eq("value"), any(CacheAccessException.class), eq(false));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.RemoveOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link org.ehcache.Ehcache#remove(Object, Object)} for
   * <ul>
   *   <li>key with unequal value present in {@code Store}</li>
   *   <li>>{@code Store.compute} throws</li>
   *   <li>{@code CacheWriter.delete} throws</li>
   * </ul>
   */
  @Test
  public void testRemoveValueUnequalStoreEntryCacheAccessExceptionCacheWriterException() throws Exception {
    final MockStore realStore = new MockStore(Collections.singletonMap("key", "unequalValue"));
    this.store = spy(realStore);
    doThrow(new CacheAccessException("")).when(this.store).computeIfPresent(eq("key"), getAnyBiFunction());

    final MockCacheWriter realCache = new MockCacheWriter(Collections.singletonMap("key", "value"));
    this.cacheWriter = spy(realCache);
    doThrow(new Exception()).when(this.cacheWriter).delete("key");
    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheWriter);

    assertFalse(ehcache.remove("key", "value"));
    verify(this.store).computeIfPresent(eq("key"), getAnyBiFunction());
    verify(this.spiedResilienceStrategy)
        .removeFailure(eq("key"), eq("value"), any(CacheAccessException.class), eq(false));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.RemoveOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link org.ehcache.Ehcache#remove(Object, Object)} for
   * <ul>
   *   <li>key with equal value present in {@code Store}</li>
   *   <li>>{@code Store.compute} throws</li>
   *   <li>{@code CacheWriter.delete} throws</li>
   * </ul>
   */
  @Test
  public void testRemoveValueEqualStoreEntryCacheAccessExceptionCacheWriterException() throws Exception {
    final MockStore realStore = new MockStore(Collections.singletonMap("key", "value"));
    this.store = spy(realStore);
    doThrow(new CacheAccessException("")).when(this.store).computeIfPresent(eq("key"), getAnyBiFunction());

    final MockCacheWriter realCache = new MockCacheWriter(Collections.singletonMap("key", "value"));
    this.cacheWriter = spy(realCache);
    doThrow(new Exception()).when(this.cacheWriter).delete("key");
    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheWriter);

    assertFalse(ehcache.remove("key", "value"));
    verify(this.store).computeIfPresent(eq("key"), getAnyBiFunction());
    verify(this.spiedResilienceStrategy)
        .removeFailure(eq("key"), eq("value"), any(CacheAccessException.class), eq(false));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.RemoveOutcome.FAILURE));
  }

  /**
   * Gets an initialized {@link org.ehcache.Ehcache Ehcache} instance using the
   * {@link org.ehcache.spi.writer.CacheWriter CacheWriter} provided.
   *
   * @param cacheWriter
   *    the {@code CacheWriter} to use; may be {@code null}
   *
   * @return a new {@code Ehcache} instance
   */
  private Ehcache<String, String> getEhcache(final CacheWriter<String, String> cacheWriter) {
    final Ehcache<String, String> ehcache = new Ehcache<String, String>(CACHE_CONFIGURATION, this.store, null, cacheWriter);
    ehcache.init();
    assertThat("cache not initialized", ehcache.getStatus(), is(Status.AVAILABLE));
    this.spiedResilienceStrategy = this.setResilienceStrategySpy(ehcache);
    return ehcache;
  }
}
