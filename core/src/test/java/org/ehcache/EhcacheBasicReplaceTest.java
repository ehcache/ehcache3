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
import org.mockito.InOrder;
import org.mockito.Mock;

import java.util.Collections;
import java.util.EnumSet;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

/**
 * Provides testing of basic REPLACE(key, value) operations on an {@code Ehcache}.
 *
 * @author Clifford W. Johnson
 */
public class EhcacheBasicReplaceTest extends EhcacheBasicCrudBase {

  @Mock
  protected CacheWriter<String, String> cacheWriter;

  @Test
  public void testReplaceNullNull() {
    final Ehcache<String, String> ehcache = this.getEhcache(null);

    try {
      ehcache.replace(null, null);
      fail();
    } catch (NullPointerException e) {
      // expected
    }
  }

  @Test
  public void testReplaceKeyNull() {
    final Ehcache<String, String> ehcache = this.getEhcache(null);

    try {
      ehcache.replace("key", null);
      fail();
    } catch (NullPointerException e) {
      // expected
    }
  }

  @Test
  public void testReplaceNullValue() {
    final Ehcache<String, String> ehcache = this.getEhcache(null);

    try {
      ehcache.replace(null, "value");
      fail();
    } catch (NullPointerException e) {
      // expected
    }
  }

  /**
   * Tests the effect of a {@link Ehcache#replace(Object, Object)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   *   <li>no {@code CacheWriter}</li>
   * </ul>
   */
  @Test
  public void testReplaceNoStoreEntryNoCacheWriter() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.<String, String>emptyMap());
    this.store = spy(fakeStore);

    final Ehcache<String, String> ehcache = this.getEhcache(null);

    assertNull(ehcache.replace("key", "value"));
    verify(this.store).computeIfPresent(eq("key"), getAnyBiFunction());
    verifyZeroInteractions(this.spiedResilienceStrategy);
    assertThat(fakeStore.getEntryMap().containsKey("key"), is(false));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ReplaceOutcome.MISS_NOT_PRESENT));
  }

  /**
   * Tests the effect of a {@link Ehcache#replace(Object, Object)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   *   <li>key not present via {@code CacheWriter}</li>
   * </ul>
   */
  @Test
  public void testReplaceNoStoreEntryNoCacheWriterEntry() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.<String, String>emptyMap());
    this.store = spy(fakeStore);

    final FakeCacheWriter fakeWriter = new FakeCacheWriter(Collections.<String, String>emptyMap());
    final Ehcache<String, String> ehcache = this.getEhcache(fakeWriter);

    assertNull(ehcache.replace("key", "value"));
    verify(this.store).computeIfPresent(eq("key"), getAnyBiFunction());
    verifyZeroInteractions(this.spiedResilienceStrategy);
    assertThat(fakeStore.getEntryMap().containsKey("key"), is(false));
    assertThat(fakeWriter.getEntryMap().containsKey("key"), is(false));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ReplaceOutcome.MISS_NOT_PRESENT));
  }

  /**
   * Tests the effect of a {@link Ehcache#replace(Object, Object)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   *   <li>key present via {@code CacheWriter}</li>
   * </ul>
   */
  @Test
  public void testReplaceNoStoreEntryHasCacheWriterEntry() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.<String, String>emptyMap());
    this.store = spy(fakeStore);

    final FakeCacheWriter fakeWriter = new FakeCacheWriter(Collections.singletonMap("key", "oldValue"));
    final Ehcache<String, String> ehcache = this.getEhcache(fakeWriter);

    assertNull(ehcache.replace("key", "value"));
    verify(this.store).computeIfPresent(eq("key"), getAnyBiFunction());
    verifyZeroInteractions(this.spiedResilienceStrategy);
    assertThat(fakeStore.getEntryMap().containsKey("key"), is(false));
    assertThat(fakeWriter.getEntryMap().get("key"), is(equalTo("oldValue")));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ReplaceOutcome.MISS_NOT_PRESENT));
  }

  /**
   * Tests the effect of a {@link Ehcache#replace(Object, Object)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   *   <li>{@code CacheLoader.write} throws</li>
   * </ul>
   */
  @Test
  public void testReplaceNoStoreEntryCacheWriterException() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.<String, String>emptyMap());
    this.store = spy(fakeStore);

    final FakeCacheWriter fakeWriter = new FakeCacheWriter(Collections.singletonMap("key", "oldValue"));
    this.cacheWriter = spy(fakeWriter);
    doThrow(new Exception()).when(this.cacheWriter).write("key", "value");
    final Ehcache<String, String> ehcache = this.getEhcache(fakeWriter);

    assertNull(ehcache.replace("key", "value"));
    verify(this.store).computeIfPresent(eq("key"), getAnyBiFunction());
    verifyZeroInteractions(this.spiedResilienceStrategy);
    assertThat(fakeStore.getEntryMap().containsKey("key"), is(false));
    assertThat(fakeWriter.getEntryMap().get("key"), is(equalTo("oldValue")));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ReplaceOutcome.MISS_NOT_PRESENT));
  }

  /**
   * Tests the effect of a {@link Ehcache#replace(Object, Object)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   *   <li>{@code Store.computeIfPresent} throws</li>
   *   <li>no {@code CacheWriter}</li>
   * </ul>
   */
  @Test
  public void testReplaceNoStoreEntryCacheAccessExceptionNoCacheWriter() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.<String, String>emptyMap());
    this.store = spy(fakeStore);
    doThrow(new CacheAccessException("")).when(this.store).computeIfPresent(eq("key"), getAnyBiFunction());

    final Ehcache<String, String> ehcache = this.getEhcache(null);

    ehcache.replace("key", "value");
    verify(this.store).computeIfPresent(eq("key"), getAnyBiFunction());
    verify(this.spiedResilienceStrategy).replaceFailure(eq("key"), eq("value"), any(CacheAccessException.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ReplaceOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link Ehcache#replace(Object, Object)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   *   <li>{@code Store.computeIfPresent} throws</li>
   *   <li>key not present via {@code CacheWriter}</li>
   * </ul>
   */
  @Test
  public void testReplaceNoStoreEntryCacheAccessExceptionNoCacheWriterEntry() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.<String, String>emptyMap());
    this.store = spy(fakeStore);
    doThrow(new CacheAccessException("")).when(this.store).computeIfPresent(eq("key"), getAnyBiFunction());

    final FakeCacheWriter fakeWriter = new FakeCacheWriter(Collections.<String, String>emptyMap());
    this.cacheWriter = spy(fakeWriter);
    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheWriter);

    final InOrder ordered = inOrder(this.cacheWriter, this.spiedResilienceStrategy);

    ehcache.replace("key", "value");
    verify(this.store).computeIfPresent(eq("key"), getAnyBiFunction());
    ordered.verify(this.cacheWriter).write(eq("key"), any(String.class));
    ordered.verify(this.spiedResilienceStrategy).replaceFailure(eq("key"), eq("value"), any(CacheAccessException.class));
    assertThat(fakeWriter.getEntryMap().get("key"), is(equalTo("value")));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ReplaceOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link Ehcache#replace(Object, Object)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   *   <li>{@code Store.computeIfPresent} throws</li>
   *   <li>key present via {@code CacheWriter}</li>
   * </ul>
   */
  @Test
  public void testReplaceNoStoreEntryCacheAccessExceptionHasCacheWriterEntry() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.<String, String>emptyMap());
    this.store = spy(fakeStore);
    doThrow(new CacheAccessException("")).when(this.store).computeIfPresent(eq("key"), getAnyBiFunction());

    final FakeCacheWriter fakeWriter = new FakeCacheWriter(Collections.singletonMap("key", "oldValue"));
    this.cacheWriter = spy(fakeWriter);
    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheWriter);

    final InOrder ordered = inOrder(this.cacheWriter, this.spiedResilienceStrategy);

    ehcache.replace("key", "value");
    verify(this.store).computeIfPresent(eq("key"), getAnyBiFunction());
    ordered.verify(this.cacheWriter).write(eq("key"), any(String.class));
    ordered.verify(this.spiedResilienceStrategy).replaceFailure(eq("key"), eq("value"), any(CacheAccessException.class));
    assertThat(fakeWriter.getEntryMap().get("key"), is(equalTo("value")));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ReplaceOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link Ehcache#replace(Object, Object)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   *   <li>{@code Store.computeIfPresent} throws</li>
   *   <li>{@code CacheLoader.write} throws</li>
   * </ul>
   */
  @Test
  public void testReplaceNoStoreEntryCacheAccessExceptionCacheWriterException() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.<String, String>emptyMap());
    this.store = spy(fakeStore);
    doThrow(new CacheAccessException("")).when(this.store).computeIfPresent(eq("key"), getAnyBiFunction());

    final FakeCacheWriter fakeWriter = new FakeCacheWriter(Collections.singletonMap("key", "oldValue"));
    this.cacheWriter = spy(fakeWriter);
    doThrow(new Exception()).when(this.cacheWriter).write("key", "value");
    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheWriter);

    final InOrder ordered = inOrder(this.cacheWriter, this.spiedResilienceStrategy);

    try {
      ehcache.replace("key", "value");
      fail();
    } catch (CacheWriterException e) {
      // Expected
    }
    verify(this.store).computeIfPresent(eq("key"), getAnyBiFunction());
    ordered.verify(this.cacheWriter).write(eq("key"), any(String.class));
    ordered.verify(this.spiedResilienceStrategy)
        .replaceFailure(eq("key"), eq("value"), any(CacheAccessException.class), any(CacheWriterException.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ReplaceOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link Ehcache#replace(Object, Object)} for
   * <ul>
   *   <li>key present in {@code Store}</li>
   *   <li>no {@code CacheWriter}</li>
   * </ul>
   */
  @Test
  public void testReplaceHasStoreEntryNoCacheWriter() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "oldValue"));
    this.store = spy(fakeStore);

    final Ehcache<String, String> ehcache = this.getEhcache(null);

    assertThat(ehcache.replace("key", "value"), is(equalTo("oldValue")));
    verify(this.store).computeIfPresent(eq("key"), getAnyBiFunction());
    verifyZeroInteractions(this.spiedResilienceStrategy);
    assertThat(fakeStore.getEntryMap().get("key"), is(equalTo("value")));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ReplaceOutcome.HIT));
  }

  /**
   * Tests the effect of a {@link Ehcache#replace(Object, Object)} for
   * <ul>
   *   <li>key present in {@code Store}</li>
   *   <li>key not present via {@code CacheWriter}</li>
   * </ul>
   */
  @Test
  public void testReplaceHasStoreEntryNoCacheWriterEntry() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "oldValue"));
    this.store = spy(fakeStore);

    final FakeCacheWriter fakeWriter = new FakeCacheWriter(Collections.<String, String>emptyMap());
    final Ehcache<String, String> ehcache = this.getEhcache(fakeWriter);

    assertThat(ehcache.replace("key", "value"), is(equalTo("oldValue")));
    verify(this.store).computeIfPresent(eq("key"), getAnyBiFunction());
    verifyZeroInteractions(this.spiedResilienceStrategy);
    assertThat(fakeStore.getEntryMap().get("key"), is(equalTo("value")));
    assertThat(fakeWriter.getEntryMap().get("key"), equalTo("value"));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ReplaceOutcome.HIT));
  }

  /**
   * Tests the effect of a {@link Ehcache#replace(Object, Object)} for
   * <ul>
   *   <li>key present in {@code Store}</li>
   *   <li>key present via {@code CacheWriter}</li>
   * </ul>
   */
  @Test
  public void testReplaceHasStoreEntryHasCacheWriterEntry() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "oldValue"));
    this.store = spy(fakeStore);

    final FakeCacheWriter fakeWriter = new FakeCacheWriter(Collections.singletonMap("key", "oldValue"));
    final Ehcache<String, String> ehcache = this.getEhcache(fakeWriter);

    assertThat(ehcache.replace("key", "value"), is(equalTo("oldValue")));
    verify(this.store).computeIfPresent(eq("key"), getAnyBiFunction());
    verifyZeroInteractions(this.spiedResilienceStrategy);
    assertThat(fakeStore.getEntryMap().get("key"), is(equalTo("value")));
    assertThat(fakeWriter.getEntryMap().get("key"), equalTo("value"));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ReplaceOutcome.HIT));
  }

  /**
   * Tests the effect of a {@link Ehcache#replace(Object, Object)} for
   * <ul>
   *   <li>key present in {@code Store}</li>
   *   <li>{@code CacheLoader.write} throws</li>
   * </ul>
   */
  @Test
  public void testReplaceHasStoreEntryCacheWriterException() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "oldValue"));
    this.store = spy(fakeStore);

    final FakeCacheWriter fakeWriter = new FakeCacheWriter(Collections.singletonMap("key", "oldValue"));
    this.cacheWriter = spy(fakeWriter);
    doThrow(new Exception()).when(this.cacheWriter).write("key", "value");
    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheWriter);

    try {
      ehcache.replace("key", "value");
      fail();
    } catch (CacheWriterException e) {
      // Expected
    }
    verify(this.store).computeIfPresent(eq("key"), getAnyBiFunction());
    verifyZeroInteractions(this.spiedResilienceStrategy);
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.ReplaceOutcome.class));
  }

  /**
   * Tests the effect of a {@link Ehcache#replace(Object, Object)} for
   * <ul>
   *   <li>key present in {@code Store}</li>
   *   <li>{@code Store.computeIfPresent} throws</li>
   *   <li>no {@code CacheWriter}</li>
   * </ul>
   */
  @Test
  public void testReplaceHasStoreEntryCacheAccessExceptionNoCacheWriter() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "oldValue"));
    this.store = spy(fakeStore);
    doThrow(new CacheAccessException("")).when(this.store).computeIfPresent(eq("key"), getAnyBiFunction());

    final Ehcache<String, String> ehcache = this.getEhcache(null);

    ehcache.replace("key", "value");
    verify(this.store).computeIfPresent(eq("key"), getAnyBiFunction());
    verify(this.spiedResilienceStrategy).replaceFailure(eq("key"), eq("value"), any(CacheAccessException.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ReplaceOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link Ehcache#replace(Object, Object)} for
   * <ul>
   *   <li>key present in {@code Store}</li>
   *   <li>{@code Store.computeIfPresent} throws</li>
   *   <li>key not present via {@code CacheWriter}</li>
   * </ul>
   */
  @Test
  public void testReplaceHasStoreEntryCacheAccessExceptionNoCacheWriterEntry() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "oldValue"));
    this.store = spy(fakeStore);
    doThrow(new CacheAccessException("")).when(this.store).computeIfPresent(eq("key"), getAnyBiFunction());

    final FakeCacheWriter fakeWriter = new FakeCacheWriter(Collections.<String, String>emptyMap());
    this.cacheWriter = spy(fakeWriter);
    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheWriter);

    final InOrder ordered = inOrder(this.cacheWriter, this.spiedResilienceStrategy);

    ehcache.replace("key", "value");
    verify(this.store).computeIfPresent(eq("key"), getAnyBiFunction());
    verify(this.store, times(1)).remove("key");
    ordered.verify(this.cacheWriter).write(eq("key"), any(String.class));
    ordered.verify(this.spiedResilienceStrategy).replaceFailure(eq("key"), eq("value"), any(CacheAccessException.class));
    assertThat(fakeWriter.getEntryMap().get("key"), is(equalTo("value")));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ReplaceOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link Ehcache#replace(Object, Object)} for
   * <ul>
   *   <li>key present in {@code Store}</li>
   *   <li>{@code Store.computeIfPresent} throws</li>
   *   <li>key present via {@code CacheWriter}</li>
   * </ul>
   */
  @Test
  public void testReplaceHasStoreEntryCacheAccessExceptionHasCacheWriterEntry() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "oldValue"));
    this.store = spy(fakeStore);
    doThrow(new CacheAccessException("")).when(this.store).computeIfPresent(eq("key"), getAnyBiFunction());

    final FakeCacheWriter fakeWriter = new FakeCacheWriter(Collections.singletonMap("key", "oldValue"));
    this.cacheWriter = spy(fakeWriter);
    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheWriter);

    final InOrder ordered = inOrder(this.cacheWriter, this.spiedResilienceStrategy);

    ehcache.replace("key", "value");
    verify(this.store).computeIfPresent(eq("key"), getAnyBiFunction());
    ordered.verify(this.cacheWriter).write(eq("key"), any(String.class));
    ordered.verify(this.spiedResilienceStrategy).replaceFailure(eq("key"), eq("value"), any(CacheAccessException.class));
    assertThat(fakeWriter.getEntryMap().get("key"), is(equalTo("value")));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ReplaceOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link Ehcache#replace(Object, Object)} for
   * <ul>
   *   <li>key present in {@code Store}</li>
   *   <li>{@code Store.computeIfPresent} throws</li>
   *   <li>{@code CacheLoader.write} throws</li>
   * </ul>
   */
  @Test
  public void testReplaceHasStoreEntryCacheAccessExceptionCacheWriterException() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "oldValue"));
    this.store = spy(fakeStore);
    doThrow(new CacheAccessException("")).when(this.store).computeIfPresent(eq("key"), getAnyBiFunction());

    final FakeCacheWriter fakeWriter = new FakeCacheWriter(Collections.singletonMap("key", "oldValue"));
    this.cacheWriter = spy(fakeWriter);
    doThrow(new Exception()).when(this.cacheWriter).write("key", "value");
    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheWriter);

    final InOrder ordered = inOrder(this.cacheWriter, this.spiedResilienceStrategy);

    try {
      ehcache.replace("key", "value");
      fail();
    } catch (CacheWriterException e) {
      // Expected
    }
    verify(this.store).computeIfPresent(eq("key"), getAnyBiFunction());
    ordered.verify(this.cacheWriter).write(eq("key"), any(String.class));
    ordered.verify(this.spiedResilienceStrategy)
        .replaceFailure(eq("key"), eq("value"), any(CacheAccessException.class), any(CacheWriterException.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ReplaceOutcome.FAILURE));
  }
  
  /**
   * Gets an initialized {@link Ehcache Ehcache} instance using the
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
