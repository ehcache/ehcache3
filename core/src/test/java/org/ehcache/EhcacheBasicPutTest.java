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
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Provides testing of basic PUT operations on an {@code Ehcache}.
 *
 * @author Clifford W. Johnson
 */
public class EhcacheBasicPutTest extends EhcacheBasicCrudBase {

  @Mock
  protected CacheWriter<String, String> cacheWriter;

  @Test
  public void testPutNullNull() {
    final Ehcache<String, String> ehcache = this.getEhcache(null);

    try {
      ehcache.put(null, null);
      fail();
    } catch (NullPointerException e) {
      // expected
    }
  }

  @Test
  public void testPutKeyNull() {
    final Ehcache<String, String> ehcache = this.getEhcache(null);

    try {
      ehcache.put("key", null);
      fail();
    } catch (NullPointerException e) {
      // expected
    }
  }

  @Test
  public void testPutNullValue() {
    final Ehcache<String, String> ehcache = this.getEhcache(null);

    try {
      ehcache.put(null, "value");
      fail();
    } catch (NullPointerException e) {
      // expected
    }
  }

  /**
   * Tests the effect of a {@link Ehcache#put(Object, Object)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   *   <li>no {@code CacheWriter}</li>
   * </ul>
   */
  @Test
  public void testPutNoStoreEntryNoCacheWriter() throws Exception {
    final MockStore realStore = new MockStore(Collections.<String, String>emptyMap());
    this.store = spy(realStore);

    final Ehcache<String, String> ehcache = this.getEhcache(null);

    ehcache.put("key", "value");
    verify(this.store).compute(eq("key"), getAnyBiFunction());
    verify(this.store, never()).remove("key");
    assertThat(realStore.getMap().get("key"), equalTo("value"));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.PutOutcome.ADDED));
  }

  /**
   * Tests the effect of a {@link Ehcache#put(Object, Object)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   *   <li>key not present via {@code CacheWriter}</li>
   * </ul>
   */
  @Test
  public void testPutNoStoreEntryNoCacheWriterEntry() throws Exception {
    final MockStore realStore = new MockStore(Collections.<String, String>emptyMap());
    this.store = spy(realStore);

    final MockCacheWriter realCache = new MockCacheWriter(Collections.<String, String>emptyMap());
    final Ehcache<String, String> ehcache = this.getEhcache(realCache);

    ehcache.put("key", "value");
    verify(this.store).compute(eq("key"), getAnyBiFunction());
    verify(this.store, never()).remove("key");
    assertThat(realStore.getMap().get("key"), equalTo("value"));
    assertThat(realCache.getEntries().get("key"), equalTo("value"));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.PutOutcome.ADDED));
  }

  /**
   * Tests the effect of a {@link Ehcache#put(Object, Object)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   *   <li>key present via {@code CacheWriter}</li>
   * </ul>
   */
  @Test
  public void testPutNoStoreEntryHasCacheWriterEntry() throws Exception {
    final MockStore realStore = new MockStore(Collections.<String, String>emptyMap());
    this.store = spy(realStore);

    final MockCacheWriter realCache = new MockCacheWriter(Collections.singletonMap("key", "oldValue"));
    final Ehcache<String, String> ehcache = this.getEhcache(realCache);

    ehcache.put("key", "value");
    verify(this.store).compute(eq("key"), getAnyBiFunction());
    verify(this.store, never()).remove("key");
    assertThat(realStore.getMap().get("key"), equalTo("value"));
    assertThat(realCache.getEntries().get("key"), equalTo("value"));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.PutOutcome.ADDED));
  }

  /**
   * Tests the effect of a {@link Ehcache#put(Object, Object)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   *   <li>{@code CacheLoader.write} throws</li>
   * </ul>
   */
  @Test
  public void testPutNoStoreEntryCacheWriterException() throws Exception {
    final MockStore realStore = new MockStore(Collections.<String, String>emptyMap());
    this.store = spy(realStore);

    final MockCacheWriter realCache = new MockCacheWriter(Collections.singletonMap("key", "oldValue"));
    this.cacheWriter = spy(realCache);
    doThrow(new Exception()).when(this.cacheWriter).write("key", "value");
    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheWriter);

    try {
      ehcache.put("key", "value");
      fail();
    } catch (CacheWriterException e) {
      // Expected
    }
    verify(this.store).compute(eq("key"), getAnyBiFunction());
    verify(this.store, times(1)).remove("key");
    assertThat(realStore.getMap().containsKey("key"), is(false));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.PutOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link Ehcache#put(Object, Object)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   *   <li>{@code Store.compute} throws</li>
   *   <li>no {@code CacheWriter}</li>
   * </ul>
   */
  @Test
  public void testPutNoStoreEntryCacheAccessExceptionNoCacheWriter() throws Exception {
    final MockStore realStore = new MockStore(Collections.<String, String>emptyMap());
    this.store = spy(realStore);
    doThrow(new CacheAccessException("")).when(this.store).compute(eq("key"), getAnyBiFunction());

    final Ehcache<String, String> ehcache = this.getEhcache(null);

    ehcache.put("key", "value");
    verify(this.store).compute(eq("key"), getAnyBiFunction());
    verify(this.store, times(1)).remove("key");
    assertThat(realStore.getMap().containsKey("key"), is(false));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.PutOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link Ehcache#put(Object, Object)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   *   <li>{@code Store.compute} throws</li>
   *   <li>key not present via {@code CacheWriter}</li>
   * </ul>
   */
  @Test
  public void testPutNoStoreEntryCacheAccessExceptionNoCacheWriterEntry() throws Exception {
    final MockStore realStore = new MockStore(Collections.<String, String>emptyMap());
    this.store = spy(realStore);
    doThrow(new CacheAccessException("")).when(this.store).compute(eq("key"), getAnyBiFunction());

    final MockCacheWriter realCache = new MockCacheWriter(Collections.<String, String>emptyMap());
    final Ehcache<String, String> ehcache = this.getEhcache(realCache);

    ehcache.put("key", "value");
    verify(this.store).compute(eq("key"), getAnyBiFunction());
    verify(this.store, times(1)).remove("key");
    assertThat(realStore.getMap().containsKey("key"), is(false));
    assertThat(realCache.getEntries().get("key"), equalTo("value"));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.PutOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link Ehcache#put(Object, Object)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   *   <li>{@code Store.compute} throws</li>
   *   <li>key present via {@code CacheWriter}</li>
   * </ul>
   */
  @Test
  public void testPutNoStoreEntryCacheAccessExceptionHasCacheWriterEntry() throws Exception {
    final MockStore realStore = new MockStore(Collections.<String, String>emptyMap());
    this.store = spy(realStore);
    doThrow(new CacheAccessException("")).when(this.store).compute(eq("key"), getAnyBiFunction());

    final MockCacheWriter realCache = new MockCacheWriter(Collections.singletonMap("key", "oldValue"));
    final Ehcache<String, String> ehcache = this.getEhcache(realCache);

    ehcache.put("key", "value");
    verify(this.store).compute(eq("key"), getAnyBiFunction());
    verify(this.store, times(1)).remove("key");
    assertThat(realStore.getMap().containsKey("key"), is(false));
    assertThat(realCache.getEntries().get("key"), equalTo("value"));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.PutOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link Ehcache#put(Object, Object)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   *   <li>{@code Store.compute} throws</li>
   *   <li>{@code CacheLoader.write} throws</li>
   * </ul>
   */
  @Test
  public void testPutNoStoreEntryCacheAccessExceptionCacheWriterException() throws Exception {
    final MockStore realStore = new MockStore(Collections.<String, String>emptyMap());
    this.store = spy(realStore);
    doThrow(new CacheAccessException("")).when(this.store).compute(eq("key"), getAnyBiFunction());

    final MockCacheWriter realCache = new MockCacheWriter(Collections.singletonMap("key", "oldValue"));
    this.cacheWriter = spy(realCache);
    doThrow(new Exception()).when(this.cacheWriter).write("key", "value");
    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheWriter);

    try {
      ehcache.put("key", "value");
      fail();
    } catch (CacheWriterException e) {
      // Expected
    }
    verify(this.store).compute(eq("key"), getAnyBiFunction());
    verify(this.store, times(1)).remove("key");
    assertThat(realStore.getMap().containsKey("key"), is(false));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.PutOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link Ehcache#put(Object, Object)} for
   * <ul>
   *   <li>key present in {@code Store}</li>
   *   <li>no {@code CacheWriter}</li>
   * </ul>
   */
  @Test
  public void testPutHasStoreEntryNoCacheWriter() throws Exception {
    final MockStore realStore = new MockStore(Collections.singletonMap("key", "oldValue"));
    this.store = spy(realStore);

    final Ehcache<String, String> ehcache = this.getEhcache(null);

    ehcache.put("key", "value");
    verify(this.store).compute(eq("key"), getAnyBiFunction());
    verify(this.store, never()).remove("key");
    assertThat(realStore.getMap().get("key"), equalTo("value"));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.PutOutcome.ADDED));
  }

  /**
   * Tests the effect of a {@link Ehcache#put(Object, Object)} for
   * <ul>
   *   <li>key present in {@code Store}</li>
   *   <li>key not present via {@code CacheWriter}</li>
   * </ul>
   */
  @Test
  public void testPutHasStoreEntryNoCacheWriterEntry() throws Exception {
    final MockStore realStore = new MockStore(Collections.singletonMap("key", "oldValue"));
    this.store = spy(realStore);

    final MockCacheWriter realCache = new MockCacheWriter(Collections.<String, String>emptyMap());
    final Ehcache<String, String> ehcache = this.getEhcache(realCache);

    ehcache.put("key", "value");
    verify(this.store).compute(eq("key"), getAnyBiFunction());
    verify(this.store, never()).remove("key");
    assertThat(realStore.getMap().get("key"), equalTo("value"));
    assertThat(realCache.getEntries().get("key"), equalTo("value"));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.PutOutcome.ADDED));
  }

  /**
   * Tests the effect of a {@link Ehcache#put(Object, Object)} for
   * <ul>
   *   <li>key present in {@code Store}</li>
   *   <li>key present via {@code CacheWriter}</li>
   * </ul>
   */
  @Test
  public void testPutHasStoreEntryHasCacheWriterEntry() throws Exception {
    final MockStore realStore = new MockStore(Collections.singletonMap("key", "oldValue"));
    this.store = spy(realStore);

    final MockCacheWriter realCache = new MockCacheWriter(Collections.singletonMap("key", "oldValue"));
    final Ehcache<String, String> ehcache = this.getEhcache(realCache);

    ehcache.put("key", "value");
    verify(this.store).compute(eq("key"), getAnyBiFunction());
    verify(this.store, never()).remove("key");
    assertThat(realStore.getMap().get("key"), equalTo("value"));
    assertThat(realCache.getEntries().get("key"), equalTo("value"));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.PutOutcome.ADDED));
  }

  /**
   * Tests the effect of a {@link Ehcache#put(Object, Object)} for
   * <ul>
   *   <li>key present in {@code Store}</li>
   *   <li>{@code CacheLoader.write} throws</li>
   * </ul>
   */
  @Test
  public void testPutHasStoreEntryCacheWriterException() throws Exception {
    final MockStore realStore = new MockStore(Collections.singletonMap("key", "oldValue"));
    this.store = spy(realStore);

    final MockCacheWriter realCache = new MockCacheWriter(Collections.singletonMap("key", "oldValue"));
    this.cacheWriter = spy(realCache);
    doThrow(new Exception()).when(this.cacheWriter).write("key", "value");
    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheWriter);

    try {
      ehcache.put("key", "value");
      fail();
    } catch (CacheWriterException e) {
      // Expected
    }
    verify(this.store).compute(eq("key"), getAnyBiFunction());
    verify(this.store, times(1)).remove("key");
    assertThat(realStore.getMap().containsKey("key"), is(false));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.PutOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link Ehcache#put(Object, Object)} for
   * <ul>
   *   <li>key present in {@code Store}</li>
   *   <li>{@code Store.compute} throws</li>
   *   <li>no {@code CacheWriter}</li>
   * </ul>
   */
  @Test
  public void testPutHasStoreEntryCacheAccessExceptionNoCacheWriter() throws Exception {
    final MockStore realStore = new MockStore(Collections.singletonMap("key", "oldValue"));
    this.store = spy(realStore);
    doThrow(new CacheAccessException("")).when(this.store).compute(eq("key"), getAnyBiFunction());

    final Ehcache<String, String> ehcache = this.getEhcache(null);

    ehcache.put("key", "value");
    verify(this.store).compute(eq("key"), getAnyBiFunction());
    verify(this.store, times(1)).remove("key");
    assertThat(realStore.getMap().containsKey("key"), is(false));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.PutOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link Ehcache#put(Object, Object)} for
   * <ul>
   *   <li>key present in {@code Store}</li>
   *   <li>{@code Store.compute} throws</li>
   *   <li>key not present via {@code CacheWriter}</li>
   * </ul>
   */
  @Test
  public void testPutHasStoreEntryCacheAccessExceptionNoCacheWriterEntry() throws Exception {
    final MockStore realStore = new MockStore(Collections.singletonMap("key", "oldValue"));
    this.store = spy(realStore);
    doThrow(new CacheAccessException("")).when(this.store).compute(eq("key"), getAnyBiFunction());

    final MockCacheWriter realCache = new MockCacheWriter(Collections.<String, String>emptyMap());
    final Ehcache<String, String> ehcache = this.getEhcache(realCache);

    ehcache.put("key", "value");
    verify(this.store).compute(eq("key"), getAnyBiFunction());
    verify(this.store, times(1)).remove("key");
    assertThat(realStore.getMap().containsKey("key"), is(false));
    assertThat(realCache.getEntries().get("key"), equalTo("value"));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.PutOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link Ehcache#put(Object, Object)} for
   * <ul>
   *   <li>key present in {@code Store}</li>
   *   <li>{@code Store.compute} throws</li>
   *   <li>key present via {@code CacheWriter}</li>
   * </ul>
   */
  @Test
  public void testPutHasStoreEntryCacheAccessExceptionHasCacheWriterEntry() throws Exception {
    final MockStore realStore = new MockStore(Collections.singletonMap("key", "oldValue"));
    this.store = spy(realStore);
    doThrow(new CacheAccessException("")).when(this.store).compute(eq("key"), getAnyBiFunction());

    final MockCacheWriter realCache = new MockCacheWriter(Collections.singletonMap("key", "oldValue"));
    final Ehcache<String, String> ehcache = this.getEhcache(realCache);

    ehcache.put("key", "value");
    verify(this.store).compute(eq("key"), getAnyBiFunction());
    verify(this.store, times(1)).remove("key");
    assertThat(realStore.getMap().containsKey("key"), is(false));
    assertThat(realCache.getEntries().get("key"), equalTo("value"));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.PutOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link Ehcache#put(Object, Object)} for
   * <ul>
   *   <li>key present in {@code Store}</li>
   *   <li>{@code Store.compute} throws</li>
   *   <li>{@code CacheLoader.write} throws</li>
   * </ul>
   */
  @Test
  public void testPutHasStoreEntryCacheAccessExceptionCacheWriterException() throws Exception {
    final MockStore realStore = new MockStore(Collections.singletonMap("key", "oldValue"));
    this.store = spy(realStore);
    doThrow(new CacheAccessException("")).when(this.store).compute(eq("key"), getAnyBiFunction());

    final MockCacheWriter realCache = new MockCacheWriter(Collections.singletonMap("key", "oldValue"));
    this.cacheWriter = spy(realCache);
    doThrow(new Exception()).when(this.cacheWriter).write("key", "value");
    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheWriter);

    try {
      ehcache.put("key", "value");
      fail();
    } catch (CacheWriterException e) {
      // Expected
    }
    verify(this.store).compute(eq("key"), getAnyBiFunction());
    verify(this.store, times(1)).remove("key");
    assertThat(realStore.getMap().containsKey("key"), is(false));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.PutOutcome.FAILURE));
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
    return ehcache;
  }
}
