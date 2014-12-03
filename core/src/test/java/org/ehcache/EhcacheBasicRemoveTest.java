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
 * Provides testing of basic REMOVE(key) operations on an {@code Ehcache}.
 *
 * @author Clifford W. Johnson
 */
public class EhcacheBasicRemoveTest extends EhcacheBasicCrudBase {

  @Mock
  protected CacheWriter<String, String> cacheWriter;

  @Test
  public void testRemoveNull() {
    final Ehcache<String, String> ehcache = this.getEhcache(null);

    try {
      ehcache.remove(null);
      fail();
    } catch (NullPointerException e) {
      // expected
    }
  }

  /**
   * Tests the effect of a {@link org.ehcache.Ehcache#remove(Object)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   *   <li>no {@code CacheWriter}</li>
   * </ul>
   */
  @Test
  public void testRemoveNoStoreEntryNoCacheWriter() throws Exception {
    final FakeStore realStore = new FakeStore(Collections.<String, String>emptyMap());
    this.store = spy(realStore);

    final Ehcache<String, String> ehcache = this.getEhcache(null);

    ehcache.remove("key");
    verify(this.store).compute(eq("key"), getAnyBiFunction());
    verifyZeroInteractions(this.spiedResilienceStrategy);
    assertThat(realStore.getEntryMap().containsKey("key"), is(false));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.RemoveOutcome.SUCCESS));
  }

  /**
   * Tests the effect of a {@link org.ehcache.Ehcache#remove(Object)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   *   <li>key not present via {@code CacheWriter}</li>
   * </ul>
   */
  @Test
  public void testRemoveNoStoreEntryNoCacheWriterEntry() throws Exception {
    final FakeStore realStore = new FakeStore(Collections.<String, String>emptyMap());
    this.store = spy(realStore);

    final FakeCacheWriter realCache = new FakeCacheWriter(Collections.<String, String>emptyMap());
    final Ehcache<String, String> ehcache = this.getEhcache(realCache);

    ehcache.remove("key");
    verify(this.store).compute(eq("key"), getAnyBiFunction());
    verifyZeroInteractions(this.spiedResilienceStrategy);
    assertThat(realStore.getEntryMap().containsKey("key"), is(false));
    assertThat(realCache.getEntryMap().containsKey("key"), is(false));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.RemoveOutcome.SUCCESS));
  }

  /**
   * Tests the effect of a {@link org.ehcache.Ehcache#remove(Object)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   *   <li>key present via {@code CacheWriter}</li>
   * </ul>
   */
  @Test
  public void testRemoveNoStoreEntryHasCacheWriterEntry() throws Exception {
    final FakeStore realStore = new FakeStore(Collections.<String, String>emptyMap());
    this.store = spy(realStore);

    final FakeCacheWriter realCache = new FakeCacheWriter(Collections.singletonMap("key", "oldValue"));
    final Ehcache<String, String> ehcache = this.getEhcache(realCache);

    ehcache.remove("key");
    verify(this.store).compute(eq("key"), getAnyBiFunction());
    verifyZeroInteractions(this.spiedResilienceStrategy);
    assertThat(realStore.getEntryMap().containsKey("key"), is(false));
    assertThat(realCache.getEntryMap().containsKey("key"), is(false));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.RemoveOutcome.SUCCESS));
  }

  /**
   * Tests the effect of a {@link org.ehcache.Ehcache#remove(Object)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   *   <li>{@code CacheWriter.delete} throws</li>
   * </ul>
   */
  @Test
  public void testRemoveNoStoreEntryCacheWriterException() throws Exception {
    final FakeStore realStore = new FakeStore(Collections.<String, String>emptyMap());
    this.store = spy(realStore);

    final FakeCacheWriter realCache = new FakeCacheWriter(Collections.singletonMap("key", "oldValue"));
    this.cacheWriter = spy(realCache);
    doThrow(new Exception()).when(this.cacheWriter).delete("key");
    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheWriter);

    try {
      ehcache.remove("key");
      fail();
    } catch (CacheWriterException e) {
      // Expected
    }
    verify(this.store).compute(eq("key"), getAnyBiFunction());
    verifyZeroInteractions(this.spiedResilienceStrategy);
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.RemoveOutcome.class));
  }

  /**
   * Tests the effect of a {@link org.ehcache.Ehcache#remove(Object)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   *   <li>{@code Store.compute} throws</li>
   *   <li>no {@code CacheWriter}</li>
   * </ul>
   */
  @Test
  public void testRemoveNoStoreEntryCacheAccessExceptionNoCacheWriter() throws Exception {
    final FakeStore realStore = new FakeStore(Collections.<String, String>emptyMap());
    this.store = spy(realStore);
    doThrow(new CacheAccessException("")).when(this.store).compute(eq("key"), getAnyBiFunction());

    final Ehcache<String, String> ehcache = this.getEhcache(null);

    ehcache.remove("key");
    verify(this.store).compute(eq("key"), getAnyBiFunction());
    verify(this.spiedResilienceStrategy).removeFailure(eq("key"), any(CacheAccessException.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.RemoveOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link org.ehcache.Ehcache#remove(Object)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   *   <li>{@code Store.compute} throws</li>
   *   <li>key not present via {@code CacheWriter}</li>
   * </ul>
   */
  @Test
  public void testRemoveNoStoreEntryCacheAccessExceptionNoCacheWriterEntry() throws Exception {
    final FakeStore realStore = new FakeStore(Collections.<String, String>emptyMap());
    this.store = spy(realStore);
    doThrow(new CacheAccessException("")).when(this.store).compute(eq("key"), getAnyBiFunction());

    final FakeCacheWriter realCache = new FakeCacheWriter(Collections.<String, String>emptyMap());
    this.cacheWriter = spy(realCache);
    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheWriter);

    final InOrder ordered = inOrder(this.cacheWriter, this.spiedResilienceStrategy);

    ehcache.remove("key");
    verify(this.store).compute(eq("key"), getAnyBiFunction());
    ordered.verify(this.cacheWriter).delete(eq("key"));
    ordered.verify(this.spiedResilienceStrategy).removeFailure(eq("key"), any(CacheAccessException.class));
    assertThat(realCache.getEntryMap().containsKey("key"), is(false));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.RemoveOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link org.ehcache.Ehcache#remove(Object)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   *   <li>{@code Store.compute} throws</li>
   *   <li>key present via {@code CacheWriter}</li>
   * </ul>
   */
  @Test
  public void testRemoveNoStoreEntryCacheAccessExceptionHasCacheWriterEntry() throws Exception {
    final FakeStore realStore = new FakeStore(Collections.<String, String>emptyMap());
    this.store = spy(realStore);
    doThrow(new CacheAccessException("")).when(this.store).compute(eq("key"), getAnyBiFunction());

    final FakeCacheWriter realCache = new FakeCacheWriter(Collections.singletonMap("key", "oldValue"));
    this.cacheWriter = spy(realCache);
    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheWriter);

    final InOrder ordered = inOrder(this.cacheWriter, this.spiedResilienceStrategy);

    ehcache.remove("key");
    verify(this.store).compute(eq("key"), getAnyBiFunction());
    ordered.verify(this.cacheWriter).delete(eq("key"));
    ordered.verify(this.spiedResilienceStrategy).removeFailure(eq("key"), any(CacheAccessException.class));
    assertThat(realCache.getEntryMap().containsKey("key"), is(false));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.RemoveOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link org.ehcache.Ehcache#remove(Object)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   *   <li>{@code Store.compute} throws</li>
   *   <li>{@code CacheWriter.delete} throws</li>
   * </ul>
   */
  @Test
  public void testRemoveNoStoreEntryCacheAccessExceptionCacheWriterException() throws Exception {
    final FakeStore realStore = new FakeStore(Collections.<String, String>emptyMap());
    this.store = spy(realStore);
    doThrow(new CacheAccessException("")).when(this.store).compute(eq("key"), getAnyBiFunction());

    final FakeCacheWriter realCache = new FakeCacheWriter(Collections.singletonMap("key", "oldValue"));
    this.cacheWriter = spy(realCache);
    doThrow(new Exception()).when(this.cacheWriter).delete("key");
    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheWriter);

    final InOrder ordered = inOrder(this.cacheWriter, this.spiedResilienceStrategy);

    try {
      ehcache.remove("key");
      fail();
    } catch (CacheWriterException e) {
      // Expected
    }
    verify(this.store).compute(eq("key"), getAnyBiFunction());
    ordered.verify(this.cacheWriter).delete(eq("key"));
    ordered.verify(this.spiedResilienceStrategy)
        .removeFailure(eq("key"), any(CacheAccessException.class), any(CacheWriterException.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.RemoveOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link org.ehcache.Ehcache#remove(Object)} for
   * <ul>
   *   <li>key present in {@code Store}</li>
   *   <li>no {@code CacheWriter}</li>
   * </ul>
   */
  @Test
  public void testRemoveHasStoreEntryNoCacheWriter() throws Exception {
    final FakeStore realStore = new FakeStore(Collections.singletonMap("key", "oldValue"));
    this.store = spy(realStore);

    final Ehcache<String, String> ehcache = this.getEhcache(null);

    ehcache.remove("key");
    verify(this.store).compute(eq("key"), getAnyBiFunction());
    verifyZeroInteractions(this.spiedResilienceStrategy);
    assertThat(realStore.getEntryMap().containsKey("key"), is(false));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.RemoveOutcome.SUCCESS));
  }

  /**
   * Tests the effect of a {@link org.ehcache.Ehcache#remove(Object)} for
   * <ul>
   *   <li>key present in {@code Store}</li>
   *   <li>key not present via {@code CacheWriter}</li>
   * </ul>
   */
  @Test
  public void testRemoveHasStoreEntryNoCacheWriterEntry() throws Exception {
    final FakeStore realStore = new FakeStore(Collections.singletonMap("key", "oldValue"));
    this.store = spy(realStore);

    final FakeCacheWriter realCache = new FakeCacheWriter(Collections.<String, String>emptyMap());
    final Ehcache<String, String> ehcache = this.getEhcache(realCache);

    ehcache.remove("key");
    verify(this.store).compute(eq("key"), getAnyBiFunction());
    verifyZeroInteractions(this.spiedResilienceStrategy);
    assertThat(realStore.getEntryMap().containsKey("key"), is(false));
    assertThat(realCache.getEntryMap().containsKey("key"), is(false));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.RemoveOutcome.SUCCESS));
  }

  /**
   * Tests the effect of a {@link org.ehcache.Ehcache#remove(Object)} for
   * <ul>
   *   <li>key present in {@code Store}</li>
   *   <li>key present via {@code CacheWriter}</li>
   * </ul>
   */
  @Test
  public void testRemoveHasStoreEntryHasCacheWriterEntry() throws Exception {
    final FakeStore realStore = new FakeStore(Collections.singletonMap("key", "oldValue"));
    this.store = spy(realStore);

    final FakeCacheWriter realCache = new FakeCacheWriter(Collections.singletonMap("key", "oldValue"));
    final Ehcache<String, String> ehcache = this.getEhcache(realCache);

    ehcache.remove("key");
    verify(this.store).compute(eq("key"), getAnyBiFunction());
    verifyZeroInteractions(this.spiedResilienceStrategy);
    assertThat(realStore.getEntryMap().containsKey("key"), is(false));
    assertThat(realCache.getEntryMap().containsKey("key"), is(false));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.RemoveOutcome.SUCCESS));
  }

  /**
   * Tests the effect of a {@link org.ehcache.Ehcache#remove(Object)} for
   * <ul>
   *   <li>key present in {@code Store}</li>
   *   <li>{@code CacheWriter.delete} throws</li>
   * </ul>
   */
  @Test
  public void testRemoveHasStoreEntryCacheWriterException() throws Exception {
    final FakeStore realStore = new FakeStore(Collections.singletonMap("key", "oldValue"));
    this.store = spy(realStore);

    final FakeCacheWriter realCache = new FakeCacheWriter(Collections.singletonMap("key", "oldValue"));
    this.cacheWriter = spy(realCache);
    doThrow(new Exception()).when(this.cacheWriter).delete("key");
    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheWriter);

    try {
      ehcache.remove("key");
      fail();
    } catch (CacheWriterException e) {
      // Expected
    }
    verify(this.store).compute(eq("key"), getAnyBiFunction());
    verifyZeroInteractions(this.spiedResilienceStrategy);
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.RemoveOutcome.class));
  }

  /**
   * Tests the effect of a {@link org.ehcache.Ehcache#remove(Object)} for
   * <ul>
   *   <li>key present in {@code Store}</li>
   *   <li>{@code Store.compute} throws</li>
   *   <li>no {@code CacheWriter}</li>
   * </ul>
   */
  @Test
  public void testRemoveHasStoreEntryCacheAccessExceptionNoCacheWriter() throws Exception {
    final FakeStore realStore = new FakeStore(Collections.singletonMap("key", "oldValue"));
    this.store = spy(realStore);
    doThrow(new CacheAccessException("")).when(this.store).compute(eq("key"), getAnyBiFunction());

    final Ehcache<String, String> ehcache = this.getEhcache(null);

    ehcache.remove("key");
    verify(this.store).compute(eq("key"), getAnyBiFunction());
    verify(this.spiedResilienceStrategy).removeFailure(eq("key"), any(CacheAccessException.class));
    assertThat(realStore.getEntryMap().containsKey("key"), is(false));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.RemoveOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link org.ehcache.Ehcache#remove(Object)} for
   * <ul>
   *   <li>key present in {@code Store}</li>
   *   <li>{@code Store.compute} throws</li>
   *   <li>key not present via {@code CacheWriter}</li>
   * </ul>
   */
  @Test
  public void testRemoveHasStoreEntryCacheAccessExceptionNoCacheWriterEntry() throws Exception {
    final FakeStore realStore = new FakeStore(Collections.singletonMap("key", "oldValue"));
    this.store = spy(realStore);
    doThrow(new CacheAccessException("")).when(this.store).compute(eq("key"), getAnyBiFunction());

    final FakeCacheWriter realCache = new FakeCacheWriter(Collections.<String, String>emptyMap());
    this.cacheWriter = spy(realCache);
    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheWriter);

    final InOrder ordered = inOrder(this.cacheWriter, this.spiedResilienceStrategy);

    ehcache.remove("key");
    verify(this.store).compute(eq("key"), getAnyBiFunction());
    ordered.verify(this.cacheWriter).delete(eq("key"));
    ordered.verify(this.spiedResilienceStrategy).removeFailure(eq("key"), any(CacheAccessException.class));
    assertThat(realCache.getEntryMap().containsKey("key"), is(false));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.RemoveOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link org.ehcache.Ehcache#remove(Object)} for
   * <ul>
   *   <li>key present in {@code Store}</li>
   *   <li>{@code Store.compute} throws</li>
   *   <li>key present via {@code CacheWriter}</li>
   * </ul>
   */
  @Test
  public void testRemoveHasStoreEntryCacheAccessExceptionHasCacheWriterEntry() throws Exception {
    final FakeStore realStore = new FakeStore(Collections.singletonMap("key", "oldValue"));
    this.store = spy(realStore);
    doThrow(new CacheAccessException("")).when(this.store).compute(eq("key"), getAnyBiFunction());

    final FakeCacheWriter realCache = new FakeCacheWriter(Collections.singletonMap("key", "oldValue"));
    this.cacheWriter = spy(realCache);
    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheWriter);

    final InOrder ordered = inOrder(this.cacheWriter, this.spiedResilienceStrategy);

    ehcache.remove("key");
    verify(this.store).compute(eq("key"), getAnyBiFunction());
    ordered.verify(this.cacheWriter).delete(eq("key"));
    ordered.verify(this.spiedResilienceStrategy).removeFailure(eq("key"), any(CacheAccessException.class));
    assertThat(realCache.getEntryMap().containsKey("key"), is(false));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.RemoveOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link org.ehcache.Ehcache#remove(Object)} for
   * <ul>
   *   <li>key present in {@code Store}</li>
   *   <li>{@code Store.compute} throws</li>
   *   <li>{@code CacheWriter.delete} throws</li>
   * </ul>
   */
  @Test
  public void testRemoveHasStoreEntryCacheAccessExceptionCacheWriterException() throws Exception {
    final FakeStore realStore = new FakeStore(Collections.singletonMap("key", "oldValue"));
    this.store = spy(realStore);
    doThrow(new CacheAccessException("")).when(this.store).compute(eq("key"), getAnyBiFunction());

    final FakeCacheWriter realCache = new FakeCacheWriter(Collections.singletonMap("key", "oldValue"));
    this.cacheWriter = spy(realCache);
    doThrow(new Exception()).when(this.cacheWriter).delete("key");
    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheWriter);

    final InOrder ordered = inOrder(this.cacheWriter, this.spiedResilienceStrategy);

    try {
      ehcache.remove("key");
      fail();
    } catch (CacheWriterException e) {
      // Expected
    }
    verify(this.store).compute(eq("key"), getAnyBiFunction());
    ordered.verify(this.cacheWriter).delete(eq("key"));
    ordered.verify(this.spiedResilienceStrategy)
        .removeFailure(eq("key"), any(CacheAccessException.class), any(CacheWriterException.class));
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
