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

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
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
 * Provides testing of basic PUT_IF_ABSENT operations on an {@code Ehcache}.
 *
 * @author Clifford W. Johnson
 */
public class EhcacheBasicPutIfAbsentTest extends EhcacheBasicCrudBase {

  @Mock
  protected CacheWriter<String, String> cacheWriter;

  @Test
  public void testPutIfAbsentNullNull() {
    final Ehcache<String, String> ehcache = this.getEhcache(null);

    try {
      ehcache.putIfAbsent(null, null);
      fail();
    } catch (NullPointerException e) {
      // expected
    }
  }

  @Test
  public void testPutIfAbsentKeyNull() {
    final Ehcache<String, String> ehcache = this.getEhcache(null);

    try {
      ehcache.putIfAbsent("key", null);
      fail();
    } catch (NullPointerException e) {
      // expected
    }
  }

  @Test
  public void testPutIfAbsentNullValue() {
    final Ehcache<String, String> ehcache = this.getEhcache(null);

    try {
      ehcache.putIfAbsent(null, "value");
      fail();
    } catch (NullPointerException e) {
      // expected
    }
  }

  /**
   * Tests the effect of a {@link Ehcache#putIfAbsent(Object, Object)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   *   <li>no {@code CacheWriter}</li>
   * </ul>
   */
  @Test
  public void testPutIfAbsentNoStoreEntryNoCacheWriter() throws Exception {
    final MockStore realStore = new MockStore(Collections.<String, String>emptyMap());
    this.store = spy(realStore);

    final Ehcache<String, String> ehcache = this.getEhcache(null);

    assertThat(ehcache.putIfAbsent("key", "value"), is(nullValue()));
    verify(this.store).computeIfAbsent(eq("key"), getAnyFunction());
    verifyZeroInteractions(this.spiedResilienceStrategy);
    assertThat(realStore.getMap().get("key"), equalTo("value"));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.PutIfAbsentOutcome.PUT));
  }

  /**
   * Tests the effect of a {@link Ehcache#putIfAbsent(Object, Object)} for
   * <ul>
   *   <li>key present in {@code Store}</li>
   *   <li>no {@code CacheWriter}</li>
   * </ul>
   */
  @Test
  public void testPutIfAbsentHasStoreEntryNoCacheWriter() throws Exception {
    final MockStore realStore = new MockStore(Collections.singletonMap("key", "oldValue"));
    this.store = spy(realStore);

    final Ehcache<String, String> ehcache = this.getEhcache(null);

    assertThat(ehcache.putIfAbsent("key", "value"), is(equalTo("oldValue")));
    verify(this.store).computeIfAbsent(eq("key"), getAnyFunction());
    verifyZeroInteractions(this.spiedResilienceStrategy);
    assertThat(realStore.getMap().get("key"), equalTo("oldValue"));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.PutIfAbsentOutcome.HIT));
  }

  /**
   * Tests the effect of a {@link Ehcache#putIfAbsent(Object, Object)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   *   <li>{@code Store.computeIfAbsent} throws</li>
   *   <li>no {@code CacheWriter}</li>
   * </ul>
   */
  @Test
  public void testPutIfAbsentNoStoreEntryCacheAccessExceptionNoCacheWriter() throws Exception {
    final MockStore realStore = new MockStore(Collections.<String, String>emptyMap());
    this.store = spy(realStore);
    doThrow(new CacheAccessException("")).when(this.store).computeIfAbsent(eq("key"), getAnyFunction());

    final Ehcache<String, String> ehcache = this.getEhcache(null);

    assertThat(ehcache.putIfAbsent("key", "value"), is(nullValue()));
    verify(this.store).computeIfAbsent(eq("key"), getAnyFunction());
    verify(this.spiedResilienceStrategy)
        .putIfAbsentFailure(eq("key"), eq("value"), any(CacheAccessException.class), eq(false));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.PutIfAbsentOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link Ehcache#putIfAbsent(Object, Object)} for
   * <ul>
   *   <li>key present in {@code Store}</li>
   *   <li>{@code Store.computeIfAbsent} throws</li>
   *   <li>no {@code CacheWriter}</li>
   * </ul>
   */
  @Test
  public void testPutIfAbsentHasStoreEntryCacheAccessExceptionNoCacheWriter() throws Exception {
    final MockStore realStore = new MockStore(Collections.singletonMap("key", "oldValue"));
    this.store = spy(realStore);
    doThrow(new CacheAccessException("")).when(this.store).computeIfAbsent(eq("key"), getAnyFunction());

    final Ehcache<String, String> ehcache = this.getEhcache(null);

    assertThat(ehcache.putIfAbsent("key", "value"), is(nullValue()));   // TODO: Confirm correctness
    verify(this.store).computeIfAbsent(eq("key"), getAnyFunction());
    verify(this.spiedResilienceStrategy)
        .putIfAbsentFailure(eq("key"), eq("value"), any(CacheAccessException.class), eq(false));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.PutIfAbsentOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link Ehcache#putIfAbsent(Object, Object)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   *   <li>key present via {@code CacheWriter}</li>
   * </ul>
   */
  @Test
  public void testPutIfAbsentNoStoreEntryNoCacheWriterEntry() throws Exception {
    final MockStore realStore = new MockStore(Collections.<String, String>emptyMap());
    this.store = spy(realStore);

    final MockCacheWriter realCache = new MockCacheWriter(Collections.<String, String>emptyMap());
    final Ehcache<String, String> ehcache = this.getEhcache(realCache);

    assertThat(ehcache.putIfAbsent("key", "value"), is(nullValue()));
    verify(this.store).computeIfAbsent(eq("key"), getAnyFunction());
    verifyZeroInteractions(this.spiedResilienceStrategy);
    assertThat(realStore.getMap().get("key"), equalTo("value"));
    assertThat(realCache.getEntries().get("key"), equalTo("value"));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.PutIfAbsentOutcome.PUT));
  }

  /**
   * Tests the effect of a {@link Ehcache#putIfAbsent(Object, Object)} for
   * <ul>
   *   <li>key present in {@code Store}</li>
   *   <li>key not present via {@code CacheWriter}</li>
   * </ul>
   */
  @Test
  public void testPutIfAbsentHasStoreEntryNoCacheWriterEntry() throws Exception {
    final MockStore realStore = new MockStore(Collections.singletonMap("key", "oldValue"));
    this.store = spy(realStore);

    final MockCacheWriter realCache = new MockCacheWriter(Collections.<String, String>emptyMap());
    final Ehcache<String, String> ehcache = this.getEhcache(realCache);

    assertThat(ehcache.putIfAbsent("key", "value"), is(equalTo("oldValue")));
    verify(this.store).computeIfAbsent(eq("key"), getAnyFunction());
    verifyZeroInteractions(this.spiedResilienceStrategy);
    assertThat(realStore.getMap().get("key"), equalTo("oldValue"));
    assertThat(realCache.getEntries().containsKey("key"), is(false));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.PutIfAbsentOutcome.HIT));
  }

  /**
   * Tests the effect of a {@link Ehcache#putIfAbsent(Object, Object)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   *   <li>{@code Store.computeIfAbsent} throws</li>
   *   <li>key not present via {@code CacheWriter}</li>
   * </ul>
   */
  @Test
  public void testPutIfAbsentNoStoreEntryCacheAccessExceptionNoCacheWriterEntry() throws Exception {
    final MockStore realStore = new MockStore(Collections.<String, String>emptyMap());
    this.store = spy(realStore);
    doThrow(new CacheAccessException("")).when(this.store).computeIfAbsent(eq("key"), getAnyFunction());

    final MockCacheWriter realCache = new MockCacheWriter(Collections.<String, String>emptyMap());
    this.cacheWriter = spy(realCache);
    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheWriter);

    final InOrder ordered = inOrder(this.cacheWriter, this.spiedResilienceStrategy);

    assertThat(ehcache.putIfAbsent("key", "value"), is(nullValue()));
    verify(this.store).computeIfAbsent(eq("key"), getAnyFunction());
    ordered.verify(this.cacheWriter).write(eq("key"), eq("value"));
    ordered.verify(this.spiedResilienceStrategy)
        .putIfAbsentFailure(eq("key"), eq("value"), any(CacheAccessException.class), eq(true));
    assertThat(realCache.getEntries().get("key"), equalTo("value"));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.PutIfAbsentOutcome.FAILURE));    // TODO: Confirm correctness
  }

  /**
   * Tests the effect of a {@link Ehcache#putIfAbsent(Object, Object)} for
   * <ul>
   *   <li>key present in {@code Store}</li>
   *   <li>{@code Store.computeIfAbsent} throws</li>
   *   <li>key not present via {@code CacheWriter}</li>
   * </ul>
   */
  @Test
  public void testPutIfAbsentHasStoreEntryCacheAccessExceptionNoCacheWriterEntry() throws Exception {
    final MockStore realStore = new MockStore(Collections.singletonMap("key", "oldValue"));
    this.store = spy(realStore);
    doThrow(new CacheAccessException("")).when(this.store).computeIfAbsent(eq("key"), getAnyFunction());

    final MockCacheWriter realCache = new MockCacheWriter(Collections.<String, String>emptyMap());
    this.cacheWriter = spy(realCache);
    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheWriter);

    final InOrder ordered = inOrder(this.cacheWriter, this.spiedResilienceStrategy);

    assertThat(ehcache.putIfAbsent("key", "value"), is(nullValue()));
    verify(this.store).computeIfAbsent(eq("key"), getAnyFunction());
    ordered.verify(this.cacheWriter).write(eq("key"), eq("value"));
    ordered.verify(this.spiedResilienceStrategy)
        .putIfAbsentFailure(eq("key"), eq("value"), any(CacheAccessException.class), eq(true));
    assertThat(realCache.getEntries().get("key"), equalTo("value"));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.PutIfAbsentOutcome.FAILURE));    // TODO: Confirm correctness
  }

  /**
   * Tests the effect of a {@link Ehcache#putIfAbsent(Object, Object)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   *   <li>key present via {@code CacheWriter}</li>
   * </ul>
   */
  @Test
  public void testPutIfAbsentNoStoreEntryHasCacheWriterEntry() throws Exception {
    final MockStore realStore = new MockStore(Collections.<String, String>emptyMap());
    this.store = spy(realStore);

    final MockCacheWriter realCache = new MockCacheWriter(Collections.singletonMap("key", "oldValue"));
    final Ehcache<String, String> ehcache = this.getEhcache(realCache);

    assertThat(ehcache.putIfAbsent("key", "value"), is(nullValue()));   // TODO: Confirm correctness
    verify(this.store).computeIfAbsent(eq("key"), getAnyFunction());
    verifyZeroInteractions(this.spiedResilienceStrategy);
    assertThat(realStore.getMap().get("key"), equalTo("value"));
    assertThat(realCache.getEntries().get("key"), equalTo("value"));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.PutIfAbsentOutcome.PUT));
  }

  /**
   * Tests the effect of a {@link Ehcache#putIfAbsent(Object, Object)} for
   * <ul>
   *   <li>key present in {@code Store}</li>
   *   <li>key present via {@code CacheWriter}</li>
   * </ul>
   */
  @Test
  public void testPutIfAbsentHasStoreEntryHasCacheWriterEntry() throws Exception {
    final MockStore realStore = new MockStore(Collections.singletonMap("key", "oldValue"));
    this.store = spy(realStore);

    final MockCacheWriter realCache = new MockCacheWriter(Collections.singletonMap("key", "oldValue"));
    final Ehcache<String, String> ehcache = this.getEhcache(realCache);

    assertThat(ehcache.putIfAbsent("key", "value"), is(equalTo("oldValue")));
    verify(this.store).computeIfAbsent(eq("key"), getAnyFunction());
    verifyZeroInteractions(this.spiedResilienceStrategy);
    assertThat(realStore.getMap().get("key"), equalTo("oldValue"));
    assertThat(realCache.getEntries().get("key"), equalTo("oldValue"));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.PutIfAbsentOutcome.HIT));
  }

  /**
   * Tests the effect of a {@link Ehcache#putIfAbsent(Object, Object)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   *   <li>{@code Store.computeIfAbsent} throws</li>
   *   <li>key present via {@code CacheWriter}</li>
   * </ul>
   */
  @Test
  public void testPutIfAbsentNoStoreEntryCacheAccessExceptionHasCacheWriterEntry() throws Exception {
    final MockStore realStore = new MockStore(Collections.<String, String>emptyMap());
    this.store = spy(realStore);
    doThrow(new CacheAccessException("")).when(this.store).computeIfAbsent(eq("key"), getAnyFunction());

    final MockCacheWriter realCache = new MockCacheWriter(Collections.singletonMap("key", "oldValue"));
    this.cacheWriter = spy(realCache);
    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheWriter);

    final InOrder ordered = inOrder(this.cacheWriter, this.spiedResilienceStrategy);

    assertThat(ehcache.putIfAbsent("key", "value"), is(nullValue()));
    verify(this.store).computeIfAbsent(eq("key"), getAnyFunction());
    ordered.verify(this.cacheWriter).write(eq("key"), eq("value"));
    ordered.verify(this.spiedResilienceStrategy)
        .putIfAbsentFailure(eq("key"), eq("value"), any(CacheAccessException.class), eq(true));
    assertThat(realCache.getEntries().get("key"), equalTo("value"));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.PutIfAbsentOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link Ehcache#putIfAbsent(Object, Object)} for
   * <ul>
   *   <li>key present in {@code Store}</li>
   *   <li>{@code Store.computeIfPresent} throws</li>
   *   <li>key present via {@code CacheWriter}</li>
   * </ul>
   */
  @Test
  public void testPutIfAbsentHasStoreEntryCacheAccessExceptionHasCacheWriterEntry() throws Exception {
    final MockStore realStore = new MockStore(Collections.singletonMap("key", "oldValue"));
    this.store = spy(realStore);
    doThrow(new CacheAccessException("")).when(this.store).computeIfAbsent(eq("key"), getAnyFunction());

    final MockCacheWriter realCache = new MockCacheWriter(Collections.singletonMap("key", "oldValue"));
    this.cacheWriter = spy(realCache);
    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheWriter);

    final InOrder ordered = inOrder(this.cacheWriter, this.spiedResilienceStrategy);

    assertThat(ehcache.putIfAbsent("key", "value"), is(nullValue()));   // TODO: Confirm correctness
    verify(this.store).computeIfAbsent(eq("key"), getAnyFunction());
    ordered.verify(this.cacheWriter).write(eq("key"), eq("value"));
    ordered.verify(this.spiedResilienceStrategy)
        .putIfAbsentFailure(eq("key"), eq("value"), any(CacheAccessException.class), eq(true));
    assertThat(realCache.getEntries().get("key"), equalTo("value"));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.PutIfAbsentOutcome.FAILURE));    // TODO: Confirm correctness
  }

  /**
   * Tests the effect of a {@link Ehcache#putIfAbsent(Object, Object)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   *   <li>{@code CacheLoader.write} throws</li>
   * </ul>
   */
  @Test
  public void testPutIfAbsentNoStoreEntryCacheWriterException() throws Exception {
    final MockStore realStore = new MockStore(Collections.<String, String>emptyMap());
    this.store = spy(realStore);

    final MockCacheWriter realCache = new MockCacheWriter(Collections.singletonMap("key", "oldValue"));
    this.cacheWriter = spy(realCache);
    doThrow(new Exception()).when(this.cacheWriter).write("key", "value");
    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheWriter);

    try {
      ehcache.putIfAbsent("key", "value");
      fail();
    } catch (CacheWriterException e) {
      // Expected
    }
    verify(this.store).computeIfAbsent(eq("key"), getAnyFunction());
    verifyZeroInteractions(this.spiedResilienceStrategy);
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.PutIfAbsentOutcome.class));
  }

  /**
   * Tests the effect of a {@link Ehcache#putIfAbsent(Object, Object)} for
   * <ul>
   *   <li>key present in {@code Store}</li>
   *   <li>{@code CacheLoader.write} throws</li>
   * </ul>
   */
  @Test
  public void testPutIfAbsentHasStoreEntryCacheWriterException() throws Exception {
    final MockStore realStore = new MockStore(Collections.singletonMap("key", "oldValue"));
    this.store = spy(realStore);

    final MockCacheWriter realCache = new MockCacheWriter(Collections.singletonMap("key", "oldValue"));
    this.cacheWriter = spy(realCache);
    doThrow(new Exception()).when(this.cacheWriter).write("key", "value");
    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheWriter);

    assertThat(ehcache.putIfAbsent("key", "value"), is(equalTo("oldValue")));
    verify(this.store).computeIfAbsent(eq("key"), getAnyFunction());
    verifyZeroInteractions(this.spiedResilienceStrategy);
    assertThat(realStore.getMap().get("key"), equalTo("oldValue"));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.PutIfAbsentOutcome.HIT));
  }

  /**
   * Tests the effect of a {@link Ehcache#putIfAbsent(Object, Object)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   *   <li>{@code Store.computeIfPresent} throws</li>
   *   <li>{@code CacheLoader.write} throws</li>
   * </ul>
   */
  @Test
  public void testPutIfAbsentNoStoreEntryCacheAccessExceptionCacheWriterException() throws Exception {
    final MockStore realStore = new MockStore(Collections.<String, String>emptyMap());
    this.store = spy(realStore);
    doThrow(new CacheAccessException("")).when(this.store).computeIfAbsent(eq("key"), getAnyFunction());

    final MockCacheWriter realCache = new MockCacheWriter(Collections.singletonMap("key", "oldValue"));
    this.cacheWriter = spy(realCache);
    doThrow(new Exception()).when(this.cacheWriter).write("key", "value");
    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheWriter);

    final InOrder ordered = inOrder(this.cacheWriter, this.spiedResilienceStrategy);

    try {
      ehcache.putIfAbsent("key", "value");
      fail();
    } catch (CacheWriterException e) {
      // Expected
    }
    verify(this.store).computeIfAbsent(eq("key"), getAnyFunction());
    ordered.verify(this.cacheWriter).write(eq("key"), eq("value"));
    ordered.verify(this.spiedResilienceStrategy)
        .putIfAbsentFailure(eq("key"), eq("value"), any(CacheAccessException.class), any(CacheWriterException.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.PutIfAbsentOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link Ehcache#putIfAbsent(Object, Object)} for
   * <ul>
   *   <li>key present in {@code Store}</li>
   *   <li>{@code Store.computeIfPresent} throws</li>
   *   <li>{@code CacheLoader.write} throws</li>
   * </ul>
   */
  @Test
  public void testPutIfAbsentHasStoreEntryCacheAccessExceptionCacheWriterException() throws Exception {
    final MockStore realStore = new MockStore(Collections.singletonMap("key", "oldValue"));
    this.store = spy(realStore);
    doThrow(new CacheAccessException("")).when(this.store).computeIfAbsent(eq("key"), getAnyFunction());

    final MockCacheWriter realCache = new MockCacheWriter(Collections.singletonMap("key", "oldValue"));
    this.cacheWriter = spy(realCache);
    doThrow(new Exception()).when(this.cacheWriter).write("key", "value");
    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheWriter);

    final InOrder ordered = inOrder(this.cacheWriter, this.spiedResilienceStrategy);

    try {
      ehcache.putIfAbsent("key", "value");
      fail();
    } catch (CacheWriterException e) {
      // Expected
    }
    verify(this.store).computeIfAbsent(eq("key"), getAnyFunction());
    ordered.verify(this.cacheWriter).write(eq("key"), eq("value"));
    ordered.verify(this.spiedResilienceStrategy)
        .putIfAbsentFailure(eq("key"), eq("value"), any(CacheAccessException.class), any(CacheWriterException.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.PutIfAbsentOutcome.FAILURE));
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
