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
import org.ehcache.exceptions.CacheWritingException;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.ehcache.statistics.CacheOperationOutcomes;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.slf4j.LoggerFactory;

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
  protected CacheLoaderWriter<String, String> cacheLoaderWriter;

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
   *   <li>no {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testPutIfAbsentNoStoreEntryNoCacheLoaderWriter() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.<String, String>emptyMap());
    this.store = spy(fakeStore);

    final Ehcache<String, String> ehcache = this.getEhcache(null);

    assertThat(ehcache.putIfAbsent("key", "value"), is(nullValue()));
    verify(this.store).computeIfAbsent(eq("key"), getAnyFunction());
    verifyZeroInteractions(this.spiedResilienceStrategy);
    assertThat(fakeStore.getEntryMap().get("key"), equalTo("value"));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.PutIfAbsentOutcome.PUT));
  }

  /**
   * Tests the effect of a {@link Ehcache#putIfAbsent(Object, Object)} for
   * <ul>
   *   <li>key present in {@code Store}</li>
   *   <li>no {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testPutIfAbsentHasStoreEntryNoCacheLoaderWriter() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "oldValue"));
    this.store = spy(fakeStore);

    final Ehcache<String, String> ehcache = this.getEhcache(null);

    assertThat(ehcache.putIfAbsent("key", "value"), is(equalTo("oldValue")));
    verify(this.store).computeIfAbsent(eq("key"), getAnyFunction());
    verifyZeroInteractions(this.spiedResilienceStrategy);
    assertThat(fakeStore.getEntryMap().get("key"), equalTo("oldValue"));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.PutIfAbsentOutcome.HIT));
  }

  /**
   * Tests the effect of a {@link Ehcache#putIfAbsent(Object, Object)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   *   <li>{@code Store.computeIfAbsent} throws</li>
   *   <li>no {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testPutIfAbsentNoStoreEntryCacheAccessExceptionNoCacheLoaderWriter() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.<String, String>emptyMap());
    this.store = spy(fakeStore);
    doThrow(new CacheAccessException("")).when(this.store).computeIfAbsent(eq("key"), getAnyFunction());

    final Ehcache<String, String> ehcache = this.getEhcache(null);

    ehcache.putIfAbsent("key", "value");
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
   *   <li>no {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testPutIfAbsentHasStoreEntryCacheAccessExceptionNoCacheLoaderWriter() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "oldValue"));
    this.store = spy(fakeStore);
    doThrow(new CacheAccessException("")).when(this.store).computeIfAbsent(eq("key"), getAnyFunction());

    final Ehcache<String, String> ehcache = this.getEhcache(null);

    ehcache.putIfAbsent("key", "value");
    verify(this.store).computeIfAbsent(eq("key"), getAnyFunction());
    verify(this.spiedResilienceStrategy)
        .putIfAbsentFailure(eq("key"), eq("value"), any(CacheAccessException.class), eq(false));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.PutIfAbsentOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link Ehcache#putIfAbsent(Object, Object)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   *   <li>key not present via {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testPutIfAbsentNoStoreEntryNoCacheLoaderWriterEntry() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.<String, String>emptyMap());
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(Collections.<String, String>emptyMap());
    final Ehcache<String, String> ehcache = this.getEhcache(fakeLoaderWriter);

    assertThat(ehcache.putIfAbsent("key", "value"), is(nullValue()));
    verify(this.store).computeIfAbsent(eq("key"), getAnyFunction());
    verifyZeroInteractions(this.spiedResilienceStrategy);
    assertThat(fakeStore.getEntryMap().get("key"), equalTo("value"));
    assertThat(fakeLoaderWriter.getEntryMap().get("key"), equalTo("value"));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.PutIfAbsentOutcome.PUT));
  }

  /**
   * Tests the effect of a {@link Ehcache#putIfAbsent(Object, Object)} for
   * <ul>
   *   <li>key present in {@code Store}</li>
   *   <li>key not present via {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testPutIfAbsentHasStoreEntryNoCacheLoaderWriterEntry() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "oldValue"));
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(Collections.<String, String>emptyMap());
    final Ehcache<String, String> ehcache = this.getEhcache(fakeLoaderWriter);

    assertThat(ehcache.putIfAbsent("key", "value"), is(equalTo("oldValue")));
    verify(this.store).computeIfAbsent(eq("key"), getAnyFunction());
    verifyZeroInteractions(this.spiedResilienceStrategy);
    assertThat(fakeStore.getEntryMap().get("key"), equalTo("oldValue"));
    // Broken initial state: CacheLoaderWriter check omitted
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.PutIfAbsentOutcome.HIT));
  }

  /**
   * Tests the effect of a {@link Ehcache#putIfAbsent(Object, Object)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   *   <li>{@code Store.computeIfAbsent} throws</li>
   *   <li>key not present via {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testPutIfAbsentNoStoreEntryCacheAccessExceptionNoCacheLoaderWriterEntry() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.<String, String>emptyMap());
    this.store = spy(fakeStore);
    doThrow(new CacheAccessException("")).when(this.store).computeIfAbsent(eq("key"), getAnyFunction());

    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(Collections.<String, String>emptyMap());
    this.cacheLoaderWriter = spy(fakeLoaderWriter);
    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    final InOrder ordered = inOrder(this.cacheLoaderWriter, this.spiedResilienceStrategy);

    ehcache.putIfAbsent("key", "value");
    verify(this.store).computeIfAbsent(eq("key"), getAnyFunction());
    ordered.verify(this.cacheLoaderWriter).write(eq("key"), eq("value"));
    ordered.verify(this.spiedResilienceStrategy)
        .putIfAbsentFailure(eq("key"), eq("value"), any(CacheAccessException.class), eq(true));
    assertThat(fakeLoaderWriter.getEntryMap().get("key"), equalTo("value"));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.PutIfAbsentOutcome.FAILURE));    // TODO: Confirm correctness
  }

  /**
   * Tests the effect of a {@link Ehcache#putIfAbsent(Object, Object)} for
   * <ul>
   *   <li>key present in {@code Store}</li>
   *   <li>{@code Store.computeIfAbsent} throws</li>
   *   <li>key not present via {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testPutIfAbsentHasStoreEntryCacheAccessExceptionNoCacheLoaderWriterEntry() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "oldValue"));
    this.store = spy(fakeStore);
    doThrow(new CacheAccessException("")).when(this.store).computeIfAbsent(eq("key"), getAnyFunction());

    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(Collections.<String, String>emptyMap());
    this.cacheLoaderWriter = spy(fakeLoaderWriter);
    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    final InOrder ordered = inOrder(this.cacheLoaderWriter, this.spiedResilienceStrategy);

    ehcache.putIfAbsent("key", "value");
    verify(this.store).computeIfAbsent(eq("key"), getAnyFunction());
    ordered.verify(this.cacheLoaderWriter).write(eq("key"), eq("value"));
    ordered.verify(this.spiedResilienceStrategy)
        .putIfAbsentFailure(eq("key"), eq("value"), any(CacheAccessException.class), eq(true));
    // Broken initial state: CacheLoaderWriter check omitted
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.PutIfAbsentOutcome.FAILURE));    // TODO: Confirm correctness
  }

  /**
   * Tests the effect of a {@link Ehcache#putIfAbsent(Object, Object)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   *   <li>key present via {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testPutIfAbsentNoStoreEntryHasCacheLoaderWriterEntry() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.<String, String>emptyMap());
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(Collections.singletonMap("key", "oldValue"));
    final Ehcache<String, String> ehcache = this.getEhcache(fakeLoaderWriter);

    assertThat(ehcache.putIfAbsent("key", "value"), is("oldValue"));
    verify(this.store).computeIfAbsent(eq("key"), getAnyFunction());
    verifyZeroInteractions(this.spiedResilienceStrategy);
    assertThat(fakeStore.getEntryMap().get("key"), equalTo("oldValue"));
    assertThat(fakeLoaderWriter.getEntryMap().get("key"), equalTo("oldValue"));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.PutIfAbsentOutcome.HIT));
  }

  /**
   * Tests the effect of a {@link Ehcache#putIfAbsent(Object, Object)} for
   * <ul>
   *   <li>key present in {@code Store}</li>
   *   <li>key present via {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testPutIfAbsentHasStoreEntryHasCacheLoaderWriterEntry() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "oldValue"));
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(Collections.singletonMap("key", "oldValue"));
    final Ehcache<String, String> ehcache = this.getEhcache(fakeLoaderWriter);

    assertThat(ehcache.putIfAbsent("key", "value"), is(equalTo("oldValue")));
    verify(this.store).computeIfAbsent(eq("key"), getAnyFunction());
    verifyZeroInteractions(this.spiedResilienceStrategy);
    assertThat(fakeStore.getEntryMap().get("key"), equalTo("oldValue"));
    assertThat(fakeLoaderWriter.getEntryMap().get("key"), equalTo("oldValue"));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.PutIfAbsentOutcome.HIT));
  }

  /**
   * Tests the effect of a {@link Ehcache#putIfAbsent(Object, Object)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   *   <li>{@code Store.computeIfAbsent} throws</li>
   *   <li>key present via {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testPutIfAbsentNoStoreEntryCacheAccessExceptionHasCacheLoaderWriterEntry() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.<String, String>emptyMap());
    this.store = spy(fakeStore);
    doThrow(new CacheAccessException("")).when(this.store).computeIfAbsent(eq("key"), getAnyFunction());

    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(Collections.singletonMap("key", "oldValue"));
    this.cacheLoaderWriter = spy(fakeLoaderWriter);
    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    final InOrder ordered = inOrder(this.cacheLoaderWriter, this.spiedResilienceStrategy);

    ehcache.putIfAbsent("key", "value");
    verify(this.store).computeIfAbsent(eq("key"), getAnyFunction());
    ordered.verify(this.cacheLoaderWriter).load(eq("key"));
    ordered.verify(this.spiedResilienceStrategy)
        .putIfAbsentFailure(eq("key"), eq("value"), any(CacheAccessException.class), eq(false));
    assertThat(fakeLoaderWriter.getEntryMap().get("key"), equalTo("oldValue"));
    // Broken initial state: CacheLoaderWriter check omitted
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.PutIfAbsentOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link Ehcache#putIfAbsent(Object, Object)} for
   * <ul>
   *   <li>key present in {@code Store}</li>
   *   <li>{@code Store.computeIfPresent} throws</li>
   *   <li>key present via {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testPutIfAbsentHasStoreEntryCacheAccessExceptionHasCacheLoaderWriterEntry() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "oldValue"));
    this.store = spy(fakeStore);
    doThrow(new CacheAccessException("")).when(this.store).computeIfAbsent(eq("key"), getAnyFunction());

    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(Collections.singletonMap("key", "oldValue"));
    this.cacheLoaderWriter = spy(fakeLoaderWriter);
    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    final InOrder ordered = inOrder(this.cacheLoaderWriter, this.spiedResilienceStrategy);

    ehcache.putIfAbsent("key", "value");
    verify(this.store).computeIfAbsent(eq("key"), getAnyFunction());
    ordered.verify(this.cacheLoaderWriter).load(eq("key"));
    ordered.verify(this.spiedResilienceStrategy)
        .putIfAbsentFailure(eq("key"), eq("value"), any(CacheAccessException.class), eq(false));
    assertThat(fakeLoaderWriter.getEntryMap().get("key"), equalTo("oldValue"));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.PutIfAbsentOutcome.FAILURE));    // TODO: Confirm correctness
  }

  /**
   * Tests the effect of a {@link Ehcache#putIfAbsent(Object, Object)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   *   <li>{@code CacheLoaderWriter.write} throws</li>
   * </ul>
   */
  @Test
  public void testPutIfAbsentNoStoreEntryCacheLoaderWriterException() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.<String, String>emptyMap());
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(Collections.<String, String>emptyMap());
    this.cacheLoaderWriter = spy(fakeLoaderWriter);
    doThrow(new Exception()).when(this.cacheLoaderWriter).write("key", "value");
    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    try {
      ehcache.putIfAbsent("key", "value");
      fail();
    } catch (CacheWritingException e) {
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
   *   <li>{@code CacheLoaderWriter.write} throws</li>
   * </ul>
   */
  @Test
  public void testPutIfAbsentHasStoreEntryCacheLoaderWriterException() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "oldValue"));
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(Collections.singletonMap("key", "oldValue"));
    this.cacheLoaderWriter = spy(fakeLoaderWriter);
    doThrow(new Exception()).when(this.cacheLoaderWriter).write("key", "value");
    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    assertThat(ehcache.putIfAbsent("key", "value"), is(equalTo("oldValue")));
    verify(this.store).computeIfAbsent(eq("key"), getAnyFunction());
    verifyZeroInteractions(this.spiedResilienceStrategy);
    assertThat(fakeStore.getEntryMap().get("key"), equalTo("oldValue"));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.PutIfAbsentOutcome.HIT));
  }

  /**
   * Tests the effect of a {@link Ehcache#putIfAbsent(Object, Object)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   *   <li>{@code Store.computeIfPresent} throws</li>
   *   <li>{@code CacheLoaderWriter.write} throws</li>
   * </ul>
   */
  @Test
  public void testPutIfAbsentNoStoreEntryCacheAccessExceptionCacheLoaderWriterException() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.<String, String>emptyMap());
    this.store = spy(fakeStore);
    doThrow(new CacheAccessException("")).when(this.store).computeIfAbsent(eq("key"), getAnyFunction());

    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(Collections.<String, String>emptyMap());
    this.cacheLoaderWriter = spy(fakeLoaderWriter);
    doThrow(new Exception()).when(this.cacheLoaderWriter).write("key", "value");
    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    final InOrder ordered = inOrder(this.cacheLoaderWriter, this.spiedResilienceStrategy);

    try {
      ehcache.putIfAbsent("key", "value");
      fail();
    } catch (CacheWritingException e) {
      // Expected
    }
    verify(this.store).computeIfAbsent(eq("key"), getAnyFunction());
    ordered.verify(this.cacheLoaderWriter).write(eq("key"), eq("value"));
    ordered.verify(this.spiedResilienceStrategy)
        .putIfAbsentFailure(eq("key"), eq("value"), any(CacheAccessException.class), any(CacheWritingException.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.PutIfAbsentOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link Ehcache#putIfAbsent(Object, Object)} for
   * <ul>
   *   <li>key present in {@code Store}</li>
   *   <li>{@code Store.computeIfPresent} throws</li>
   *   <li>{@code CacheLoaderWriter.write} throws</li>
   * </ul>
   */
  @Test
  public void testPutIfAbsentHasStoreEntryCacheAccessExceptionCacheLoaderWriterException() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "oldValue"));
    this.store = spy(fakeStore);
    doThrow(new CacheAccessException("")).when(this.store).computeIfAbsent(eq("key"), getAnyFunction());

    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(Collections.<String, String>emptyMap());
    this.cacheLoaderWriter = spy(fakeLoaderWriter);
    doThrow(new Exception()).when(this.cacheLoaderWriter).write("key", "value");
    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    final InOrder ordered = inOrder(this.cacheLoaderWriter, this.spiedResilienceStrategy);

    try {
      ehcache.putIfAbsent("key", "value");
      fail();
    } catch (CacheWritingException e) {
      // Expected
    }
    verify(this.store).computeIfAbsent(eq("key"), getAnyFunction());
    ordered.verify(this.cacheLoaderWriter).write(eq("key"), eq("value"));
    ordered.verify(this.spiedResilienceStrategy)
        .putIfAbsentFailure(eq("key"), eq("value"), any(CacheAccessException.class), any(CacheWritingException.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.PutIfAbsentOutcome.FAILURE));
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
    RuntimeConfiguration<String, String> runtimeConfiguration = new RuntimeConfiguration<String, String>(CACHE_CONFIGURATION, null);
    final Ehcache<String, String> ehcache = new Ehcache<String, String>(runtimeConfiguration, this.store, cacheLoaderWriter, LoggerFactory.getLogger(Ehcache.class + "-" + "EhcacheBasicPutIfAbsentTest"));
    ehcache.init();
    assertThat("cache not initialized", ehcache.getStatus(), is(Status.AVAILABLE));
    this.spiedResilienceStrategy = this.setResilienceStrategySpy(ehcache);
    return ehcache;
  }
}
