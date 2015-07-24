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
import org.ehcache.function.BiFunction;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.ehcache.statistics.CacheOperationOutcomes;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.EnumSet;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertEquals;
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
 * Provides testing of basic COMPUTEIFABSENT(key, function) operations on an {@code Ehcache}.
 *
 * @author Ludovic Orban
 */
public class EhcacheBasicComputeIfAbsentTest extends EhcacheBasicCrudBase {

  @Mock
  protected CacheLoaderWriter<String, String> cacheLoaderWriter;

  @Test
  public void testComputeIfAbsentNullNull() {
    final Ehcache<String, String> ehcache = this.getEhcache(null);

    try {
      ehcache.computeIfAbsent(null, null);
      fail();
    } catch (NullPointerException e) {
      // expected
    }
  }

  @Test
  public void testComputeIfAbsentKeyNull() {
    final Ehcache<String, String> ehcache = this.getEhcache(null);

    try {
      ehcache.computeIfAbsent("key", null);
      fail();
    } catch (NullPointerException e) {
      // expected
    }
  }

  @Test
  public void testComputeIfAbsentNullValue() {
    final Ehcache<String, String> ehcache = this.getEhcache(null);

    try {
      ehcache.computeIfAbsent(null, new BiFunction<String, String, String>() {
        @Override
        public String apply(String s, String s2) {
          return null;
        }
      });
      fail();
    } catch (NullPointerException e) {
      // expected
    }
  }

  /**
   * Tests the effect of a {@link Ehcache#computeIfAbsent(Object, BiFunction)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   *   <li>function returning a value</li>
   *   <li>no {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testComputeIfAbsentNoStoreEntryNoCacheLoaderWriterReturnValue() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.<String, String>emptyMap());
    this.store = spy(fakeStore);

    final Ehcache<String, String> ehcache = this.getEhcache(null);

    assertEquals("value", ehcache.computeIfAbsent("key", new BiFunction<String, String, String>() {
      @Override
      public String apply(String k, String v) {
        assertEquals("key", k);
        assertNull(v);
        return "value";
      }
    }));
    verify(this.store).compute(eq("key"), getAnyBiFunction());
    verifyZeroInteractions(this.spiedResilienceStrategy);
    assertThat(fakeStore.getEntryMap().containsKey("key"), is(true));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ComputeOutcome.NOT_PRESENT_ADDED));
  }

  /**
   * Tests the effect of a {@link Ehcache#computeIfAbsent(Object, BiFunction)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   *   <li>function returning null</li>
   *   <li>no {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testComputeIfAbsentNoStoreEntryNoCacheLoaderWriterReturnNull() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.<String, String>emptyMap());
    this.store = spy(fakeStore);

    final Ehcache<String, String> ehcache = this.getEhcache(null);

    assertNull(ehcache.computeIfAbsent("key", new BiFunction<String, String, String>() {
      @Override
      public String apply(String k, String v) {
        assertEquals("key", k);
        assertNull(v);
        return null;
      }
    }));
    verify(this.store).compute(eq("key"), getAnyBiFunction());
    verifyZeroInteractions(this.spiedResilienceStrategy);
    assertThat(fakeStore.getEntryMap().containsKey("key"), is(false));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ComputeOutcome.NOT_PRESENT_NOOP));
  }

  /**
   * Tests the effect of a {@link Ehcache#computeIfAbsent(Object, BiFunction)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   *   <li>function returning a value</li>
   *   <li>key not present via {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testComputeIfAbsentNoStoreEntryNoCacheLoaderWriterEntryReturnNull() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.<String, String>emptyMap());
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(Collections.<String, String>emptyMap());
    final Ehcache<String, String> ehcache = this.getEhcache(fakeLoaderWriter);

    assertEquals("value", ehcache.computeIfAbsent("key", new BiFunction<String, String, String>() {
      @Override
      public String apply(String k, String v) {
        assertEquals("key", k);
        assertNull(v);
        return "value";
      }
    }));
    verify(this.store).compute(eq("key"), getAnyBiFunction());
    verifyZeroInteractions(this.spiedResilienceStrategy);
    assertThat(fakeStore.getEntryMap().containsKey("key"), is(true));
    assertThat(fakeLoaderWriter.getEntryMap().containsKey("key"), is(true));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ComputeOutcome.NOT_PRESENT_ADDED));
  }

  /**
   * Tests the effect of a {@link Ehcache#computeIfAbsent(Object, BiFunction)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   *   <li>function returning null</li>
   *   <li>key not present via {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testComputeIfAbsentNoStoreEntryNoCacheLoaderWriterEntryReturnValue() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.<String, String>emptyMap());
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(Collections.<String, String>emptyMap());
    final Ehcache<String, String> ehcache = this.getEhcache(fakeLoaderWriter);

    assertNull(ehcache.computeIfAbsent("key", new BiFunction<String, String, String>() {
      @Override
      public String apply(String k, String v) {
        assertEquals("key", k);
        assertNull(v);
        return null;
      }
    }));
    verify(this.store).compute(eq("key"), getAnyBiFunction());
    verifyZeroInteractions(this.spiedResilienceStrategy);
    assertThat(fakeStore.getEntryMap().containsKey("key"), is(false));
    assertThat(fakeLoaderWriter.getEntryMap().containsKey("key"), is(false));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ComputeOutcome.NOT_PRESENT_NOOP));
  }

  /**
   * Tests the effect of a {@link Ehcache#computeIfAbsent(Object, BiFunction)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   *   <li>key present via {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testComputeIfAbsentNoStoreEntryHasCacheLoaderWriterEntry() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.<String, String>emptyMap());
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(Collections.singletonMap("key", "oldValue"));
    final Ehcache<String, String> ehcache = this.getEhcache(fakeLoaderWriter);

    assertThat(ehcache.computeIfAbsent("key", new BiFunction<String, String, String>() {
      @Override
      public String apply(String k, String v) {
        throw new AssertionError("should not be called");
      }
    }), is(nullValue()));
    verify(this.store).compute(eq("key"), getAnyBiFunction());
    verifyZeroInteractions(this.spiedResilienceStrategy);
    assertThat(fakeStore.getEntryMap().containsKey("key"), is(true));
    assertThat(fakeLoaderWriter.getEntryMap().get("key"), is(equalTo("oldValue")));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ComputeOutcome.PRESENT_NOOP));
  }

  /**
   * Tests the effect of a {@link Ehcache#computeIfAbsent(Object, BiFunction)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   *   <li>{@code CacheLoaderWriter.write} throws</li>
   * </ul>
   */
  @Test
  public void testComputeIfAbsentNoStoreEntryCacheWritingException() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.<String, String>emptyMap());
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(Collections.singletonMap("key", "oldValue"));
    this.cacheLoaderWriter = spy(fakeLoaderWriter);
    doThrow(new Exception()).when(this.cacheLoaderWriter).write("key", "value");
    final Ehcache<String, String> ehcache = this.getEhcache(cacheLoaderWriter);

    ehcache.computeIfAbsent("key", new BiFunction<String, String, String>() {
      @Override
      public String apply(String k, String v) {
        throw new AssertionError("should not be called");
      }
    });
    verify(this.store).compute(eq("key"), getAnyBiFunction());
    verifyZeroInteractions(this.spiedResilienceStrategy);
    assertThat(fakeStore.getEntryMap().containsKey("key"), is(true));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ComputeOutcome.PRESENT_NOOP));
  }

  /**
   * Tests the effect of a {@link Ehcache#computeIfAbsent(Object, BiFunction)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   *   <li>{@code Store.compute} throws</li>
   *   <li>no {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testComputeIfAbsentNoStoreEntryCacheAccessExceptionNoCacheLoaderWriter() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.<String, String>emptyMap());
    this.store = spy(fakeStore);
    doThrow(new CacheAccessException("")).when(this.store).compute(eq("key"), getAnyBiFunction());

    final Ehcache<String, String> ehcache = this.getEhcache(null);

    ehcache.computeIfAbsent("key", new BiFunction<String, String, String>() {
      @Override
      public String apply(String k, String v) {
        throw new AssertionError("should not be called");
      }
    });
    verify(this.store).compute(eq("key"), getAnyBiFunction());
    verify(this.spiedResilienceStrategy).computeIfAbsentFailure(eq("key"), any(CacheAccessException.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ComputeOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link Ehcache#computeIfAbsent(Object, BiFunction)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   *   <li>function returning a value</li>
   *   <li>{@code Store.compute} throws</li>
   *   <li>key not present via {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testComputeIfAbsentNoStoreEntryCacheAccessExceptionNoCacheLoaderWriterEntryReturnValue() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.<String, String>emptyMap());
    this.store = spy(fakeStore);
    doThrow(new CacheAccessException("")).when(this.store).compute(eq("key"), getAnyBiFunction());

    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(Collections.<String, String>emptyMap());
    this.cacheLoaderWriter = spy(fakeLoaderWriter);
    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    final InOrder ordered = inOrder(this.cacheLoaderWriter, this.spiedResilienceStrategy);

    ehcache.computeIfAbsent("key", new BiFunction<String, String, String>() {
      @Override
      public String apply(String k, String v) {
        assertEquals("key", k);
        assertNull(v);
        return "value";
      }
    });
    verify(this.store).compute(eq("key"), getAnyBiFunction());
    ordered.verify(this.cacheLoaderWriter).load("key");
    ordered.verify(this.spiedResilienceStrategy).computeIfAbsentFailure(eq("key"), any(CacheAccessException.class));
    assertThat(fakeLoaderWriter.getEntryMap().get("key"), is(equalTo("value")));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ComputeOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link Ehcache#computeIfAbsent(Object, BiFunction)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   *   <li>function returning null</li>
   *   <li>{@code Store.compute} throws</li>
   *   <li>key not present via {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testComputeIfAbsentNoStoreEntryCacheAccessExceptionNoCacheLoaderWriterEntryReturnNull() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.<String, String>emptyMap());
    this.store = spy(fakeStore);
    doThrow(new CacheAccessException("")).when(this.store).compute(eq("key"), getAnyBiFunction());

    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(Collections.<String, String>emptyMap());
    this.cacheLoaderWriter = spy(fakeLoaderWriter);
    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    final InOrder ordered = inOrder(this.cacheLoaderWriter, this.spiedResilienceStrategy);

    ehcache.computeIfAbsent("key", new BiFunction<String, String, String>() {
      @Override
      public String apply(String k, String v) {
        assertEquals("key", k);
        assertNull(v);
        return null;
      }
    });
    verify(this.store).compute(eq("key"), getAnyBiFunction());
    ordered.verify(this.cacheLoaderWriter).load("key");
    ordered.verify(this.spiedResilienceStrategy).computeIfAbsentFailure(eq("key"), any(CacheAccessException.class));
    assertThat(fakeLoaderWriter.getEntryMap().containsKey("key"), is(false));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ComputeOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link Ehcache#computeIfAbsent(Object, BiFunction)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   *   <li>{@code Store.compute} throws</li>
   *   <li>key present via {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testComputeIfAbsentNoStoreEntryCacheAccessExceptionHasCacheLoaderWriterEntry() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.<String, String>emptyMap());
    this.store = spy(fakeStore);
    doThrow(new CacheAccessException("")).when(this.store).compute(eq("key"), getAnyBiFunction());

    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(Collections.singletonMap("key", "oldValue"));
    this.cacheLoaderWriter = spy(fakeLoaderWriter);
    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    ehcache.computeIfAbsent("key", new BiFunction<String, String, String>() {
      @Override
      public String apply(String k, String v) {
        throw new AssertionError("should not be called");
      }
    });
    verify(this.store).compute(eq("key"), getAnyBiFunction());
    verify(this.spiedResilienceStrategy).computeIfAbsentFailure(eq("key"), any(CacheAccessException.class));
    assertThat(fakeLoaderWriter.getEntryMap().get("key"), is(equalTo("oldValue")));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ComputeOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link Ehcache#computeIfAbsent(Object, BiFunction)} for
   * <ul>
   *   <li>key present in {@code Store}</li>
   *   <li>no {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testComputeIfAbsentHasStoreEntryNoCacheLoaderWriter() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "oldValue"));
    this.store = spy(fakeStore);

    final Ehcache<String, String> ehcache = this.getEhcache(null);

    assertThat(ehcache.computeIfAbsent("key", new BiFunction<String, String, String>() {
      @Override
      public String apply(String k, String v) {
        throw new AssertionError("should not be called");
      }
    }), is(nullValue()));
    verify(this.store).compute(eq("key"), getAnyBiFunction());
    verifyZeroInteractions(this.spiedResilienceStrategy);
    assertThat(fakeStore.getEntryMap().get("key"), is(equalTo("oldValue")));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ComputeOutcome.PRESENT_NOOP));
  }

  /**
   * Tests the effect of a {@link Ehcache#computeIfAbsent(Object, BiFunction)} for
   * <ul>
   *   <li>key present in {@code Store}</li>
   *   <li>{@code Store.compute} throws</li>
   *   <li>no {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testComputeIfAbsentHasStoreEntryCacheAccessExceptionNoCacheLoaderWriter() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "oldValue"));
    this.store = spy(fakeStore);
    doThrow(new CacheAccessException("")).when(this.store).compute(eq("key"), getAnyBiFunction());

    final Ehcache<String, String> ehcache = this.getEhcache(null);

    ehcache.computeIfAbsent("key", new BiFunction<String, String, String>() {
      @Override
      public String apply(String s, String s2) {
        throw new AssertionError("should not be called");
      }
    });
    verify(this.store).compute(eq("key"), getAnyBiFunction());
    verify(this.spiedResilienceStrategy).computeIfAbsentFailure(eq("key"), any(CacheAccessException.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ComputeOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link Ehcache#computeIfAbsent(Object, BiFunction)} for
   * <ul>
   *   <li>key present in {@code Store}</li>
   *   <li>function returning a value</li>
   *   <li>{@code Store.compute} throws</li>
   *   <li>key not present via {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testComputeIfAbsentHasStoreEntryCacheAccessExceptionNoCacheLoaderWriterEntryReturnValue() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "oldValue"));
    this.store = spy(fakeStore);
    doThrow(new CacheAccessException("")).when(this.store).compute(eq("key"), getAnyBiFunction());

    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(Collections.<String, String>emptyMap());
    this.cacheLoaderWriter = spy(fakeLoaderWriter);
    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    final InOrder ordered = inOrder(this.cacheLoaderWriter, this.spiedResilienceStrategy);

    ehcache.computeIfAbsent("key", new BiFunction<String, String, String>() {
      @Override
      public String apply(String k, String v) {
        assertEquals("key", k);
        assertNull(v);
        return "value";
      }
    });
    verify(this.store).compute(eq("key"), getAnyBiFunction());
    verify(this.store, times(1)).remove("key");
    ordered.verify(this.cacheLoaderWriter).load(eq("key"));
    ordered.verify(this.spiedResilienceStrategy).computeIfAbsentFailure(eq("key"), any(CacheAccessException.class));
    assertThat(fakeLoaderWriter.getEntryMap().get("key"), equalTo("value"));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ComputeOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link Ehcache#computeIfAbsent(Object, BiFunction)} for
   * <ul>
   *   <li>key present in {@code Store}</li>
   *   <li>function returning null</li>
   *   <li>{@code Store.compute} throws</li>
   *   <li>key not present via {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testComputeIfAbsentHasStoreEntryCacheAccessExceptionNoCacheLoaderWriterEntryReturnNull() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "oldValue"));
    this.store = spy(fakeStore);
    doThrow(new CacheAccessException("")).when(this.store).compute(eq("key"), getAnyBiFunction());

    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(Collections.<String, String>emptyMap());
    this.cacheLoaderWriter = spy(fakeLoaderWriter);
    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    final InOrder ordered = inOrder(this.cacheLoaderWriter, this.spiedResilienceStrategy);

    ehcache.computeIfAbsent("key", new BiFunction<String, String, String>() {
      @Override
      public String apply(String k, String v) {
        assertEquals("key", k);
        assertNull(v);
        return null;
      }
    });
    verify(this.store).compute(eq("key"), getAnyBiFunction());
    verify(this.store, times(1)).remove("key");
    ordered.verify(this.cacheLoaderWriter).load(eq("key"));
    ordered.verify(this.spiedResilienceStrategy).computeIfAbsentFailure(eq("key"), any(CacheAccessException.class));
    assertThat(fakeLoaderWriter.getEntryMap().containsKey("key"), is(false));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ComputeOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link Ehcache#computeIfAbsent(Object, BiFunction)} for
   * <ul>
   *   <li>key present in {@code Store}</li>
   *   <li>{@code Store.compute} throws</li>
   *   <li>key present via {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testComputeIfAbsentHasStoreEntryCacheAccessExceptionHasCacheLoaderWriterEntry() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "oldValue"));
    this.store = spy(fakeStore);
    doThrow(new CacheAccessException("")).when(this.store).compute(eq("key"), getAnyBiFunction());

    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(Collections.singletonMap("key", "oldValue"));
    this.cacheLoaderWriter = spy(fakeLoaderWriter);
    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    ehcache.computeIfAbsent("key", new BiFunction<String, String, String>() {
      @Override
      public String apply(String k, String v) {
        throw new AssertionError("should not be called");
      }
    });
    verify(this.store).compute(eq("key"), getAnyBiFunction());
    verify(this.spiedResilienceStrategy).computeIfAbsentFailure(eq("key"), any(CacheAccessException.class));
    assertThat(fakeLoaderWriter.getEntryMap().get("key"), is(equalTo("oldValue")));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ComputeOutcome.FAILURE));
  }

  /**
   * Gets an initialized {@link Ehcache Ehcache} instance using the
   * {@link CacheLoaderWriter} provided.
   *
   * @param cacheLoaderWriter the {@code CacheLoaderWriter} to use; may be {@code null}
   *
   * @return a new {@code Ehcache} instance
   */
  private Ehcache<String, String> getEhcache(final CacheLoaderWriter<String, String> cacheLoaderWriter) {
    RuntimeConfiguration<String, String> runtimeConfiguration = new RuntimeConfiguration<String, String>(CACHE_CONFIGURATION, null);
    final Ehcache<String, String> ehcache
        = new Ehcache<String, String>(runtimeConfiguration, this.store, cacheLoaderWriter, LoggerFactory.getLogger(Ehcache.class + "-" + "EhcacheBasicReplaceTest"));
    ehcache.init();
    assertThat("cache not initialized", ehcache.getStatus(), is(Status.AVAILABLE));
    this.spiedResilienceStrategy = this.setResilienceStrategySpy(ehcache);
    return ehcache;
  }
}
