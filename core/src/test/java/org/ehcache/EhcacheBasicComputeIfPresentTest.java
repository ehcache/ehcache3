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
 * Provides testing of basic COMPUTEIFPRESENT(key, function) operations on an {@code Ehcache}.
 *
 * @author Ludovic Orban
 */
public class EhcacheBasicComputeIfPresentTest extends EhcacheBasicCrudBase {

  @Mock
  protected CacheLoaderWriter<String, String> cacheLoaderWriter;

  @Test
  public void testComputeIfPresentNullNull() {
    final Ehcache<String, String> ehcache = this.getEhcache(null);

    try {
      ehcache.computeIfPresent(null, null);
      fail();
    } catch (NullPointerException e) {
      // expected
    }
  }

  @Test
  public void testComputeIfPresentKeyNull() {
    final Ehcache<String, String> ehcache = this.getEhcache(null);

    try {
      ehcache.computeIfPresent("key", null);
      fail();
    } catch (NullPointerException e) {
      // expected
    }
  }

  @Test
  public void testComputeIfPresentNullValue() {
    final Ehcache<String, String> ehcache = this.getEhcache(null);

    try {
      ehcache.computeIfPresent(null, new BiFunction<String, String, String>() {
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
   * Tests the effect of a {@link Ehcache#computeIfPresent(Object, BiFunction)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   *   <li>no {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testComputeIfPresentNoStoreEntryNoCacheLoaderWriter() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.<String, String>emptyMap());
    this.store = spy(fakeStore);

    final Ehcache<String, String> ehcache = this.getEhcache(null);

    assertNull(ehcache.computeIfPresent("key", new BiFunction<String, String, String>() {
      @Override
      public String apply(String k, String v) {
        throw new AssertionError("should not be called");
      }
    }));
    verify(this.store).compute(eq("key"), getAnyBiFunction());
    verifyZeroInteractions(this.spiedResilienceStrategy);
    assertThat(fakeStore.getEntryMap().containsKey("key"), is(false));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ComputeOutcome.NOT_PRESENT_NOOP));
  }

  /**
   * Tests the effect of a {@link Ehcache#computeIfPresent(Object, BiFunction)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   *   <li>key not present via {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testComputeIfPresentNoStoreEntryNoCacheLoaderWriterEntry() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.<String, String>emptyMap());
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(Collections.<String, String>emptyMap());
    final Ehcache<String, String> ehcache = this.getEhcache(fakeLoaderWriter);

    assertNull(ehcache.computeIfPresent("key", new BiFunction<String, String, String>() {
      @Override
      public String apply(String k, String v) {
        throw new AssertionError("should not be called");
      }
    }));
    verify(this.store).compute(eq("key"), getAnyBiFunction());
    verifyZeroInteractions(this.spiedResilienceStrategy);
    assertThat(fakeStore.getEntryMap().containsKey("key"), is(false));
    assertThat(fakeLoaderWriter.getEntryMap().containsKey("key"), is(false));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ComputeOutcome.NOT_PRESENT_NOOP));
  }

  /**
   * Tests the effect of a {@link Ehcache#computeIfPresent(Object, BiFunction)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   *   <li>function returning a value</li>
   *   <li>key present via {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testComputeIfPresentNoStoreEntryHasCacheLoaderWriterEntryReturnValue() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.<String, String>emptyMap());
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(Collections.singletonMap("key", "oldValue"));
    final Ehcache<String, String> ehcache = this.getEhcache(fakeLoaderWriter);

    assertThat(ehcache.computeIfPresent("key", new BiFunction<String, String, String>() {
      @Override
      public String apply(String k, String v) {
        assertEquals("key", k);
        assertEquals("oldValue", v);
        return "value";
      }
    }), is("value"));
    verify(this.store).compute(eq("key"), getAnyBiFunction());
    verifyZeroInteractions(this.spiedResilienceStrategy);
    assertThat(fakeStore.getEntryMap().containsKey("key"), is(true));
    assertThat(fakeLoaderWriter.getEntryMap().get("key"), is(equalTo("value")));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ComputeOutcome.PRESENT_UPDATED));
  }

  /**
   * Tests the effect of a {@link Ehcache#computeIfPresent(Object, BiFunction)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   *   <li>function returning null</li>
   *   <li>key present via {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testComputeIfPresentNoStoreEntryHasCacheLoaderWriterEntryReturnNull() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.<String, String>emptyMap());
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(Collections.singletonMap("key", "oldValue"));
    final Ehcache<String, String> ehcache = this.getEhcache(fakeLoaderWriter);

    assertThat(ehcache.computeIfPresent("key", new BiFunction<String, String, String>() {
      @Override
      public String apply(String k, String v) {
        assertEquals("key", k);
        assertEquals("oldValue", v);
        return null;
      }
    }), is(nullValue()));
    verify(this.store).compute(eq("key"), getAnyBiFunction());
    verifyZeroInteractions(this.spiedResilienceStrategy);
    assertThat(fakeStore.getEntryMap().containsKey("key"), is(false));
    assertThat(fakeLoaderWriter.getEntryMap().containsKey("key"), is(false));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ComputeOutcome.PRESENT_REMOVED));
  }

  /**
   * Tests the effect of a {@link Ehcache#computeIfPresent(Object, BiFunction)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   *   <li>function returning a value</li>
   *   <li>{@code CacheLoaderWriter.write} throws</li>
   * </ul>
   */
  @Test
  public void testComputeIfPresentNoStoreEntryCacheWritingExceptionReturnValue() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.<String, String>emptyMap());
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(Collections.singletonMap("key", "oldValue"));
    this.cacheLoaderWriter = spy(fakeLoaderWriter);
    doThrow(new Exception()).when(this.cacheLoaderWriter).write("key", "value");
    final Ehcache<String, String> ehcache = this.getEhcache(cacheLoaderWriter);

    try {
      ehcache.computeIfPresent("key", new BiFunction<String, String, String>() {
        @Override
        public String apply(String k, String v) {
          assertEquals("key", k);
          assertEquals("oldValue", v);
          return "value";
        }
      });
      fail();
    } catch (CacheWritingException e) {
      e.printStackTrace();
    }
    verify(this.store).compute(eq("key"), getAnyBiFunction());
    verifyZeroInteractions(this.spiedResilienceStrategy);
    assertThat(fakeStore.getEntryMap().containsKey("key"), is(false));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.ComputeOutcome.class));
  }

  /**
   * Tests the effect of a {@link Ehcache#computeIfPresent(Object, BiFunction)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   *   <li>function returning null</li>
   *   <li>{@code CacheLoaderWriter.write} throws</li>
   * </ul>
   */
  @Test
  public void testComputeIfPresentNoStoreEntryCacheWritingExceptionReturnNull() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.<String, String>emptyMap());
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(Collections.singletonMap("key", "oldValue"));
    this.cacheLoaderWriter = spy(fakeLoaderWriter);
    doThrow(new Exception()).when(this.cacheLoaderWriter).delete("key");
    final Ehcache<String, String> ehcache = this.getEhcache(cacheLoaderWriter);

    try {
      ehcache.computeIfPresent("key", new BiFunction<String, String, String>() {
        @Override
        public String apply(String k, String v) {
          assertEquals("key", k);
          assertEquals("oldValue", v);
          return null;
        }
      });
      fail();
    } catch (CacheWritingException e) {
      e.printStackTrace();
    }
    verify(this.store).compute(eq("key"), getAnyBiFunction());
    verifyZeroInteractions(this.spiedResilienceStrategy);
    assertThat(fakeStore.getEntryMap().containsKey("key"), is(false));
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.ComputeOutcome.class));
  }

  /**
   * Tests the effect of a {@link Ehcache#computeIfPresent(Object, BiFunction)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   *   <li>{@code Store.compute} throws</li>
   *   <li>no {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testComputeIfPresentNoStoreEntryCacheAccessExceptionNoCacheLoaderWriter() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.<String, String>emptyMap());
    this.store = spy(fakeStore);
    doThrow(new CacheAccessException("")).when(this.store).compute(eq("key"), getAnyBiFunction());

    final Ehcache<String, String> ehcache = this.getEhcache(null);

    ehcache.computeIfPresent("key", new BiFunction<String, String, String>() {
      @Override
      public String apply(String k, String v) {
        throw new AssertionError("should not be called");
      }
    });
    verify(this.store).compute(eq("key"), getAnyBiFunction());
    verify(this.spiedResilienceStrategy).computeIfPresentFailure(eq("key"), any(CacheAccessException.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ComputeOutcome.FAILURE));
  }


  /**
   * Tests the effect of a {@link Ehcache#computeIfPresent(Object, BiFunction)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   *   <li>{@code Store.compute} throws</li>
   *   <li>key not present via {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testComputeIfPresentNoStoreEntryCacheAccessExceptionNoCacheLoaderWriterEntry() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.<String, String>emptyMap());
    this.store = spy(fakeStore);
    doThrow(new CacheAccessException("")).when(this.store).compute(eq("key"), getAnyBiFunction());

    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(Collections.<String, String>emptyMap());
    this.cacheLoaderWriter = spy(fakeLoaderWriter);
    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    final InOrder ordered = inOrder(this.cacheLoaderWriter, this.spiedResilienceStrategy);

    ehcache.computeIfPresent("key", new BiFunction<String, String, String>() {
      @Override
      public String apply(String k, String v) {
        throw new AssertionError("should not be called");
      }
    });
    verify(this.store).compute(eq("key"), getAnyBiFunction());
    ordered.verify(this.cacheLoaderWriter).load("key");
    ordered.verify(this.spiedResilienceStrategy).computeIfPresentFailure(eq("key"), any(CacheAccessException.class));
    assertThat(fakeLoaderWriter.getEntryMap().containsKey("key"), is(false));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ComputeOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link Ehcache#computeIfPresent(Object, BiFunction)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   *   <li>function returning a value</li>
   *   <li>{@code Store.compute} throws</li>
   *   <li>key present via {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testComputeIfPresentNoStoreEntryCacheAccessExceptionHasCacheLoaderWriterEntryReturnValue() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.<String, String>emptyMap());
    this.store = spy(fakeStore);
    doThrow(new CacheAccessException("")).when(this.store).compute(eq("key"), getAnyBiFunction());

    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(Collections.singletonMap("key", "oldValue"));
    this.cacheLoaderWriter = spy(fakeLoaderWriter);
    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    final InOrder ordered = inOrder(this.cacheLoaderWriter, this.spiedResilienceStrategy);

    ehcache.computeIfPresent("key", new BiFunction<String, String, String>() {
      @Override
      public String apply(String k, String v) {
        assertEquals("key", k);
        assertEquals("oldValue", v);
        return "value";
      }
    });
    verify(this.store).compute(eq("key"), getAnyBiFunction());
    ordered.verify(this.cacheLoaderWriter).write(eq("key"), any(String.class));
    ordered.verify(this.spiedResilienceStrategy).computeIfPresentFailure(eq("key"), any(CacheAccessException.class));
    assertThat(fakeLoaderWriter.getEntryMap().get("key"), is(equalTo("value")));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ComputeOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link Ehcache#computeIfPresent(Object, BiFunction)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   *   <li>function returning null</li>
   *   <li>{@code Store.compute} throws</li>
   *   <li>key present via {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testComputeIfPresentNoStoreEntryCacheAccessExceptionHasCacheLoaderWriterEntryReturnNull() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.<String, String>emptyMap());
    this.store = spy(fakeStore);
    doThrow(new CacheAccessException("")).when(this.store).compute(eq("key"), getAnyBiFunction());

    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(Collections.singletonMap("key", "oldValue"));
    this.cacheLoaderWriter = spy(fakeLoaderWriter);
    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    final InOrder ordered = inOrder(this.cacheLoaderWriter, this.spiedResilienceStrategy);

    ehcache.computeIfPresent("key", new BiFunction<String, String, String>() {
      @Override
      public String apply(String k, String v) {
        assertEquals("key", k);
        assertEquals("oldValue", v);
        return null;
      }
    });
    verify(this.store).compute(eq("key"), getAnyBiFunction());
    ordered.verify(this.cacheLoaderWriter).delete(eq("key"));
    ordered.verify(this.spiedResilienceStrategy).computeIfPresentFailure(eq("key"), any(CacheAccessException.class));
    assertThat(fakeLoaderWriter.getEntryMap().containsKey("key"), is(false));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ComputeOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link Ehcache#computeIfPresent(Object, BiFunction)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   *   <li>function returning a value</li>
   *   <li>{@code Store.compute} throws</li>
   *   <li>{@code CacheLoaderWriter.write} throws</li>
   * </ul>
   */
  @Test
  public void testComputeIfPresentNoStoreEntryCacheAccessExceptionCacheWritingExceptionReturnValue() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.<String, String>emptyMap());
    this.store = spy(fakeStore);
    doThrow(new CacheAccessException("")).when(this.store).compute(eq("key"), getAnyBiFunction());

    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(Collections.singletonMap("key", "oldValue"));
    this.cacheLoaderWriter = spy(fakeLoaderWriter);
    doThrow(new Exception()).when(this.cacheLoaderWriter).write("key", "value");
    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    final InOrder ordered = inOrder(this.cacheLoaderWriter, this.spiedResilienceStrategy);

    ehcache.computeIfPresent("key", new BiFunction<String, String, String>() {
      @Override
      public String apply(String k, String v) {
        assertEquals("key", k);
        assertEquals("oldValue", v);
        return "value";
      }
    });
    verify(this.store).compute(eq("key"), getAnyBiFunction());
    ordered.verify(this.cacheLoaderWriter).write(eq("key"), any(String.class));
    ordered.verify(this.spiedResilienceStrategy)
        .computeIfPresentFailure(eq("key"), any(CacheAccessException.class), any(CacheWritingException.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ComputeOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link Ehcache#computeIfPresent(Object, BiFunction)} for
   * <ul>
   *   <li>key not present in {@code Store}</li>
   *   <li>function returning null</li>
   *   <li>{@code Store.compute} throws</li>
   *   <li>{@code CacheLoaderWriter.write} throws</li>
   * </ul>
   */
  @Test
  public void testComputeIfPresentNoStoreEntryCacheAccessExceptionCacheWritingExceptionReturnNull() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.<String, String>emptyMap());
    this.store = spy(fakeStore);
    doThrow(new CacheAccessException("")).when(this.store).compute(eq("key"), getAnyBiFunction());

    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(Collections.singletonMap("key", "oldValue"));
    this.cacheLoaderWriter = spy(fakeLoaderWriter);
    doThrow(new Exception()).when(this.cacheLoaderWriter).delete("key");
    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    final InOrder ordered = inOrder(this.cacheLoaderWriter, this.spiedResilienceStrategy);

    ehcache.computeIfPresent("key", new BiFunction<String, String, String>() {
      @Override
      public String apply(String k, String v) {
        assertEquals("key", k);
        assertEquals("oldValue", v);
        return null;
      }
    });
    verify(this.store).compute(eq("key"), getAnyBiFunction());
    ordered.verify(this.cacheLoaderWriter).delete(eq("key"));
    ordered.verify(this.spiedResilienceStrategy)
        .computeIfPresentFailure(eq("key"), any(CacheAccessException.class), any(CacheWritingException.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ComputeOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link Ehcache#computeIfPresent(Object, BiFunction)} for
   * <ul>
   *   <li>key present in {@code Store}</li>
   *   <li>function returning a value</li>
   *   <li>no {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testComputeIfPresentHasStoreEntryNoCacheLoaderWriterReturnValue() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "oldValue"));
    this.store = spy(fakeStore);

    final Ehcache<String, String> ehcache = this.getEhcache(null);

    assertThat(ehcache.computeIfPresent("key", new BiFunction<String, String, String>() {
      @Override
      public String apply(String k, String v) {
        assertEquals("key", k);
        assertEquals("oldValue", v);
        return "value";
      }
    }), is(equalTo("value")));
    verify(this.store).compute(eq("key"), getAnyBiFunction());
    verifyZeroInteractions(this.spiedResilienceStrategy);
    assertThat(fakeStore.getEntryMap().get("key"), is(equalTo("value")));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ComputeOutcome.PRESENT_UPDATED));
  }

  /**
   * Tests the effect of a {@link Ehcache#computeIfPresent(Object, BiFunction)} for
   * <ul>
   *   <li>key present in {@code Store}</li>
   *   <li>function returning null</li>
   *   <li>no {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testComputeIfPresentHasStoreEntryNoCacheLoaderWriterReturnNull() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "oldValue"));
    this.store = spy(fakeStore);

    final Ehcache<String, String> ehcache = this.getEhcache(null);

    assertThat(ehcache.computeIfPresent("key", new BiFunction<String, String, String>() {
      @Override
      public String apply(String k, String v) {
        assertEquals("key", k);
        assertEquals("oldValue", v);
        return null;
      }
    }), is(nullValue()));
    verify(this.store).compute(eq("key"), getAnyBiFunction());
    verifyZeroInteractions(this.spiedResilienceStrategy);
    assertThat(fakeStore.getEntryMap().containsKey("key"), is(false));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ComputeOutcome.PRESENT_REMOVED));
  }

  /**
   * Tests the effect of a {@link Ehcache#computeIfPresent(Object, BiFunction)} for
   * <ul>
   *   <li>key present in {@code Store}</li>
   *   <li>function returning a value</li>
   *   <li>key not present via {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testComputeIfPresentHasStoreEntryNoCacheLoaderWriterEntryReturnValue() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "oldValue"));
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(Collections.<String, String>emptyMap());
    final Ehcache<String, String> ehcache = this.getEhcache(fakeLoaderWriter);

    assertThat(ehcache.computeIfPresent("key", new BiFunction<String, String, String>() {
      @Override
      public String apply(String k, String v) {
        assertEquals("key", k);
        assertEquals("oldValue", v);
        return "value";
      }
    }), is(equalTo("value")));
    verify(this.store).compute(eq("key"), getAnyBiFunction());
    verifyZeroInteractions(this.spiedResilienceStrategy);
    assertThat(fakeStore.getEntryMap().get("key"), is(equalTo("value")));
    assertThat(fakeLoaderWriter.getEntryMap().get("key"), equalTo("value"));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ComputeOutcome.PRESENT_UPDATED));
  }

  /**
   * Tests the effect of a {@link Ehcache#computeIfPresent(Object, BiFunction)} for
   * <ul>
   *   <li>key present in {@code Store}</li>
   *   <li>function returning null</li>
   *   <li>key not present via {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testComputeIfPresentHasStoreEntryNoCacheLoaderWriterEntryReturnNull() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "oldValue"));
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(Collections.<String, String>emptyMap());
    final Ehcache<String, String> ehcache = this.getEhcache(fakeLoaderWriter);

    assertThat(ehcache.computeIfPresent("key", new BiFunction<String, String, String>() {
      @Override
      public String apply(String k, String v) {
        assertEquals("key", k);
        assertEquals("oldValue", v);
        return null;
      }
    }), is(nullValue()));
    verify(this.store).compute(eq("key"), getAnyBiFunction());
    verifyZeroInteractions(this.spiedResilienceStrategy);
    assertThat(fakeStore.getEntryMap().containsKey("key"), is(false));
    assertThat(fakeLoaderWriter.getEntryMap().containsKey("key"), equalTo(false));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ComputeOutcome.PRESENT_REMOVED));
  }

  /**
   * Tests the effect of a {@link Ehcache#computeIfPresent(Object, BiFunction)} for
   * <ul>
   *   <li>key present in {@code Store}</li>
   *   <li>function returning a value</li>
   *   <li>key present via {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testComputeIfPresentHasStoreEntryHasCacheLoaderWriterEntryReturnValue() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "oldValue"));
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(Collections.singletonMap("key", "oldValue"));
    final Ehcache<String, String> ehcache = this.getEhcache(fakeLoaderWriter);

    assertThat(ehcache.computeIfPresent("key", new BiFunction<String, String, String>() {
      @Override
      public String apply(String k, String v) {
        assertEquals("key", k);
        assertEquals("oldValue", v);
        return "value";
      }
    }), is(equalTo("value")));
    verify(this.store).compute(eq("key"), getAnyBiFunction());
    verifyZeroInteractions(this.spiedResilienceStrategy);
    assertThat(fakeStore.getEntryMap().get("key"), is(equalTo("value")));
    assertThat(fakeLoaderWriter.getEntryMap().get("key"), equalTo("value"));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ComputeOutcome.PRESENT_UPDATED));
  }

  /**
   * Tests the effect of a {@link Ehcache#computeIfPresent(Object, BiFunction)} for
   * <ul>
   *   <li>key present in {@code Store}</li>
   *   <li>function returning null</li>
   *   <li>key present via {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testComputeIfPresentHasStoreEntryHasCacheLoaderWriterEntryReturnNull() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "oldValue"));
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(Collections.singletonMap("key", "oldValue"));
    final Ehcache<String, String> ehcache = this.getEhcache(fakeLoaderWriter);

    assertThat(ehcache.computeIfPresent("key", new BiFunction<String, String, String>() {
      @Override
      public String apply(String k, String v) {
        assertEquals("key", k);
        assertEquals("oldValue", v);
        return null;
      }
    }), is(nullValue()));
    verify(this.store).compute(eq("key"), getAnyBiFunction());
    verifyZeroInteractions(this.spiedResilienceStrategy);
    assertThat(fakeStore.getEntryMap().containsKey("key"), is(false));
    assertThat(fakeLoaderWriter.getEntryMap().containsKey("key"), equalTo(false));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ComputeOutcome.PRESENT_REMOVED));
  }

  /**
   * Tests the effect of a {@link Ehcache#computeIfPresent(Object, BiFunction)} for
   * <ul>
   *   <li>key present in {@code Store}</li>
   *   <li>function returning a value</li>
   *   <li>{@code CacheLoaderWriter.write} throws</li>
   * </ul>
   */
  @Test
  public void testComputeIfPresentHasStoreEntryCacheWritingExceptionReturnValue() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "oldValue"));
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(Collections.singletonMap("key", "oldValue"));
    this.cacheLoaderWriter = spy(fakeLoaderWriter);
    doThrow(new Exception()).when(this.cacheLoaderWriter).write("key", "value");
    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    try {
      ehcache.computeIfPresent("key", new BiFunction<String, String, String>() {
        @Override
        public String apply(String k, String v) {
          assertEquals("key", k);
          assertEquals("oldValue", v);
          return "value";
        }
      });
      fail();
    } catch (CacheWritingException e) {
      // Expected
    }
    verify(this.store).compute(eq("key"), getAnyBiFunction());
    verifyZeroInteractions(this.spiedResilienceStrategy);
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.ComputeOutcome.class));
  }

  /**
   * Tests the effect of a {@link Ehcache#computeIfPresent(Object, BiFunction)} for
   * <ul>
   *   <li>key present in {@code Store}</li>
   *   <li>function returning null</li>
   *   <li>{@code CacheLoaderWriter.write} throws</li>
   * </ul>
   */
  @Test
  public void testComputeIfPresentHasStoreEntryCacheWritingExceptionReturnNull() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "oldValue"));
    this.store = spy(fakeStore);

    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(Collections.singletonMap("key", "oldValue"));
    this.cacheLoaderWriter = spy(fakeLoaderWriter);
    doThrow(new Exception()).when(this.cacheLoaderWriter).delete("key");
    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    try {
      ehcache.computeIfPresent("key", new BiFunction<String, String, String>() {
        @Override
        public String apply(String k, String v) {
          assertEquals("key", k);
          assertEquals("oldValue", v);
          return null;
        }
      });
      fail();
    } catch (CacheWritingException e) {
      // Expected
    }
    verify(this.store).compute(eq("key"), getAnyBiFunction());
    verifyZeroInteractions(this.spiedResilienceStrategy);
    validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.ComputeOutcome.class));
  }

  /**
   * Tests the effect of a {@link Ehcache#computeIfPresent(Object, BiFunction)} for
   * <ul>
   *   <li>key present in {@code Store}</li>
   *   <li>{@code Store.compute} throws</li>
   *   <li>no {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testComputeIfPresentHasStoreEntryCacheAccessExceptionNoCacheLoaderWriter() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "oldValue"));
    this.store = spy(fakeStore);
    doThrow(new CacheAccessException("")).when(this.store).compute(eq("key"), getAnyBiFunction());

    final Ehcache<String, String> ehcache = this.getEhcache(null);

    ehcache.computeIfPresent("key", new BiFunction<String, String, String>() {
      @Override
      public String apply(String s, String s2) {
        throw new AssertionError("should not be called");
      }
    });
    verify(this.store).compute(eq("key"), getAnyBiFunction());
    verify(this.spiedResilienceStrategy).computeIfPresentFailure(eq("key"), any(CacheAccessException.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ComputeOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link Ehcache#computeIfPresent(Object, BiFunction)} for
   * <ul>
   *   <li>key present in {@code Store}</li>
   *   <li>{@code Store.compute} throws</li>
   *   <li>key not present via {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testComputeIfPresentHasStoreEntryCacheAccessExceptionNoCacheLoaderWriterEntry() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "oldValue"));
    this.store = spy(fakeStore);
    doThrow(new CacheAccessException("")).when(this.store).compute(eq("key"), getAnyBiFunction());

    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(Collections.<String, String>emptyMap());
    this.cacheLoaderWriter = spy(fakeLoaderWriter);
    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    ehcache.computeIfPresent("key", new BiFunction<String, String, String>() {
      @Override
      public String apply(String k, String v) {
        throw new AssertionError("should not be called");
      }
    });
    verify(this.store).compute(eq("key"), getAnyBiFunction());
    verify(this.store, times(1)).remove("key");
    verify(this.spiedResilienceStrategy).computeIfPresentFailure(eq("key"), any(CacheAccessException.class));
    assertThat(fakeLoaderWriter.getEntryMap().containsKey("key"), is(false));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ComputeOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link Ehcache#computeIfPresent(Object, BiFunction)} for
   * <ul>
   *   <li>key present in {@code Store}</li>
   *   <li>function returning a value</li>
   *   <li>{@code Store.compute} throws</li>
   *   <li>key present via {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testComputeIfPresentHasStoreEntryCacheAccessExceptionHasCacheLoaderWriterEntryReturnValue() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "oldValue"));
    this.store = spy(fakeStore);
    doThrow(new CacheAccessException("")).when(this.store).compute(eq("key"), getAnyBiFunction());

    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(Collections.singletonMap("key", "oldValue"));
    this.cacheLoaderWriter = spy(fakeLoaderWriter);
    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    final InOrder ordered = inOrder(this.cacheLoaderWriter, this.spiedResilienceStrategy);

    ehcache.computeIfPresent("key", new BiFunction<String, String, String>() {
      @Override
      public String apply(String k, String v) {
        assertEquals("key", k);
        assertEquals("oldValue", v);
        return "value";
      }
    });
    verify(this.store).compute(eq("key"), getAnyBiFunction());
    ordered.verify(this.cacheLoaderWriter).write(eq("key"), any(String.class));
    ordered.verify(this.spiedResilienceStrategy).computeIfPresentFailure(eq("key"), any(CacheAccessException.class));
    assertThat(fakeLoaderWriter.getEntryMap().get("key"), is(equalTo("value")));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ComputeOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link Ehcache#computeIfPresent(Object, BiFunction)} for
   * <ul>
   *   <li>key present in {@code Store}</li>
   *   <li>function returning null</li>
   *   <li>{@code Store.compute} throws</li>
   *   <li>key present via {@code CacheLoaderWriter}</li>
   * </ul>
   */
  @Test
  public void testComputeIfPresentHasStoreEntryCacheAccessExceptionHasCacheLoaderWriterEntryReturnNull() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "oldValue"));
    this.store = spy(fakeStore);
    doThrow(new CacheAccessException("")).when(this.store).compute(eq("key"), getAnyBiFunction());

    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(Collections.singletonMap("key", "oldValue"));
    this.cacheLoaderWriter = spy(fakeLoaderWriter);
    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    final InOrder ordered = inOrder(this.cacheLoaderWriter, this.spiedResilienceStrategy);

    ehcache.computeIfPresent("key", new BiFunction<String, String, String>() {
      @Override
      public String apply(String k, String v) {
        assertEquals("key", k);
        assertEquals("oldValue", v);
        return null;
      }
    });
    verify(this.store).compute(eq("key"), getAnyBiFunction());
    ordered.verify(this.cacheLoaderWriter).delete(eq("key"));
    ordered.verify(this.spiedResilienceStrategy).computeIfPresentFailure(eq("key"), any(CacheAccessException.class));
    assertThat(fakeLoaderWriter.getEntryMap().containsKey("key"), is(false));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ComputeOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link Ehcache#computeIfPresent(Object, BiFunction)} for
   * <ul>
   *   <li>key present in {@code Store}</li>
   *   <li>function returning a value</li>
   *   <li>{@code Store.compute} throws</li>
   *   <li>{@code CacheLoaderWriter.write} throws</li>
   * </ul>
   */
  @Test
  public void testComputeIfPresentHasStoreEntryCacheAccessExceptionCacheWritingExceptionReturnValue() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "oldValue"));
    this.store = spy(fakeStore);
    doThrow(new CacheAccessException("")).when(this.store).compute(eq("key"), getAnyBiFunction());

    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(Collections.singletonMap("key", "oldValue"));
    this.cacheLoaderWriter = spy(fakeLoaderWriter);
    doThrow(new Exception()).when(this.cacheLoaderWriter).write("key", "value");
    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    final InOrder ordered = inOrder(this.cacheLoaderWriter, this.spiedResilienceStrategy);

    ehcache.computeIfPresent("key", new BiFunction<String, String, String>() {
      @Override
      public String apply(String k, String v) {
        assertEquals("key", k);
        assertEquals("oldValue", v);
        return "value";
      }
    });
    verify(this.store).compute(eq("key"), getAnyBiFunction());
    ordered.verify(this.cacheLoaderWriter).write(eq("key"), any(String.class));
    ordered.verify(this.spiedResilienceStrategy)
        .computeIfPresentFailure(eq("key"), any(CacheAccessException.class), any(CacheWritingException.class));
    validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.ComputeOutcome.FAILURE));
  }

  /**
   * Tests the effect of a {@link Ehcache#computeIfPresent(Object, BiFunction)} for
   * <ul>
   *   <li>key present in {@code Store}</li>
   *   <li>function returning null</li>
   *   <li>{@code Store.compute} throws</li>
   *   <li>{@code CacheLoaderWriter.write} throws</li>
   * </ul>
   */
  @Test
  public void testComputeIfPresentHasStoreEntryCacheAccessExceptionCacheWritingExceptionReturnNull() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "oldValue"));
    this.store = spy(fakeStore);
    doThrow(new CacheAccessException("")).when(this.store).compute(eq("key"), getAnyBiFunction());

    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(Collections.singletonMap("key", "oldValue"));
    this.cacheLoaderWriter = spy(fakeLoaderWriter);
    doThrow(new Exception()).when(this.cacheLoaderWriter).delete("key");
    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoaderWriter);

    final InOrder ordered = inOrder(this.cacheLoaderWriter, this.spiedResilienceStrategy);

    ehcache.computeIfPresent("key", new BiFunction<String, String, String>() {
      @Override
      public String apply(String k, String v) {
        assertEquals("key", k);
        assertEquals("oldValue", v);
        return null;
      }
    });
    verify(this.store).compute(eq("key"), getAnyBiFunction());
    ordered.verify(this.cacheLoaderWriter).delete(eq("key"));
    ordered.verify(this.spiedResilienceStrategy)
        .computeIfPresentFailure(eq("key"), any(CacheAccessException.class), any(CacheWritingException.class));
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
