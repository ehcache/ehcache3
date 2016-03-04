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

package org.ehcache.impl.internal.store.offheap;

import org.ehcache.config.Eviction;
import org.ehcache.config.EvictionVeto;
import org.ehcache.function.BiFunction;
import org.ehcache.impl.internal.spi.serialization.DefaultSerializationProvider;
import org.ehcache.impl.internal.store.offheap.factories.EhcacheSegmentFactory;
import org.ehcache.impl.internal.store.offheap.portability.SerializerPortability;
import org.ehcache.spi.serialization.SerializationProvider;
import org.ehcache.spi.serialization.Serializer;
import org.ehcache.spi.serialization.UnsupportedTypeException;
import org.junit.Test;
import org.terracotta.offheapstore.paging.PageSource;
import org.terracotta.offheapstore.paging.UpfrontAllocatingPageSource;
import org.terracotta.offheapstore.storage.OffHeapBufferStorageEngine;
import org.terracotta.offheapstore.storage.PointerSize;
import org.terracotta.offheapstore.storage.portability.Portability;
import org.terracotta.offheapstore.util.Factory;

import static org.ehcache.impl.internal.spi.TestServiceProvider.providerContaining;
import static org.ehcache.impl.internal.store.offheap.OffHeapStoreUtils.getBufferSource;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

/**
 *
 * @author cdennis
 */
public class EhcacheConcurrentOffHeapClockCacheTest {

  private EhcacheConcurrentOffHeapClockCache<String, String> createTestSegment() {
    return createTestSegment(Eviction.none(), mock(EhcacheSegmentFactory.EhcacheSegment.EvictionListener.class));
  }

  private EhcacheConcurrentOffHeapClockCache<String, String> createTestSegment(EvictionVeto<? super String, ? super String> evictionPredicate) {
    return createTestSegment(evictionPredicate, mock(EhcacheSegmentFactory.EhcacheSegment.EvictionListener.class));
  }

  private EhcacheConcurrentOffHeapClockCache<String, String> createTestSegment(EhcacheSegmentFactory.EhcacheSegment.EvictionListener<String, String> evictionListener) {
    return createTestSegment(Eviction.none(), evictionListener);
  }

  private EhcacheConcurrentOffHeapClockCache<String, String> createTestSegment(EvictionVeto<? super String, ? super String> evictionPredicate, EhcacheSegmentFactory.EhcacheSegment.EvictionListener<String, String> evictionListener) {
    try {
      HeuristicConfiguration configuration = new HeuristicConfiguration(1024 * 1024);
      SerializationProvider serializationProvider = new DefaultSerializationProvider(null);
      serializationProvider.start(providerContaining());
      PageSource pageSource = new UpfrontAllocatingPageSource(getBufferSource(), configuration.getMaximumSize(), configuration.getMaximumChunkSize(), configuration.getMinimumChunkSize());
      Serializer<String> keySerializer = serializationProvider.createKeySerializer(String.class, EhcacheConcurrentOffHeapClockCacheTest.class.getClassLoader());
      Serializer<String> valueSerializer = serializationProvider.createValueSerializer(String.class, EhcacheConcurrentOffHeapClockCacheTest.class.getClassLoader());
      Portability<String> keyPortability = new SerializerPortability<String>(keySerializer);
      Portability<String> elementPortability = new SerializerPortability<String>(valueSerializer);
      Factory<OffHeapBufferStorageEngine<String, String>> storageEngineFactory = OffHeapBufferStorageEngine.createFactory(PointerSize.INT, pageSource, configuration.getInitialSegmentTableSize(), keyPortability, elementPortability, false, true);
      EhcacheSegmentFactory<String, String> segmentFactory = new EhcacheSegmentFactory<String, String>(pageSource, storageEngineFactory, 0, evictionPredicate, evictionListener);
      return new EhcacheConcurrentOffHeapClockCache<String, String>(evictionPredicate, segmentFactory, 1);
    } catch (UnsupportedTypeException e) {
      throw new AssertionError(e);
    }
  }

  @Test
  public void testComputeFunctionCalledWhenNoMapping() {
    EhcacheConcurrentOffHeapClockCache<String, String> segment = createTestSegment();
    try {
      String value = segment.compute("key", new BiFunction<String, String, String>() {
        @Override
        public String apply(String s, String s2) {
          return "value";
        }
      }, false);
      assertThat(value, is("value"));
      assertThat(segment.get("key"), is(value));
    } finally {
      segment.destroy();
    }
  }

  @Test
  public void testComputeFunctionReturnsSameNoPin() {
    EhcacheConcurrentOffHeapClockCache<String, String> segment = createTestSegment();
    try {
      segment.put("key", "value");
      String value = segment.compute("key", new BiFunction<String, String, String>() {
        @Override
        public String apply(String s, String s2) {
          return s2;
        }
      }, false);
      assertThat(value, is("value"));
      assertThat(segment.isPinned("key"), is(false));
    } finally {
      segment.destroy();
    }
  }

  @Test
  public void testComputeFunctionReturnsSamePins() {
    EhcacheConcurrentOffHeapClockCache<String, String> segment = createTestSegment();
    try {
      segment.put("key", "value");
      String value = segment.compute("key", new BiFunction<String, String, String>() {
        @Override
        public String apply(String s, String s2) {
          return s2;
        }
      }, true);
      assertThat(value, is("value"));
      assertThat(segment.isPinned("key"), is(true));
    } finally {
      segment.destroy();
    }
  }

  @Test
  public void testComputeFunctionReturnsSamePreservesPinWhenNoPin() {
    EhcacheConcurrentOffHeapClockCache<String, String> segment = createTestSegment();
    try {
      segment.putPinned("key", "value");
      String value = segment.compute("key", new BiFunction<String, String, String>() {
        @Override
        public String apply(String s, String s2) {
          return s2;
        }
      }, false);
      assertThat(value, is("value"));
      assertThat(segment.isPinned("key"), is(true));
    } finally {
      segment.destroy();
    }
  }

  @Test
  public void testComputeFunctionReturnsDifferentNoPin() {
    EhcacheConcurrentOffHeapClockCache<String, String> segment = createTestSegment();
    try {
      segment.put("key", "value");
      String value = segment.compute("key", new BiFunction<String, String, String>() {
        @Override
        public String apply(String s, String s2) {
          return "otherValue";
        }
      }, false);
      assertThat(value, is("otherValue"));
      assertThat(segment.isPinned("key"), is(false));
    } finally {
      segment.destroy();
    }
  }

  @Test
  public void testComputeFunctionReturnsDifferentPins() {
    EhcacheConcurrentOffHeapClockCache<String, String> segment = createTestSegment();
    try {
      segment.put("key", "value");
      String value = segment.compute("key", new BiFunction<String, String, String>() {
        @Override
        public String apply(String s, String s2) {
          return "otherValue";
        }
      }, true);
      assertThat(value, is("otherValue"));
      assertThat(segment.isPinned("key"), is(true));
    } finally {
      segment.destroy();
    }
  }

  @Test
  public void testComputeFunctionReturnsDifferentClearsPin() {
    EhcacheConcurrentOffHeapClockCache<String, String> segment = createTestSegment();
    try {
      segment.putPinned("key", "value");
      String value = segment.compute("key", new BiFunction<String, String, String>() {
        @Override
        public String apply(String s, String s2) {
          return "otherValue";
        }
      }, false);
      assertThat(value, is("otherValue"));
      assertThat(segment.isPinned("key"), is(false));
    } finally {
      segment.destroy();
    }
  }

  @Test
  public void testComputeFunctionReturnsNullRemoves() {
    EhcacheConcurrentOffHeapClockCache<String, String> segment = createTestSegment();
    try {
      segment.putPinned("key", "value");
      String value = segment.compute("key", new BiFunction<String, String, String>() {
        @Override
        public String apply(String s, String s2) {
          return null;
        }
      }, false);
      assertThat(value, nullValue());
      assertThat(segment.containsKey("key"), is(false));
    } finally {
      segment.destroy();
    }
  }

  @Test
  public void testComputeIfPresentNotCalledOnNotContainedKey() {
    EhcacheConcurrentOffHeapClockCache<String, String> segment = createTestSegment();
    try {
      try {
        segment.computeIfPresent("key", new BiFunction<String, String, String>() {
          @Override
          public String apply(String s, String s2) {
            throw new UnsupportedOperationException("Should not have been called!");
          }
        });
      } catch (UnsupportedOperationException e) {
        fail("Mapping function should not have been called.");
      }
    } finally {
      segment.destroy();
    }
  }

  @Test
  public void testComputeIfPresentReturnsSameValue() {
    EhcacheConcurrentOffHeapClockCache<String, String> segment = createTestSegment();
    try {
      segment.put("key", "value");
      String value = segment.computeIfPresent("key", new BiFunction<String, String, String>() {
        @Override
        public String apply(String s, String s2) {
          return s2;
        }
      });
      assertThat(segment.get("key"), is(value));
    } finally {
      segment.destroy();
    }
  }

  @Test
  public void testComputeIfPresentReturnsDifferentValue() {
    EhcacheConcurrentOffHeapClockCache<String, String> segment = createTestSegment();
    try {
      segment.put("key", "value");
      String value = segment.computeIfPresent("key", new BiFunction<String, String, String>() {
        @Override
        public String apply(String s, String s2) {
          return "newValue";
        }
      });
      assertThat(segment.get("key"), is(value));
    } finally {
      segment.destroy();
    }
  }

  @Test
  public void testComputeIfPresentReturnsNullRemovesMapping() {
    EhcacheConcurrentOffHeapClockCache<String, String> segment = createTestSegment();
    try {
      segment.put("key", "value");
      String value = segment.computeIfPresent("key", new BiFunction<String, String, String>() {
        @Override
        public String apply(String s, String s2) {
          return null;
        }
      });
      assertThat(segment.containsKey("key"), is(false));
    } finally {
      segment.destroy();
    }
  }

  @Test
  public void testPutVetoedComputesMetadata() {
    EhcacheConcurrentOffHeapClockCache<String, String> segment = createTestSegment(new EvictionVeto<String, String>() {
      @Override
      public boolean vetoes(String key, String value) {
        return "vetoed".equals(key);
      }
    });
    try {
      segment.put("vetoed", "value");
      assertThat(segment.getMetadata("vetoed", EhcacheSegmentFactory.EhcacheSegment.VETOED), is(EhcacheSegmentFactory.EhcacheSegment.VETOED));
    } finally {
      segment.destroy();
    }
  }

  @Test
  public void testPutPinnedVetoedComputesMetadata() {
    EhcacheConcurrentOffHeapClockCache<String, String> segment = createTestSegment(new EvictionVeto<String, String>() {
      @Override
      public boolean vetoes(String key, String value) {
        return "vetoed".equals(key);
      }
    });
    try {
      segment.putPinned("vetoed", "value");
      assertThat(segment.getMetadata("vetoed", EhcacheSegmentFactory.EhcacheSegment.VETOED), is(EhcacheSegmentFactory.EhcacheSegment.VETOED));
    } finally {
      segment.destroy();
    }
  }
}
