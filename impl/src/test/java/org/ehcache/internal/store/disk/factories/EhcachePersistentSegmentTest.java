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

package org.ehcache.internal.store.disk.factories;

import java.io.IOException;
import org.ehcache.internal.store.offheap.factories.*;
import org.ehcache.function.BiFunction;
import org.ehcache.function.Predicate;
import org.ehcache.function.Predicates;
import org.ehcache.internal.store.offheap.HeuristicConfiguration;
import org.ehcache.internal.store.offheap.portability.SerializerPortability;
import org.ehcache.spi.serialization.DefaultSerializationProvider;
import org.ehcache.spi.serialization.SerializationProvider;
import org.ehcache.spi.serialization.Serializer;
import org.ehcache.spi.serialization.UnsupportedTypeException;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.terracotta.offheapstore.util.Factory;

import java.util.Map;
import org.ehcache.internal.store.disk.factories.EhcachePersistentSegmentFactory.EhcachePersistentSegment;

import static org.ehcache.internal.store.disk.OffHeapDiskStore.persistent;
import org.ehcache.internal.store.offheap.factories.EhcacheSegmentFactory.EhcacheSegment.EvictionListener;
import static org.ehcache.spi.TestServiceProvider.providerContaining;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import org.terracotta.offheapstore.disk.paging.MappedPageSource;
import org.terracotta.offheapstore.disk.persistent.PersistentPortability;
import org.terracotta.offheapstore.disk.storage.FileBackedStorageEngine;

public class EhcachePersistentSegmentTest {

  @Rule
  public final TemporaryFolder folder = new TemporaryFolder();
  
  private EhcachePersistentSegmentFactory.EhcachePersistentSegment<String, String> createTestSegment() throws IOException {
    return createTestSegment(Predicates.<Map.Entry<String, String>>none(), mock(EvictionListener.class));
  }
  
  private EhcachePersistentSegmentFactory.EhcachePersistentSegment<String, String> createTestSegment(Predicate<Map.Entry<String, String>> evictionPredicate) throws IOException {
    return createTestSegment(evictionPredicate, mock(EvictionListener.class));
  }
  
  private EhcachePersistentSegmentFactory.EhcachePersistentSegment<String, String> createTestSegment(EvictionListener<String, String> evictionListener) throws IOException {
    return createTestSegment(Predicates.<Map.Entry<String, String>>none(), evictionListener);
  }
  
  private EhcachePersistentSegmentFactory.EhcachePersistentSegment<String, String> createTestSegment(Predicate<Map.Entry<String, String>> evictionPredicate, EvictionListener<String, String> evictionListener) throws IOException {
    try {
      HeuristicConfiguration configuration = new HeuristicConfiguration(1024 * 1024);
      SerializationProvider serializationProvider = new DefaultSerializationProvider(null);
      serializationProvider.start(providerContaining());
      MappedPageSource pageSource = new MappedPageSource(folder.newFile(), true, configuration.getMaximumSize());
      Serializer<String> keySerializer = serializationProvider.createKeySerializer(String.class, EhcachePersistentSegmentTest.class.getClassLoader());
      Serializer<String> valueSerializer = serializationProvider.createValueSerializer(String.class, EhcachePersistentSegmentTest.class.getClassLoader());
      PersistentPortability<String> keyPortability = persistent(new SerializerPortability<String>(keySerializer));
      PersistentPortability<String> elementPortability = persistent(new SerializerPortability<String>(valueSerializer));
      Factory<FileBackedStorageEngine<String, String>> storageEngineFactory = FileBackedStorageEngine.createFactory(pageSource, keyPortability, elementPortability);
      return new EhcachePersistentSegmentFactory.EhcachePersistentSegment<String, String>(pageSource, storageEngineFactory.newInstance(), 1, true, evictionPredicate, evictionListener);
    } catch (UnsupportedTypeException e) {
      throw new AssertionError(e);
    }
  }

  @Test
  public void testComputeFunctionCalledWhenNoMapping() throws IOException {
    EhcachePersistentSegment<String, String> segment = createTestSegment();
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
  public void testComputeFunctionReturnsSameNoPin() throws IOException {
    EhcachePersistentSegment<String, String> segment = createTestSegment();
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
  public void testComputeFunctionReturnsSamePins() throws IOException {
    EhcachePersistentSegment<String, String> segment = createTestSegment();
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
  public void testComputeFunctionReturnsSamePreservesPinWhenNoPin() throws IOException {
    EhcachePersistentSegment<String, String> segment = createTestSegment();
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
  public void testComputeFunctionReturnsDifferentNoPin() throws IOException {
    EhcachePersistentSegment<String, String> segment = createTestSegment();
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
  public void testComputeFunctionReturnsDifferentPins() throws IOException {
    EhcachePersistentSegment<String, String> segment = createTestSegment();
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
  public void testComputeFunctionReturnsDifferentClearsPin() throws IOException {
    EhcachePersistentSegment<String, String> segment = createTestSegment();
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
  public void testComputeFunctionReturnsNullRemoves() throws IOException {
    EhcachePersistentSegment<String, String> segment = createTestSegment();
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
  public void testComputeIfPresentNotCalledOnNotContainedKey() throws IOException {
    EhcachePersistentSegment<String, String> segment = createTestSegment();
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
  public void testComputeIfPresentReturnsSameValue() throws IOException {
    EhcachePersistentSegment<String, String> segment = createTestSegment();
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
  public void testComputeIfPresentReturnsDifferentValue() throws IOException {
    EhcachePersistentSegment<String, String> segment = createTestSegment();
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
  public void testComputeIfPresentReturnsNullRemovesMapping() throws IOException {
    EhcachePersistentSegment<String, String> segment = createTestSegment();
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
  public void testPutVetoedComputesMetadata() throws IOException {
    EhcachePersistentSegment<String, String> segment = createTestSegment(new Predicate<Map.Entry<String, String>>() {
      @Override
      public boolean test(Map.Entry<String, String> argument) {
        return "vetoed".equals(argument.getKey());
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
  public void testPutPinnedVetoedComputesMetadata() throws IOException {
    EhcachePersistentSegment<String, String> segment = createTestSegment(new Predicate<Map.Entry<String, String>>() {
      @Override
      public boolean test(Map.Entry<String, String> argument) {
        return "vetoed".equals(argument.getKey());
      }
    });
    try {
      segment.putPinned("vetoed", "value");
      assertThat(segment.getMetadata("vetoed", EhcacheSegmentFactory.EhcacheSegment.VETOED), is(EhcacheSegmentFactory.EhcacheSegment.VETOED));
    } finally {
      segment.destroy();
    }
  }

  @Test
  public void testVetoedPreventsEviction() throws IOException {
    EhcachePersistentSegment<String, String> segment = createTestSegment();
    try {
      assertThat(segment.evictable(1), is(true));
      assertThat(segment.evictable(EhcacheSegmentFactory.EhcacheSegment.VETOED | 1), is(false));
    } finally {
      segment.destroy();
    }
  }

  @Test
  public void testEvictionFiresEvent() throws IOException {
    EvictionListener<String, String> evictionListener = mock(EvictionListener.class);
    EhcachePersistentSegment<String, String> segment = createTestSegment(evictionListener);
    try {
      segment.put("key", "value");
      segment.evict(segment.getEvictionIndex(), false);
      verify(evictionListener).onEviction("key", "value");
    } finally {
      segment.destroy();
    }
  }
}