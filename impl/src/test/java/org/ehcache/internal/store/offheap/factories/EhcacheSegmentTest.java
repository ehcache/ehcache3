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

package org.ehcache.internal.store.offheap.factories;

import org.ehcache.function.BiFunction;
import org.ehcache.function.Predicate;
import org.ehcache.function.Predicates;
import org.ehcache.internal.store.offheap.HeuristicConfiguration;
import org.ehcache.internal.store.offheap.portability.SerializerPortability;
import org.ehcache.spi.serialization.DefaultSerializationProvider;
import org.ehcache.spi.serialization.SerializationProvider;
import org.ehcache.spi.serialization.Serializer;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.terracotta.offheapstore.paging.PageSource;
import org.terracotta.offheapstore.paging.UpfrontAllocatingPageSource;
import org.terracotta.offheapstore.storage.OffHeapBufferStorageEngine;
import org.terracotta.offheapstore.storage.PointerSize;
import org.terracotta.offheapstore.storage.portability.Portability;
import org.terracotta.offheapstore.util.Factory;

import java.util.Map;

import static org.ehcache.internal.store.offheap.OffHeapStoreUtils.getBufferSource;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class EhcacheSegmentTest {

  private static EhcacheSegmentFactory.EhcacheSegment<String, String> segment;
  private static TestPredicate<Map.Entry<String, String>> predicate;
  private static EhcacheSegmentFactory.EhcacheSegment.EvictionListener<String, String> evictionListener;

  @BeforeClass
  public static void setUpClass() {
    HeuristicConfiguration configuration = new HeuristicConfiguration(1024 * 1024);
    SerializationProvider serializationProvider = new DefaultSerializationProvider();
    serializationProvider.start(null, null);
    PageSource pageSource = new UpfrontAllocatingPageSource(getBufferSource(), configuration.getMaximumSize(), configuration.getMaximumChunkSize(), configuration.getMinimumChunkSize());
    Serializer<String> stringSerializer = serializationProvider.createSerializer(String.class, EhcacheSegmentTest.class.getClassLoader());
    Portability<String> keyPortability = new SerializerPortability<String>(stringSerializer);
    Portability<String> elementPortability = new SerializerPortability<String>(stringSerializer);
    Factory<OffHeapBufferStorageEngine<String, String>> storageEngineFactory = OffHeapBufferStorageEngine.createFactory(PointerSize.INT, pageSource, configuration.getInitialSegmentTableSize(), keyPortability, elementPortability, false, true);
    predicate = new TestPredicate<Map.Entry<String, String>>();
    evictionListener = mock(EhcacheSegmentFactory.EhcacheSegment.EvictionListener.class);
    segment = new EhcacheSegmentFactory.EhcacheSegment<String, String>(pageSource, storageEngineFactory.newInstance(), 1, predicate, evictionListener);
  }

  @AfterClass
  public static void tearDownClass() {
    segment.destroy();
  }

  @Before
  public void setup() {
    predicate.delegate = Predicates.none();
  }

  @After
  public void tearDown() {
    segment.clear();
  }

  @Test
  public void testComputeFunctionCalledWhenNoMapping() {
    String value = segment.compute("key", new BiFunction<String, String, String>() {
      @Override
      public String apply(String s, String s2) {
        return "value";
      }
    }, false);
    assertThat(value, is("value"));
    assertThat(segment.get("key"), is(value));
  }

  @Test
  public void testComputeFunctionReturnsSameNoPin() {
    segment.put("key", "value");
    String value = segment.compute("key", new BiFunction<String, String, String>() {
      @Override
      public String apply(String s, String s2) {
        return s2;
      }
    }, false);
    assertThat(value, is("value"));
    assertThat(segment.isPinned("key"), is(false));
  }

  @Test
  public void testComputeFunctionReturnsSamePins() {
    segment.put("key", "value");
    String value = segment.compute("key", new BiFunction<String, String, String>() {
      @Override
      public String apply(String s, String s2) {
        return s2;
      }
    }, true);
    assertThat(value, is("value"));
    assertThat(segment.isPinned("key"), is(true));
  }

  @Test
  public void testComputeFunctionReturnsSamePreservesPinWhenNoPin() {
    segment.putPinned("key", "value");
    String value = segment.compute("key", new BiFunction<String, String, String>() {
      @Override
      public String apply(String s, String s2) {
        return s2;
      }
    }, false);
    assertThat(value, is("value"));
    assertThat(segment.isPinned("key"), is(true));
  }

  @Test
  public void testComputeFunctionReturnsDifferentNoPin() {
    segment.put("key", "value");
    String value = segment.compute("key", new BiFunction<String, String, String>() {
      @Override
      public String apply(String s, String s2) {
        return "otherValue";
      }
    }, false);
    assertThat(value, is("otherValue"));
    assertThat(segment.isPinned("key"), is(false));
  }

  @Test
  public void testComputeFunctionReturnsDifferentPins() {
    segment.put("key", "value");
    String value = segment.compute("key", new BiFunction<String, String, String>() {
      @Override
      public String apply(String s, String s2) {
        return "otherValue";
      }
    }, true);
    assertThat(value, is("otherValue"));
    assertThat(segment.isPinned("key"), is(true));
  }

  @Test
  public void testComputeFunctionReturnsDifferentClearsPin() {
    segment.putPinned("key", "value");
    String value = segment.compute("key", new BiFunction<String, String, String>() {
      @Override
      public String apply(String s, String s2) {
        return "otherValue";
      }
    }, false);
    assertThat(value, is("otherValue"));
    assertThat(segment.isPinned("key"), is(false));
  }

  @Test
  public void testComputeFunctionReturnsNullRemoves() {
    segment.putPinned("key", "value");
    String value = segment.compute("key", new BiFunction<String, String, String>() {
      @Override
      public String apply(String s, String s2) {
        return null;
      }
    }, false);
    assertThat(value, nullValue());
    assertThat(segment.containsKey("key"), is(false));
  }

  @Test
  public void testComputeIfPresentNotCalledOnNotContainedKey() {
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
  }

  @Test
  public void testComputeIfPresentReturnsSameValue() {
    segment.put("key", "value");
    String value = segment.computeIfPresent("key", new BiFunction<String, String, String>() {
      @Override
      public String apply(String s, String s2) {
        return s2;
      }
    });
    assertThat(segment.get("key"), is(value));
  }

  @Test
  public void testComputeIfPresentReturnsDifferentValue() {
    segment.put("key", "value");
    String value = segment.computeIfPresent("key", new BiFunction<String, String, String>() {
      @Override
      public String apply(String s, String s2) {
        return "newValue";
      }
    });
    assertThat(segment.get("key"), is(value));
  }

  @Test
  public void testComputeIfPresentReturnsNullRemovesMapping() {
    segment.put("key", "value");
    String value = segment.computeIfPresent("key", new BiFunction<String, String, String>() {
      @Override
      public String apply(String s, String s2) {
        return null;
      }
    });
    assertThat(segment.containsKey("key"), is(false));
  }

  @Test
  public void testPutVetoedComputesMetadata() {
    predicate.delegate = new Predicate<Map.Entry<String, String>>() {
      @Override
      public boolean test(Map.Entry<String, String> argument) {
        return "vetoed".equals(argument.getKey());
      }
    };
    segment.put("vetoed", "value");
    assertThat(segment.getMetadata("vetoed") & EhcacheSegmentFactory.EhcacheSegment.VETOED, is(EhcacheSegmentFactory.EhcacheSegment.VETOED));
  }

  @Test
  public void testPutPinnedVetoedComputesMetadata() {
    predicate.delegate = new Predicate<Map.Entry<String, String>>() {
      @Override
      public boolean test(Map.Entry<String, String> argument) {
        return "vetoed".equals(argument.getKey());
      }
    };
    segment.putPinned("vetoed", "value");
    assertThat(segment.getMetadata("vetoed") & EhcacheSegmentFactory.EhcacheSegment.VETOED, is(EhcacheSegmentFactory.EhcacheSegment.VETOED));
  }

  @Test
  public void testVetoedPreventsEviction() {
    assertThat(segment.evictable(1), is(true));
    assertThat(segment.evictable(EhcacheSegmentFactory.EhcacheSegment.VETOED | 1), is(false));
  }

  @Test
  public void testEvictionFiresEvent() {
    segment.put("key", "value");
    segment.evict(segment.getEvictionIndex(), false);
    verify(evictionListener).onEviction("key", "value");
  }

  private static class TestPredicate<T> implements Predicate<T> {
    private Predicate<T> delegate = Predicates.none();

    @Override
    public boolean test(T argument) {
      return delegate.test(argument);
    }
  }

}