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

import org.ehcache.config.EvictionAdvisor;
import org.ehcache.core.spi.function.BiFunction;
import org.ehcache.core.spi.function.Function;
import org.ehcache.impl.internal.store.offheap.factories.EhcacheSegmentFactory;
import org.junit.Test;

import java.io.IOException;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * AbstractEhcacheOffHeapBackingMapTest
 */
public abstract class AbstractEhcacheOffHeapBackingMapTest {
  protected abstract EhcacheOffHeapBackingMap<String, String> createTestSegment() throws IOException;

  protected abstract EhcacheOffHeapBackingMap<String, String> createTestSegment(EvictionAdvisor<? super String, ? super String> evictionPredicate) throws IOException;

  protected abstract void destroySegment(EhcacheOffHeapBackingMap<String, String> segment);

  protected abstract void putPinned(String key, String value, EhcacheOffHeapBackingMap<String, String> segment);

  protected abstract boolean isPinned(String key, EhcacheOffHeapBackingMap<String, String> segment);

  protected abstract int getMetadata(String key, int mask, EhcacheOffHeapBackingMap<String, String> segment);

  @Test
  public void testComputeFunctionCalledWhenNoMapping() throws Exception {
    EhcacheOffHeapBackingMap<String, String> segment = createTestSegment();
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
      destroySegment(segment);
    }
  }

  @Test
  public void testComputeFunctionReturnsSameNoPin() throws Exception {
    EhcacheOffHeapBackingMap<String, String> segment = createTestSegment();
    try {
      segment.put("key", "value");
      String value = segment.compute("key", new BiFunction<String, String, String>() {
        @Override
        public String apply(String s, String s2) {
          return s2;
        }
      }, false);
      assertThat(value, is("value"));
      assertThat(isPinned("key", segment), is(false));
    } finally {
      destroySegment(segment);
    }
  }

  @Test
  public void testComputeFunctionReturnsSamePins() throws Exception {
    EhcacheOffHeapBackingMap<String, String> segment = createTestSegment();
    try {
      segment.put("key", "value");
      String value = segment.compute("key", new BiFunction<String, String, String>() {
        @Override
        public String apply(String s, String s2) {
          return s2;
        }
      }, true);
      assertThat(value, is("value"));
      assertThat(isPinned("key", segment), is(true));
    } finally {
      destroySegment(segment);
    }
  }

  @Test
  public void testComputeFunctionReturnsSamePreservesPinWhenNoPin() throws Exception {
    EhcacheOffHeapBackingMap<String, String> segment = createTestSegment();
    try {
      putPinned("key", "value", segment);
      String value = segment.compute("key", new BiFunction<String, String, String>() {
        @Override
        public String apply(String s, String s2) {
          return s2;
        }
      }, false);
      assertThat(value, is("value"));
      assertThat(isPinned("key", segment), is(true));
    } finally {
      destroySegment(segment);
    }
  }

  @Test
  public void testComputeFunctionReturnsDifferentNoPin() throws Exception {
    EhcacheOffHeapBackingMap<String, String> segment = createTestSegment();
    try {
      segment.put("key", "value");
      String value = segment.compute("key", new BiFunction<String, String, String>() {
        @Override
        public String apply(String s, String s2) {
          return "otherValue";
        }
      }, false);
      assertThat(value, is("otherValue"));
      assertThat(isPinned("key", segment), is(false));
    } finally {
      destroySegment(segment);
    }
  }

  @Test
  public void testComputeFunctionReturnsDifferentPins() throws Exception {
    EhcacheOffHeapBackingMap<String, String> segment = createTestSegment();
    try {
      segment.put("key", "value");
      String value = segment.compute("key", new BiFunction<String, String, String>() {
        @Override
        public String apply(String s, String s2) {
          return "otherValue";
        }
      }, true);
      assertThat(value, is("otherValue"));
      assertThat(isPinned("key", segment), is(true));
    } finally {
      destroySegment(segment);
    }
  }

  @Test
  public void testComputeFunctionReturnsDifferentClearsPin() throws Exception {
    EhcacheOffHeapBackingMap<String, String> segment = createTestSegment();
    try {
      putPinned("key", "value", segment);
      String value = segment.compute("key", new BiFunction<String, String, String>() {
        @Override
        public String apply(String s, String s2) {
          return "otherValue";
        }
      }, false);
      assertThat(value, is("otherValue"));
      assertThat(isPinned("key", segment), is(false));
    } finally {
      destroySegment(segment);
    }
  }

  @Test
  public void testComputeFunctionReturnsNullRemoves() throws Exception {
    EhcacheOffHeapBackingMap<String, String> segment = createTestSegment();
    try {
      putPinned("key", "value", segment);
      String value = segment.compute("key", new BiFunction<String, String, String>() {
        @Override
        public String apply(String s, String s2) {
          return null;
        }
      }, false);
      assertThat(value, nullValue());
      assertThat(segment.containsKey("key"), is(false));
    } finally {
      destroySegment(segment);
    }
  }

  @Test
  public void testComputeIfPresentNotCalledOnNotContainedKey() throws Exception {
    EhcacheOffHeapBackingMap<String, String> segment = createTestSegment();
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
      destroySegment(segment);
    }
  }

  @Test
  public void testComputeIfPresentReturnsSameValue() throws Exception {
    EhcacheOffHeapBackingMap<String, String> segment = createTestSegment();
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
      destroySegment(segment);
    }
  }

  @Test
  public void testComputeIfPresentReturnsDifferentValue() throws Exception {
    EhcacheOffHeapBackingMap<String, String> segment = createTestSegment();
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
      destroySegment(segment);
    }
  }

  @Test
  public void testComputeIfPresentReturnsNullRemovesMapping() throws Exception {
    EhcacheOffHeapBackingMap<String, String> segment = createTestSegment();
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
      destroySegment(segment);
    }
  }

  @Test
  public void testComputeIfPinnedNoOpUnpinned() throws Exception {
    EhcacheOffHeapBackingMap<String, String> segment = createTestSegment();
    try {
      segment.put("key", "value");
      boolean result = segment.computeIfPinned("key", new BiFunction<String, String, String>() {
        @Override
        public String apply(String s, String s2) {
          fail("Method should not be invoked");
          return null;
        }
      }, new Function<String, Boolean>() {
        @Override
        public Boolean apply(String s) {
          fail("Method should not be invoked");
          return false;
        }
      });
      assertThat(isPinned("key", segment), is(false));
      assertThat(result, is(false));
    } finally {
      destroySegment(segment);
    }
  }

  @Test
  public void testComputeIfPinnedClearsMappingOnNullReturnWithPinningFalse() throws Exception {
    EhcacheOffHeapBackingMap<String, String> segment = createTestSegment();
    final String value = "value";
    try {
      putPinned("key", value, segment);
      boolean result = segment.computeIfPinned("key", new BiFunction<String, String, String>() {
        @Override
        public String apply(String s, String s2) {
          assertThat(s2, is(value));
          return null;
        }
      }, new Function<String, Boolean>() {
        @Override
        public Boolean apply(String s) {
          assertThat(s, is(value));
          return false;
        }
      });
      assertThat(segment.containsKey("key"), is(false));
      assertThat(result, is(true));
    } finally {
      destroySegment(segment);
    }
  }

  @Test
  public void testComputeIfPinnedClearsMappingOnNullReturnWithPinningTrue() throws Exception {
    EhcacheOffHeapBackingMap<String, String> segment = createTestSegment();
    final String value = "value";
    try {
      putPinned("key", value, segment);
      boolean result = segment.computeIfPinned("key", new BiFunction<String, String, String>() {
        @Override
        public String apply(String s, String s2) {
          assertThat(s2, is(value));
          return null;
        }
      }, new Function<String, Boolean>() {
        @Override
        public Boolean apply(String s) {
          assertThat(s, is(value));
          return true;
        }
      });
      assertThat(segment.containsKey("key"), is(false));
      assertThat(result, is(true));
    } finally {
      destroySegment(segment);
    }
  }

  @Test
  public void testComputeIfPinnedClearsPinWithoutChangingValue() throws Exception {
    EhcacheOffHeapBackingMap<String, String> segment = createTestSegment();
    final String value = "value";
    try {
      putPinned("key", value, segment);
      boolean result = segment.computeIfPinned("key", new BiFunction<String, String, String>() {
        @Override
        public String apply(String s, String s2) {
          assertThat(s2, is(value));
          return s2;
        }
      }, new Function<String, Boolean>() {
        @Override
        public Boolean apply(String s) {
          assertThat(s, is(value));
          return true;
        }
      });
      assertThat(isPinned("key", segment), is(false));
      assertThat(result, is(true));
    } finally {
      destroySegment(segment);
    }
  }

  @Test
  public void testComputeIfPinnedPreservesPinWithoutChangingValue() throws Exception {
    EhcacheOffHeapBackingMap<String, String> segment = createTestSegment();
    final String value = "value";
    try {
      putPinned("key", value, segment);
      boolean result = segment.computeIfPinned("key", new BiFunction<String, String, String>() {
        @Override
        public String apply(String s, String s2) {
          assertThat(s2, is(value));
          return s2;
        }
      }, new Function<String, Boolean>() {
        @Override
        public Boolean apply(String s) {
          assertThat(s, is(value));
          return false;
        }
      });
      assertThat(isPinned("key", segment), is(true));
      assertThat(result, is(false));
    } finally {
      destroySegment(segment);
    }
  }

  @Test
  public void testComputeIfPinnedReplacesValueUnpinnedWhenUnpinFunctionFalse() throws Exception {
    EhcacheOffHeapBackingMap<String, String> segment = createTestSegment();
    final String value = "value";
    final String newValue = "newValue";
    try {
      putPinned("key", value, segment);
      boolean result = segment.computeIfPinned("key", new BiFunction<String, String, String>() {
        @Override
        public String apply(String s, String s2) {
          assertThat(s2, is(value));
          return newValue;
        }
      }, new Function<String, Boolean>() {
        @Override
        public Boolean apply(String s) {
          assertThat(s, is(value));
          return false;
        }
      });
      assertThat(segment.get("key"), is(newValue));
      assertThat(isPinned("key", segment), is(false));
      assertThat(result, is(false));
    } finally {
      destroySegment(segment);
    }
  }

  @Test
  public void testComputeIfPinnedReplacesValueUnpinnedWhenUnpinFunctionTrue() throws Exception {
    EhcacheOffHeapBackingMap<String, String> segment = createTestSegment();
    final String value = "value";
    final String newValue = "newValue";
    try {
      putPinned("key", value, segment);
      boolean result = segment.computeIfPinned("key", new BiFunction<String, String, String>() {
        @Override
        public String apply(String s, String s2) {
          assertThat(s2, is(value));
          return newValue;
        }
      }, new Function<String, Boolean>() {
        @Override
        public Boolean apply(String s) {
          assertThat(s, is(value));
          return true;
        }
      });
      assertThat(segment.get("key"), is(newValue));
      assertThat(isPinned("key", segment), is(false));
      assertThat(result, is(false));
    } finally {
      destroySegment(segment);
    }
  }

  @Test
  public void testComputeIfPresentAndPinNoOpNoMapping() throws Exception {
    EhcacheOffHeapBackingMap<String, String> segment = createTestSegment();
    segment.computeIfPresentAndPin("key", new BiFunction<String, String, String>() {
      @Override
      public String apply(String s, String s2) {
        fail("Function should not be invoked");
        return null;
      }
    });
  }

  @Test
  public void testComputeIfPresentAndPinDoesPin() throws Exception {
    EhcacheOffHeapBackingMap<String, String> segment = createTestSegment();
    final String value = "value";
    segment.put("key", value);
    segment.computeIfPresentAndPin("key", new BiFunction<String, String, String>() {
      @Override
      public String apply(String s, String s2) {
        assertThat(s2, is(value));
        return value;
      }
    });
    assertThat(isPinned("key", segment), is(true));
  }

  @Test
  public void testComputeIfPresentAndPinPreservesPin() throws Exception {
    EhcacheOffHeapBackingMap<String, String> segment = createTestSegment();
    final String value = "value";
    putPinned("key", value, segment);
    segment.computeIfPresentAndPin("key", new BiFunction<String, String, String>() {
      @Override
      public String apply(String s, String s2) {
        assertThat(s2, is(value));
        return value;
      }
    });
    assertThat(isPinned("key", segment), is(true));
  }

  @Test
  public void testComputeIfPresentAndPinReplacesAndPins() throws Exception {
    EhcacheOffHeapBackingMap<String, String> segment = createTestSegment();
    final String value = "value";
    final String newValue = "newValue";
    segment.put("key", value);
    segment.computeIfPresentAndPin("key", new BiFunction<String, String, String>() {
      @Override
      public String apply(String s, String s2) {
        assertThat(s2, is(value));
        return newValue;
      }
    });
    assertThat(isPinned("key", segment), is(true));
    assertThat(segment.get("key"), is(newValue));
  }

  @Test
  public void testPutAdvicedAgainstEvictionComputesMetadata() throws Exception {
    EhcacheOffHeapBackingMap<String, String> segment = createTestSegment(new EvictionAdvisor<String, String>() {
      @Override
      public boolean adviseAgainstEviction(String key, String value) {
        return "please-do-not-evict-me".equals(key);
      }
    });
    try {
      segment.put("please-do-not-evict-me", "value");
      assertThat(getMetadata("please-do-not-evict-me", EhcacheSegmentFactory.EhcacheSegment.ADVISED_AGAINST_EVICTION, segment), is(EhcacheSegmentFactory.EhcacheSegment.ADVISED_AGAINST_EVICTION));
    } finally {
      destroySegment(segment);
    }
  }

  @Test
  public void testPutPinnedAdvicedAgainstEvictionComputesMetadata() throws Exception {
    EhcacheOffHeapBackingMap<String, String> segment = createTestSegment(new EvictionAdvisor<String, String>() {
      @Override
      public boolean adviseAgainstEviction(String key, String value) {
        return "please-do-not-evict-me".equals(key);
      }
    });
    try {
      putPinned("please-do-not-evict-me", "value", segment);
      assertThat(getMetadata("please-do-not-evict-me", EhcacheSegmentFactory.EhcacheSegment.ADVISED_AGAINST_EVICTION, segment), is(EhcacheSegmentFactory.EhcacheSegment.ADVISED_AGAINST_EVICTION));
    } finally {
      destroySegment(segment);
    }
  }
}
