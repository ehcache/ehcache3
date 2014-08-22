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

package org.ehcache.internal;

import org.junit.Test;
import org.ehcache.internal.cachingtier.ClockEvictingHeapCachingTier;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

/**
 * @author Alex Snaps
 */
public class ClockEvictingHeapCachingTierTest {

  @Test
  public void testEvictsWhenAtCapacity() {
    final long maximumSize = 4000;
    final long hotStart = maximumSize + 1000;
    final long hotEnd = maximumSize + 1500;

    ClockEvictingHeapCachingTier<String> cache = new ClockEvictingHeapCachingTier<String>(maximumSize);

    for(long i = 0; i < maximumSize * 4; i++) {
      for (long j = hotStart; j < hotEnd; j++) {
        cache.get("key" + j);
      }
      final String key = "key" + i;
      assertThat(cache.putIfAbsent(key, i), nullValue());
    }

    assertThat(cache.size(), is(maximumSize));

    for (long j = hotStart; j < hotEnd; j++) {
      final String key = "key" + j;
      assertThat("Hot '" + key + "' missing!", (Long)cache.get(key), equalTo(j));
    }
  }

  @Test
  public void testReplacesAlright() {
    ClockEvictingHeapCachingTier<String> cache = new ClockEvictingHeapCachingTier<String>(10);
    assertThat(cache.putIfAbsent("key", 1), nullValue());
    assertThat(cache.replace("key", 2, 1), is(false));
    assertThat(cache.get("key"), is((Object) 1));
    assertThat(cache.replace("key", 1, 2), is(true));
    assertThat(cache.get("key"), is((Object) 2));
  }

  @Test
  public void testRemovesAlright() {
    ClockEvictingHeapCachingTier<String> cache = new ClockEvictingHeapCachingTier<String>(10);
    assertThat(cache.putIfAbsent("key", 1), nullValue());
    cache.remove("key", 2);
    assertThat(cache.get("key"), is((Object) 1));
    cache.remove("key", 1);
    assertThat(cache.get("key"), nullValue());
  }
}
