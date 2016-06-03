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

package org.ehcache.impl.internal.store;

import org.ehcache.UserManagedCache;
import org.ehcache.config.units.MemoryUnit;
import org.junit.Test;

import java.util.Random;

import static org.ehcache.config.builders.ResourcePoolsBuilder.heap;
import static org.ehcache.config.builders.UserManagedCacheBuilder.newUserManagedCacheBuilder;
import static org.junit.Assert.assertNotNull;

/**
 * EmergencyValveTest
 */
public class EmergencyValveTest {

  @Test
  public void testEmergencyValve() throws Exception {
    UserManagedCache<Long, byte[]> cache = newUserManagedCacheBuilder(Long.class, byte[].class)
        .withResourcePools(heap(100).offheap(5, MemoryUnit.MB))
        .build(true);

    long seed = System.currentTimeMillis();
    Random random = new Random(seed);

    for (int i = 0; i < 100; i++) {
      long key = random.nextLong();
      cache.put(key, new byte[1024 * 1024]);
      assertNotNull("Not expecting null entry - seed is " + seed, cache.get(key));
    }
  }
}
