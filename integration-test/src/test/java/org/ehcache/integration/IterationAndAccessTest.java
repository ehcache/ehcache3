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
package org.ehcache.integration;

import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.config.Builder;
import org.ehcache.config.ResourcePools;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Arrays.asList;
import static org.ehcache.config.builders.CacheConfigurationBuilder.newCacheConfigurationBuilder;
import static org.ehcache.config.builders.CacheManagerBuilder.newCacheManagerBuilder;
import static org.ehcache.config.builders.ResourcePoolsBuilder.heap;
import static org.ehcache.config.units.MemoryUnit.MB;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

@RunWith(Parameterized.class)
public class IterationAndAccessTest {

  private static final int KEYS = 1000;

  @Parameterized.Parameters
  public static List<Object[]> resourceCombinations() {
    return asList(
      new Object[] {heap(2000)},
      new Object[] {heap(2000).offheap(16, MB)}
    );
  }

  private final Builder<ResourcePools> resources;

  public IterationAndAccessTest(Builder<ResourcePools> resources) {
    this.resources = resources;
  }

  @Test
  public void testIterationWithConcurrentMutationOnCache() throws InterruptedException {
    CacheManager manager = newCacheManagerBuilder().build(true);
    try {
      Cache<Integer, String> cache = manager.createCache("cache", newCacheConfigurationBuilder(Integer.class, String.class, resources));

      AtomicBoolean mutate = new AtomicBoolean(true);
      Runnable mutator = () -> {
        Random rndm = new Random(System.nanoTime());
        while (mutate.get()) {
          Integer put = rndm.nextInt(KEYS);
          cache.put(put, "value-" + put.toString());
          int remove = rndm.nextInt(KEYS);
          cache.remove(remove);
        }
      };

      Thread t = new Thread(mutator);
      t.start();
      try {
        while (checkIterator(cache) < KEYS >> 1);
      } finally {
        mutate.set(false);
        t.join();
      }
    } finally {
      manager.close();
    }
  }

  private int checkIterator(Cache<Integer, String> cache) {
    int count = 0;
    for (Cache.Entry<Integer, String> entry : cache) {
      try {
        assertThat(entry.getValue(), is("value-" + entry.getKey().toString()));
        count++;
      } catch (AssertionError e) {
        System.out.println("Entry Key:   " + entry.getKey());
        System.out.println("Entry Value: " + entry.getValue());
        System.out.println("Cache Value: " + cache.get(entry.getKey()));
        throw e;
      }
    }
    return count;
  }
}
