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

package org.ehcache.config;

import org.ehcache.Cache;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.expiry.Duration;
import org.ehcache.expiry.Expirations;
import org.ehcache.expiry.Expiry;
import org.hamcrest.Matchers;
import org.junit.Test;

import static org.junit.Assert.assertThat;

public class CacheConfigurationBuilderTest {

  @Test
  public void testNothing() {
    final CacheConfigurationBuilder<Long, CharSequence> builder = CacheConfigurationBuilder.newCacheConfigurationBuilder();
   
    builder.usingEvictionPrioritizer(Eviction.Prioritizer.LFU)
        .buildConfig(Long.class, CharSequence.class);

    final EvictionPrioritizer<Long, CharSequence> prioritizer = new EvictionPrioritizer<Long, CharSequence>() {
      @Override
      public int compare(final Cache.Entry<Long, CharSequence> o1, final Cache.Entry<Long, CharSequence> o2) {
        return Integer.signum(o2.getValue().length() - o1.getValue().length());
      }
    };

    final Expiry<Object, Object> expiry = Expirations.timeToIdleExpiration(Duration.FOREVER);

    builder
        .evictionVeto(new EvictionVeto<Long, String>() {
          @Override
          public boolean test(final Cache.Entry<Long, String> argument) {
            return argument.getValue().startsWith("A");
          }
        })
        .usingEvictionPrioritizer(prioritizer)
        .withExpiry(expiry)
        .buildConfig(Long.class, String.class);
    builder
        .buildConfig(Long.class, String.class, null, prioritizer);
    builder
        .buildConfig(Long.class, String.class, null, Eviction.Prioritizer.FIFO);
  }

  @Test
  public void testOffheapGetsAddedToCacheConfiguration() {
    CacheConfigurationBuilder<Long, CharSequence> builder = CacheConfigurationBuilder.newCacheConfigurationBuilder();

    final EvictionPrioritizer<Long, CharSequence> prioritizer = new EvictionPrioritizer<Long, CharSequence>() {
      @Override
      public int compare(final Cache.Entry<Long, CharSequence> o1, final Cache.Entry<Long, CharSequence> o2) {
        return Integer.signum(o2.getValue().length() - o1.getValue().length());
      }
    };

    final Expiry<Object, Object> expiry = Expirations.timeToIdleExpiration(Duration.FOREVER);

    builder = builder.withResourcePools(ResourcePoolsBuilder.newResourcePoolsBuilder().heap(10, EntryUnit.ENTRIES)
        .offheap(10, MemoryUnit.MB));
    CacheConfiguration config = builder
        .evictionVeto(new EvictionVeto<Long, String>() {
          @Override
          public boolean test(final Cache.Entry<Long, String> argument) {
            return argument.getValue().startsWith("A");
          }
        })
        .usingEvictionPrioritizer(prioritizer)
        .withExpiry(expiry)
        .buildConfig(Long.class, String.class);
    assertThat(config.getResourcePools().getPoolForResource(ResourceType.Core.OFFHEAP).getType(), Matchers.<ResourceType>is(ResourceType.Core.OFFHEAP));
    assertThat(config.getResourcePools().getPoolForResource(ResourceType.Core.OFFHEAP).getUnit(), Matchers.<ResourceUnit>is(MemoryUnit.MB));
  }
}
