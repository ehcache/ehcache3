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
import org.ehcache.expiry.Duration;
import org.ehcache.expiry.Expirations;
import org.ehcache.expiry.Expiry;
import org.junit.Test;

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
        .evitionVeto(new EvictionVeto<Long, String>() {
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
}
