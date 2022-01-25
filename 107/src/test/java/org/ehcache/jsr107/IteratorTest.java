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
package org.ehcache.jsr107;

import org.ehcache.core.config.DefaultConfiguration;
import org.ehcache.core.spi.time.TimeSource;
import org.ehcache.impl.internal.TimeSourceConfiguration;
import org.junit.Test;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.Caching;
import javax.cache.configuration.MutableConfiguration;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ExpiryPolicy;
import java.net.URI;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

/**
 * @author Ludovic Orban
 */
public class IteratorTest {

  private static class TestTimeSource implements TimeSource {

    private long time = 0;

    @Override
    public long getTimeMillis() {
      return time;
    }

    private void advanceTime(long delta) {
      this.time += delta;
    }
  }

  @Test
  public void testIterateExpiredIsSkipped() throws Exception {
    EhcacheCachingProvider provider = (EhcacheCachingProvider) Caching.getCachingProvider();
    TestTimeSource testTimeSource = new TestTimeSource();
    TimeSourceConfiguration timeSourceConfiguration = new TimeSourceConfiguration(testTimeSource);
    CacheManager cacheManager = provider.getCacheManager(new URI("test://testIterateExpiredReturnsNull"), new DefaultConfiguration(getClass().getClassLoader(), timeSourceConfiguration));

    Cache<Number, CharSequence> testCache = cacheManager.createCache("testCache", new MutableConfiguration<Number, CharSequence>()
        .setExpiryPolicyFactory(() -> new ExpiryPolicy() {
          @Override
          public Duration getExpiryForCreation() {
            return Duration.ETERNAL;
          }

          @Override
          public Duration getExpiryForAccess() {
            return new Duration(TimeUnit.SECONDS, 1L);
          }

          @Override
          public Duration getExpiryForUpdate() {
            return Duration.ZERO;
          }
        })
        .setTypes(Number.class, CharSequence.class));

    testCache.put(1, "one");
    testCache.get(1);

    testTimeSource.advanceTime(1000);

    Iterator<Cache.Entry<Number, CharSequence>> iterator = testCache.iterator();
    assertThat(iterator.hasNext(), is(false));

    cacheManager.close();
  }

}
