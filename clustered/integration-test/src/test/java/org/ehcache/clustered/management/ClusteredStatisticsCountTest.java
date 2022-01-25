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
package org.ehcache.clustered.management;

import static org.hamcrest.CoreMatchers.is;

import java.util.List;
import org.ehcache.Cache;
import org.junit.Assert;
import org.junit.Test;
import org.terracotta.management.model.stats.ContextualStatistics;
import org.terracotta.management.model.stats.Sample;
import org.terracotta.management.model.stats.history.CounterHistory;

public class ClusteredStatisticsCountTest extends AbstractClusteringManagementTest {

  private static final long CACHE_HIT_COUNT = 2L;
  private static final long CLUSTERED_HIT_COUNT = 2L;
  private static final long CACHE_MISS_COUNT = 2L;
  private static final long CLUSTERED_MISS_COUNT = 2L;

  @Test
  public void countTest() throws Exception {
    sendManagementCallOnClientToCollectStats("Cache:HitCount","Clustered:HitCount","Cache:MissCount","Clustered:MissCount");

    Cache<String, String> cache = cacheManager.getCache("dedicated-cache-1", String.class, String.class);
    cache.put("one", "val1");
    cache.put("two", "val2");

    cache.get("one");   //hit
    cache.get("two");   //hit

    cache.get("three"); //miss
    cache.get("four");  //miss


    long cacheHitCount = 0;
    long clusteredHitCount = 0;
    long cacheMissCount = 0;
    long clusteredMissCount = 0;

    // it could be several seconds before the sampled stats could become available
    // let's try until we find the correct values
    do {

      // get the stats (we are getting the primitive counter, not the sample history)
      List<ContextualStatistics> stats = waitForNextStats();
      for (ContextualStatistics stat : stats) {
        if (stat.getContext().contains("cacheName") && stat.getContext().get("cacheName").equals("dedicated-cache-1")) {

          Sample<Long>[] samplesCacheHitCount = stat.getStatistic(CounterHistory.class, "Cache:HitCount").getValue();
          if(samplesCacheHitCount.length > 0) {
            cacheHitCount = samplesCacheHitCount[samplesCacheHitCount.length - 1].getValue();
          }

          Sample<Long>[] samplesClusteredHitCount = stat.getStatistic(CounterHistory.class, "Clustered:HitCount").getValue();
          if(samplesClusteredHitCount.length > 0) {
            clusteredHitCount = samplesClusteredHitCount[samplesClusteredHitCount.length - 1].getValue();
          }

          Sample<Long>[] samplesClusteredMissCount = stat.getStatistic(CounterHistory.class, "Clustered:MissCount").getValue();
          if(samplesClusteredMissCount.length > 0) {
            clusteredMissCount = samplesClusteredMissCount[samplesClusteredMissCount.length - 1].getValue();
          }

          Sample<Long>[] samplesCacheMissCount = stat.getStatistic(CounterHistory.class, "Cache:MissCount").getValue();
          if(samplesCacheMissCount.length > 0) {
            cacheMissCount = samplesCacheMissCount[samplesCacheMissCount.length - 1].getValue();
          }
        }
      }
    } while(!Thread.currentThread().isInterrupted() &&
            (cacheHitCount != CACHE_HIT_COUNT) && (clusteredHitCount != CLUSTERED_HIT_COUNT) &&
            (cacheMissCount != CACHE_MISS_COUNT) && (clusteredMissCount != CLUSTERED_MISS_COUNT));

    Assert.assertThat(cacheHitCount,is(CACHE_HIT_COUNT));
    Assert.assertThat(clusteredHitCount,is(CLUSTERED_HIT_COUNT));
    Assert.assertThat(cacheMissCount,is(CACHE_MISS_COUNT));
    Assert.assertThat(clusteredMissCount,is(CLUSTERED_MISS_COUNT));

  }



}
