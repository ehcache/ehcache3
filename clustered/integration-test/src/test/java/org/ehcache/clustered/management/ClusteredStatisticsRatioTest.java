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

import static org.ehcache.clustered.management.AbstractClusteringManagementTest.cacheManager;
import static org.ehcache.clustered.management.AbstractClusteringManagementTest.sendManagementCallToCollectStats;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.List;
import org.ehcache.Cache;
import org.junit.Assert;
import org.junit.Test;
import org.terracotta.management.model.call.ContextualReturn;
import org.terracotta.management.model.stats.ContextualStatistics;
import org.terracotta.management.model.stats.Sample;
import org.terracotta.management.model.stats.history.RatioHistory;

public class ClusteredStatisticsRatioTest extends AbstractClusteringManagementTest {

  private static final double CACHE_HIT_RATIO = .5d;
  private static final double CLUSTERED_HIT_RATIO = .5d;
  private static final double CACHE_MISS_RATIO = .5d;
  private static final double CLUSTERED_MISS_RATIO = .5d;

  @Test
  public void ratioTest() throws Exception {
    ContextualReturn<?> contextualReturn = sendManagementCallToCollectStats("Cache:HitRatio","Clustered:HitRatio","Cache:MissRatio","Clustered:MissRatio");
    assertThat(contextualReturn.hasExecuted(), is(true));

    Cache<String, String> cache = cacheManager.getCache("dedicated-cache-1", String.class, String.class);
    cache.put("one", "val1");
    cache.put("two", "val2");

    cache.get("one");   //hit
    cache.get("two");   //hit

    cache.get("three"); //miss
    cache.get("four");  //miss


    double cacheHitRatio = 0;
    double clusteredHitRatio = 0;
    double cacheMissRatio = 0;
    double clusteredMissRatio = 0;

    // it could be several seconds before the sampled stats could become available
    // let's try until we find the correct values
    do {

      // get the stats (we are getting the primitive counter, not the sample history)
      List<ContextualStatistics> stats = waitForNextStats();
      for (ContextualStatistics stat : stats) {
        if (stat.getContext().get("cacheName").equals("dedicated-cache-1")) {

          Sample<Double>[] samplesCacheHitRatio = stat.getStatistic(RatioHistory.class, "Cache:HitRatio").getValue();
          if(samplesCacheHitRatio.length > 0) {
            cacheHitRatio = samplesCacheHitRatio[samplesCacheHitRatio.length - 1].getValue();
          }

          Sample<Double>[] samplesClusteredHitRatio = stat.getStatistic(RatioHistory.class, "Clustered:HitRatio").getValue();
          if(samplesClusteredHitRatio.length > 0) {
            clusteredHitRatio = samplesClusteredHitRatio[samplesClusteredHitRatio.length - 1].getValue();
          }

          Sample<Double>[] samplesClusteredMissRatio = stat.getStatistic(RatioHistory.class, "Clustered:MissRatio").getValue();
          if(samplesClusteredMissRatio.length > 0) {
            clusteredMissRatio = samplesClusteredMissRatio[samplesClusteredMissRatio.length - 1].getValue();
          }

          Sample<Double>[] samplesCacheMissRatio = stat.getStatistic(RatioHistory.class, "Cache:MissRatio").getValue();
          if(samplesCacheMissRatio.length > 0) {
            cacheMissRatio = samplesCacheMissRatio[samplesCacheMissRatio.length - 1].getValue();
          }
        }
      }
    } while( (cacheHitRatio != CACHE_HIT_RATIO) && (clusteredHitRatio != CLUSTERED_HIT_RATIO) &&
            (cacheMissRatio != CACHE_MISS_RATIO) && (clusteredMissRatio != CLUSTERED_MISS_RATIO));

    Assert.assertThat(cacheHitRatio,is(CACHE_HIT_RATIO));
    Assert.assertThat(clusteredHitRatio,is(CLUSTERED_HIT_RATIO));
    Assert.assertThat(cacheMissRatio,is(CACHE_MISS_RATIO));
    Assert.assertThat(clusteredMissRatio,is(CLUSTERED_MISS_RATIO));

  }



}
