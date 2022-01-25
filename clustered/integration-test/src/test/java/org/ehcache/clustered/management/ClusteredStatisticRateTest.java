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
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.ehcache.Cache;
import org.ehcache.management.config.DefaultStatisticsProviderConfiguration;
import org.ehcache.management.providers.statistics.EhcacheStatisticsProvider;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.terracotta.management.model.stats.ContextualStatistics;
import org.terracotta.management.model.stats.Sample;
import org.terracotta.management.model.stats.history.RateHistory;


public class ClusteredStatisticRateTest extends AbstractClusteringManagementTest {

  private static DefaultStatisticsProviderConfiguration config = new DefaultStatisticsProviderConfiguration(EhcacheStatisticsProvider.class);
  private static double HIT_RATE;
  private static double MISS_RATE;

  @BeforeClass
  public static void initSeconds() {
    long seconds;
    switch (config.averageWindowUnit()) {
      case SECONDS:
        seconds = config.averageWindowDuration();
        HIT_RATE = 2.0d / (double)seconds;
        MISS_RATE = 2.0d / (double)seconds;
        break;
      case MINUTES:
        seconds = TimeUnit.MINUTES.toSeconds(config.averageWindowDuration());
        HIT_RATE = 2.0d / (double)seconds;
        MISS_RATE = 2.0d / (double)seconds;
        break;
      case HOURS:
        seconds = TimeUnit.HOURS.toSeconds(config.averageWindowDuration());
        HIT_RATE = 2.0d / (double)seconds;
        MISS_RATE = 2.0d / (double)seconds;
      default:
        throw new IllegalArgumentException("invalid averageWindowUnit: " + config.averageWindowUnit() + " for unit test!  You can add this TimeUnit if neccessary");
    }
  }

  @Test
  public void test() throws Exception {

    double cacheHitRate = 0d;
    double clusteredHitRate = 0d;
    double cacheMissRate = 0d;
    double clusteredMissRate = 0d;

    sendManagementCallOnClientToCollectStats("Cache:HitRate", "Clustered:HitRate","Cache:MissRate","Clustered:MissRate");
    Thread.sleep(25000);

    Cache<String, String> cache = cacheManager.getCache("dedicated-cache-1", String.class, String.class);
    cache.put("one", "val1");
    cache.put("two", "val2");

    cache.get("one");   //hit
    cache.get("two");   //hit

    cache.get("three"); //miss
    cache.get("four");  //miss


    // it could be several seconds before the sampled stats could become available
    // let's try until we find the correct values
    do {

      // get the stats (we are getting the primitive counter, not the sample history)
      List<ContextualStatistics> stats = waitForNextStats()
        .stream()
        .filter(statistics -> "dedicated-cache-1".equals(statistics.getContext().get("cacheName")))
        .collect(Collectors.toList());;

      for (ContextualStatistics stat : stats) {
        if (stat.getContext().get("cacheName") != null && stat.getContext().get("cacheName").equals("dedicated-cache-1")) {

          //HIT stats
          Sample<Double>[] samplesCacheHitRate = stat.getStatistic(RateHistory.class, "Cache:HitRate").getValue();
          if(samplesCacheHitRate.length > 0) {
            cacheHitRate = samplesCacheHitRate[samplesCacheHitRate.length - 1].getValue();
          }

          Sample<Double>[] samplesClusteredHitRate = stat.getStatistic(RateHistory.class, "Clustered:HitRate").getValue();
          if(samplesClusteredHitRate.length > 0) {
            clusteredHitRate = samplesClusteredHitRate[samplesClusteredHitRate.length - 1].getValue();
          }

          //MISS stats
          Sample<Double>[] samplesCacheMissRate = stat.getStatistic(RateHistory.class, "Cache:MissRate").getValue();
          if(samplesCacheMissRate.length > 0) {
            cacheMissRate = samplesCacheMissRate[samplesCacheMissRate.length - 1].getValue();
          }

          Sample<Double>[] samplesClusteredMissRate = stat.getStatistic(RateHistory.class, "Clustered:MissRate").getValue();
          if(samplesClusteredMissRate.length > 0) {
            clusteredMissRate = samplesClusteredMissRate[samplesClusteredMissRate.length - 1].getValue();
          }
        }
      }
    } while(!Thread.currentThread().isInterrupted() &&
            (cacheHitRate == 0d) && (clusteredHitRate == 0d) &&
            (cacheMissRate == 0d) && (clusteredMissRate == 0d));

    Assert.assertThat(cacheHitRate,is(HIT_RATE));
    Assert.assertThat(clusteredHitRate,is(HIT_RATE));

    Assert.assertThat(cacheMissRate,is(MISS_RATE));
    Assert.assertThat(clusteredMissRate,is(MISS_RATE));
  }

}
