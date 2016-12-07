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


import java.util.List;
import java.util.stream.Collectors;
import org.ehcache.Cache;
import org.hamcrest.Matchers;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.Assert;
import org.terracotta.management.model.stats.ContextualStatistics;
import org.terracotta.management.model.stats.Sample;
import org.terracotta.management.model.stats.history.AverageHistory;
import org.terracotta.management.model.stats.history.DurationHistory;

@Ignore
public class ClusteredStatisticsLatencyTest extends AbstractClusteringManagementTest {

  @Test
  public void test() throws Exception {

    long cacheHitLatencyMin=0L;
    long cacheHitLatencyMax=0L;
    double cacheHitLatencyAvg=Double.NaN;

    long clusteredHitLatencyMin=0L;
    long clusteredHitLatencyMax=0L;
    double clusteredHitLatencyAvg=Double.NaN;

    long cacheMissLatencyMin=0L;
    long cacheMissLatencyMax=0L;
    double cacheMissLatencyAvg=Double.NaN;

    long clusteredMissLatencyMin=0L;
    long clusteredMissLatencyMax=0L;
    double clusteredMissLatencyAvg=Double.NaN;

    sendManagementCallOnClientToCollectStats("Cache:HitLatencyMinimum","Cache:HitLatencyMaximum","Cache:HitLatencyAverage",
                                             "Clustered:HitLatencyMinimum","Clustered:HitLatencyMaximum","Clustered:HitLatencyAverage",
                                             "Cache:MissLatencyMinimum","Cache:MissLatencyMaximum","Cache:MissLatencyAverage",
                                             "Clustered:MissLatencyMinimum","Clustered:MissLatencyMaximum","Clustered:MissLatencyAverage");
    Thread.sleep(25000);

    Cache<String, String> cache = cacheManager.getCache("dedicated-cache-1", String.class, String.class);
    cache.put("one", "val1");
    cache.put("two", "val2");

    cache.get("one");   //hit
    cache.get("two");   //hit

    cache.get("three"); //miss
    cache.get("four");  //miss


    //It may take several seconds for the sampled stat values to be available and correct.
    //In the meantime the default values will be available.
    //Thus let's loop until the correct value we are expecting is available.
    do {

      // get the stats (we are getting the primitive counter, not the sample history)
      List<ContextualStatistics> stats = waitForNextStats()
        .stream()
        .filter(statistics -> "dedicated-cache-1".equals(statistics.getContext().get("cacheName")))
        .collect(Collectors.toList());;

      for (ContextualStatistics stat : stats) {
        if (stat.getContext().get("cacheName") != null && stat.getContext().get("cacheName").equals("dedicated-cache-1")) {

          //Cache HIT stats
          cacheHitLatencyMin = getDurationHistorySampleValue(stat, "Cache:HitLatencyMinimum");
          cacheHitLatencyMax = getDurationHistorySampleValue(stat, "Cache:HitLatencyMaximum");
          cacheHitLatencyAvg = getAverageHistorySampleValue(stat, "Cache:HitLatencyAverage");

          //Clustered HIT stats
          clusteredHitLatencyMin = getDurationHistorySampleValue(stat, "Clustered:HitLatencyMinimum");
          clusteredHitLatencyMax = getDurationHistorySampleValue(stat, "Clustered:HitLatencyMaximum");
          clusteredHitLatencyAvg = getAverageHistorySampleValue(stat, "Clustered:HitLatencyAverage");

          //Cache MISS stats
          cacheMissLatencyMin = getDurationHistorySampleValue(stat, "Cache:MissLatencyMinimum");
          cacheMissLatencyMax = getDurationHistorySampleValue(stat, "Cache:MissLatencyMaximum");
          cacheMissLatencyAvg = getAverageHistorySampleValue(stat, "Cache:MissLatencyAverage");

          //Clustered MISS stats
          clusteredMissLatencyMin = getDurationHistorySampleValue(stat, "Clustered:MissLatencyMinimum");
          clusteredMissLatencyMax = getDurationHistorySampleValue(stat, "Clustered:MissLatencyMaximum");
          clusteredMissLatencyAvg = getAverageHistorySampleValue(stat, "Clustered:MissLatencyAverage");

        }
      }
    } while(!Thread.currentThread().isInterrupted() &&
            ((cacheHitLatencyMin == 0L) || (cacheHitLatencyMax == 0L) || Double.isNaN(cacheHitLatencyAvg) ||
            (clusteredHitLatencyMin == 0L) || (clusteredHitLatencyMax == 0L) || Double.isNaN(clusteredHitLatencyAvg) ||
            (cacheMissLatencyMin == 0L) || (cacheMissLatencyMax == 0L) || Double.isNaN(cacheMissLatencyAvg) ||
            (clusteredMissLatencyMin == 0L) || (clusteredMissLatencyMax == 0L) || Double.isNaN(clusteredMissLatencyAvg)));


    Assert.assertThat((double)cacheHitLatencyMin,Matchers.lessThanOrEqualTo(cacheHitLatencyAvg));
    Assert.assertThat((double)cacheHitLatencyMax,Matchers.greaterThanOrEqualTo(cacheHitLatencyAvg));
    Assert.assertThat((double)clusteredHitLatencyMin,Matchers.lessThanOrEqualTo(clusteredHitLatencyAvg));
    Assert.assertThat((double)clusteredHitLatencyMax,Matchers.greaterThanOrEqualTo(clusteredHitLatencyAvg));
    Assert.assertThat((double)cacheMissLatencyMin,Matchers.lessThanOrEqualTo(cacheMissLatencyAvg));
    Assert.assertThat((double)cacheMissLatencyMax,Matchers.greaterThanOrEqualTo(cacheMissLatencyAvg));
    Assert.assertThat((double)clusteredMissLatencyMin,Matchers.lessThanOrEqualTo(clusteredMissLatencyAvg));
    Assert.assertThat((double)clusteredMissLatencyMax,Matchers.greaterThanOrEqualTo(clusteredMissLatencyAvg));
  }

  private static long getDurationHistorySampleValue(ContextualStatistics stat, String statName) {
    Sample<Long>[] samplesCacheHitLatencyMin = stat.getStatistic(DurationHistory.class, statName).getValue();
    if(samplesCacheHitLatencyMin.length > 0 && samplesCacheHitLatencyMin[samplesCacheHitLatencyMin.length - 1].getValue() != null) {
        return samplesCacheHitLatencyMin[samplesCacheHitLatencyMin.length - 1].getValue();
    }

    return 0L;
  }

  private static double getAverageHistorySampleValue(ContextualStatistics stat, String statName) {
    Sample<Double>[] samplesCacheHitLatencyAvg = stat.getStatistic(AverageHistory.class, statName).getValue();
    if(samplesCacheHitLatencyAvg.length > 0 && !Double.isNaN(samplesCacheHitLatencyAvg[samplesCacheHitLatencyAvg.length - 1].getValue())) {
      return samplesCacheHitLatencyAvg[samplesCacheHitLatencyAvg.length - 1].getValue();
    }

    return Double.NaN;
  }

}
