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

import org.ehcache.Cache;
import org.hamcrest.collection.IsArray;
import org.junit.Test;
import org.terracotta.management.model.stats.ContextualStatistics;
import org.terracotta.management.model.stats.Sample;
import org.terracotta.management.model.stats.history.RatioHistory;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.array;
import static org.junit.Assert.assertThat;

public class ClusteredStatisticsRatioTest extends AbstractClusteringManagementTest {

  @Test
  public void ratioTest() throws Exception {
    String[] statNames = {"Cache:HitRatio", "Clustered:HitRatio", "Cache:MissRatio", "Clustered:MissRatio"};
    sendManagementCallOnClientToCollectStats(statNames);

    // When testing ratios, we need to wait for the first computation (we do not have any choice) to happen because ratio depends on 2 other sampled statistics.
    // If you do not wait, then you'll always get some NaN because the hits will be done within the 1st second, and the hits won't be done in the right "window".
    // A ratio is computed by dividing a rate with another rate. See CompoundOperationImpl.ratioOf().
    // And a rate is computed with values aggregated into a EventRateSimpleMovingAverage.
    // The call to EventRateSimpleMovingAverage.rateUsingSeconds() will return 0 during the fist second (until first computation did happen).
    // So the hits must be after the first second so that values get accumulated into the partitions of EventRateSimpleMovingAverage.

    // Also, we have to take in consideration that in clustered, there is a collector that is scheduled at 75% of the TTD to collect and send stats.
    // So the delay can be greater than just the duration of the first sampling.
    Thread.sleep(25000);

    Cache<String, String> cache = cacheManager.getCache("dedicated-cache-1", String.class, String.class);
    cache.put("one", "val1");
    cache.put("two", "val2");

    cache.get("one");   //hit
    cache.get("two");   //hit

    cache.get("three"); //miss
    cache.get("four");  //miss

    Double[] ratios = new Double[statNames.length];

    // it could be several seconds before the sampled stats could become available
    // let's try until we find the correct values
    do {

      // get the stats (we are getting the primitive counter, not the sample history)
      // only keep CM stats for the following checks
      List<ContextualStatistics> stats = waitForNextStats()
        .stream()
        .filter(statistics -> "dedicated-cache-1".equals(statistics.getContext().get("cacheName")))
        .collect(Collectors.toList());

      for (ContextualStatistics stat : stats) {
        for (int i = 0; i < statNames.length; i++) {
          String statName = statNames[i];
          Sample<Double>[] samples = stat.getStatistic(RatioHistory.class, statName).getValue();
          ratios[i] = samples.length > 0 ? samples[samples.length - 1].getValue() : 0d;
        }
      }
    } while (!Thread.currentThread().isInterrupted() && !Arrays.equals(ratios, new Double[]{.5d, .5d, .5d, .5d}));

    @SuppressWarnings("unchecked")
    IsArray<Double> array = array(equalTo(.5d), equalTo(.5d), equalTo(.5d), equalTo(.5d));
    assertThat(ratios, is(array));
  }

}
