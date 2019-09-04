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
package org.ehcache.management.providers.statistics;

import org.ehcache.core.statistics.LatencyHistogramConfiguration;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class LatencyHistogramConfigurationTest {

  @Test
  public void test() {
    LatencyHistogramConfiguration conf = LatencyHistogramConfiguration.DEFAULT;
    assertThat(conf.getPhi()).isEqualTo(LatencyHistogramConfiguration.DEFAULT_PHI);
    assertThat(conf.getBucketCount()).isEqualTo(LatencyHistogramConfiguration.DEFAULT_BUCKET_COUNT);
    assertThat(conf.getWindow()).isEqualTo(LatencyHistogramConfiguration.DEFAULT_WINDOW);
  }
}
