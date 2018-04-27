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
package org.ehcache.impl.internal.statistics;

import org.ehcache.core.spi.service.StatisticsService;
import org.ehcache.core.spi.service.StatisticsServiceConfiguration;
import org.junit.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Mathieu Carbou
 */
public class DefaultStatisticsServiceFactoryTest {

  private final DefaultStatisticsServiceFactory factory = new DefaultStatisticsServiceFactory();

  @Test
  public void createNoConfig() {
    StatisticsServiceConfiguration configuration = new DefaultStatisticsServiceConfiguration();
    StatisticsService statisticsService = factory.create(null);
    assertThat(statisticsService.getConfiguration().getDefaultHistogramWindow()).isEqualTo(configuration.getDefaultHistogramWindow());
  }

  @Test
  public void createWithConfig() {
    StatisticsService statisticsService = factory.create(new DefaultStatisticsServiceConfiguration()
      .withDefaultHistogramWindow(Duration.ofHours(1)));
    assertThat(statisticsService.getConfiguration().getDefaultHistogramWindow()).isEqualTo(Duration.ofHours(1));
  }

  @Test(expected = IllegalArgumentException.class)
  public void createWithBadConfig() {
    factory.create(() -> StatisticsService.class);
  }

  @Test
  public void getServiceType() {
    assertThat(factory.getServiceType()).isEqualTo(StatisticsService.class);
  }
}
