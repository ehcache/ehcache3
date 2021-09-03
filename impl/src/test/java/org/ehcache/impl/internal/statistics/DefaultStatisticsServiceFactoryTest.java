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

import java.util.concurrent.TimeUnit;

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
    assertThat(statisticsService.getConfiguration().getLatencyHistorySize()).isEqualTo(configuration.getLatencyHistorySize());
    assertThat(statisticsService.getConfiguration().getLatencyHistoryWindowInterval()).isEqualTo(configuration.getLatencyHistoryWindowInterval());
    assertThat(statisticsService.getConfiguration().getLatencyHistoryWindowUnit()).isEqualTo(configuration.getLatencyHistoryWindowUnit());
  }

  @Test
  public void createWithConfig() {
    StatisticsService statisticsService = factory.create(new DefaultStatisticsServiceConfiguration()
      .withLatencyHistorySize(1)
      .withLatencyHistoryWindow(1, TimeUnit.SECONDS));
    assertThat(statisticsService.getConfiguration().getLatencyHistorySize()).isEqualTo(1);
    assertThat(statisticsService.getConfiguration().getLatencyHistoryWindowInterval()).isEqualTo(1L);
    assertThat(statisticsService.getConfiguration().getLatencyHistoryWindowUnit()).isEqualTo(TimeUnit.SECONDS);
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
