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

package org.ehcache.internal.executor;

import org.ehcache.spi.service.StatisticsExecutorService;
import org.ehcache.spi.service.ServiceConfiguration;
import org.ehcache.util.StatisticsThreadPoolUtil;

import java.util.concurrent.ScheduledExecutorService;

/**
 * @author Ludovic Orban
 */
public class DefaultStatisticsExecutorService implements StatisticsExecutorService {

  private ScheduledExecutorService statisticsExecutor;

  @Override
  public ScheduledExecutorService getStatisticsExecutor() {
    if (statisticsExecutor == null) {
      throw new IllegalStateException(getClass().getSimpleName() + " not started");
    }
    return statisticsExecutor;
  }

  @Override
  public void start(ServiceConfiguration<?> config) {
    this.statisticsExecutor = StatisticsThreadPoolUtil.createStatisticsExecutor();
  }

  @Override
  public void stop() {
    statisticsExecutor.shutdownNow();
  }
}
