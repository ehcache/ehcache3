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

import org.ehcache.spi.ServiceProvider;
import org.ehcache.spi.service.ThreadPoolsService;
import org.ehcache.util.ThreadPoolUtil;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

/**
 * @author Ludovic Orban
 */
public class DefaultThreadPoolsService implements ThreadPoolsService {

  private volatile ScheduledExecutorService statisticsExecutor;
  private volatile ExecutorService eventsOrderedDeliveryExecutor;
  private volatile ExecutorService eventsUnorderedDeliveryExecutor;

  @Override
  public ScheduledExecutorService getStatisticsExecutor() {
    if (statisticsExecutor == null) {
      throw new IllegalStateException(getClass().getSimpleName() + " not started");
    }
    return statisticsExecutor;
  }

  @Override
  public ExecutorService getEventsOrderedDeliveryExecutor() {
    if (eventsOrderedDeliveryExecutor == null) {
      throw new IllegalStateException(getClass().getSimpleName() + " not started");
    }
    return eventsOrderedDeliveryExecutor;
  }

  @Override
  public ExecutorService getEventsUnorderedDeliveryExecutor() {
    if (eventsUnorderedDeliveryExecutor == null) {
      throw new IllegalStateException(getClass().getSimpleName() + " not started");
    }
    return eventsUnorderedDeliveryExecutor;
  }

  @Override
  public void start(final ServiceProvider serviceProvider) {
    this.statisticsExecutor = ThreadPoolUtil.createStatisticsExecutor();
    this.eventsOrderedDeliveryExecutor = ThreadPoolUtil.createEventsOrderedDeliveryExecutor();
    this.eventsUnorderedDeliveryExecutor = ThreadPoolUtil.createEventsUnorderedDeliveryExecutor();

  }

  @Override
  public void stop() {
    statisticsExecutor.shutdownNow();
    eventsOrderedDeliveryExecutor.shutdown();
    eventsUnorderedDeliveryExecutor.shutdown();
  }
}
