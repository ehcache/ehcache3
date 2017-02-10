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
package org.ehcache.management;

import org.ehcache.management.config.StatisticsProviderConfiguration;
import org.ehcache.spi.service.Service;
import org.terracotta.management.model.notification.ContextualNotification;
import org.terracotta.management.model.stats.ContextualStatistics;

import java.util.Collection;

/**
 * Collector service installed in a cache manager
 * <p>
 * The collecting time is automatically calculated from {@link StatisticsProviderConfiguration#timeToDisable()}
 */
public interface CollectorService extends Service {

  interface Collector {

    Collector EMPTY = new Collector() {
      @Override
      public void onNotification(ContextualNotification notification) {

      }

      @Override
      public void onStatistics(Collection<ContextualStatistics> statistics) {

      }
    };

    void onNotification(ContextualNotification notification);

    void onStatistics(Collection<ContextualStatistics> statistics);
  }

}
