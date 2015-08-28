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

package org.ehcache.config.event;

import org.ehcache.config.Builder;
import org.ehcache.events.CacheEventNotificationServiceConfiguration;

/**
 * @author rism
 */
public class CacheEventNotificationServiceConfigurationBuilder implements Builder<CacheEventNotificationServiceConfiguration> {
  private int numberOfEventProcessingQueues;

  private CacheEventNotificationServiceConfigurationBuilder(int numberOfEventProcessingQueues) {
    this.numberOfEventProcessingQueues = numberOfEventProcessingQueues;
  }

  public static CacheEventNotificationServiceConfigurationBuilder withEventProcessingQueueCount(int numberOfEventProcessingQueues) {
    if (numberOfEventProcessingQueues < 1) {
      throw new IllegalArgumentException("Unacceptable number of Event Processing Queues provided");
    }
    return new CacheEventNotificationServiceConfigurationBuilder(numberOfEventProcessingQueues);
  }

  public DefaultCacheEventNotificationServiceConfiguration build() {
    return new DefaultCacheEventNotificationServiceConfiguration(numberOfEventProcessingQueues);
  }
}
