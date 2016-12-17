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
package org.ehcache.management.config;

import org.terracotta.management.registry.ManagementProvider;

import java.util.concurrent.TimeUnit;

public interface StatisticsProviderConfiguration {

  long averageWindowDuration();
  TimeUnit averageWindowUnit();

  int historySize();
  long historyInterval();
  TimeUnit historyIntervalUnit();

  long timeToDisable();
  TimeUnit timeToDisableUnit();


  Class<? extends ManagementProvider> getStatisticsProviderType();

}
