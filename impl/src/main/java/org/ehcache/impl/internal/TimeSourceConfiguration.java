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
package org.ehcache.impl.internal;

import org.ehcache.core.spi.time.TimeSource;
import org.ehcache.core.spi.time.TimeSourceService;
import org.ehcache.spi.service.ServiceCreationConfiguration;

/**
 * Configuration for the {@link TimeSourceService}
 *
 * This configuration has to be applied at the {@link org.ehcache.CacheManager} level.
 */
public class TimeSourceConfiguration implements ServiceCreationConfiguration<TimeSourceService, TimeSource> {

  private final TimeSource timeSource;

  /**
   * Constructor for this configuration object which takes the {@link TimeSource} to use.
   *
   * @param timeSource the {@code TimeSource} to use
   */
  public TimeSourceConfiguration(TimeSource timeSource) {
    this.timeSource = timeSource;
  }

  @Override
  public Class<TimeSourceService> getServiceType() {
    return TimeSourceService.class;
  }

  /**
   * Exposes the {@link TimeSource} configured.
   *
   * @return the {@code TimeSource}
   */
  public TimeSource getTimeSource() {
    return this.timeSource;
  }

  @Override
  public TimeSource derive() {
    return getTimeSource();
  }

  @Override
  public TimeSourceConfiguration build(TimeSource timeSource) {
    return new TimeSourceConfiguration(timeSource);
  }
}
