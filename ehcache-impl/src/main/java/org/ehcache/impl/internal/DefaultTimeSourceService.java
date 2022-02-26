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

import org.ehcache.core.spi.time.SystemTimeSource;
import org.ehcache.core.spi.time.TickingTimeSource;
import org.ehcache.core.spi.time.TimeSource;
import org.ehcache.core.spi.time.TimeSourceService;
import org.ehcache.spi.service.ServiceProvider;
import org.ehcache.spi.service.Service;

/**
 * DefaultTimeSourceService
 */
public class DefaultTimeSourceService implements TimeSourceService {

  private final TimeSource timeSource;

  public DefaultTimeSourceService(TimeSourceConfiguration config) {
    if (config != null) {
      timeSource = config.getTimeSource();
    } else {
      timeSource = SystemTimeSource.INSTANCE;
    }
  }

  @Override
  public TimeSource getTimeSource() {
    return timeSource;
  }

  @Override
  public void start(ServiceProvider<Service> serviceProvider) {
    if (timeSource instanceof Service) {
      ((Service) timeSource).start(serviceProvider);
    }
  }

  @Override
  public void stop() {
    if (timeSource instanceof Service) {
      ((Service) timeSource).stop();
    }
  }
}
