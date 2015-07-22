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

package org.ehcache.internal;

import org.ehcache.spi.ServiceProvider;
import org.ehcache.spi.service.ServiceConfiguration;

/**
 * DefaultTimeSourceService
 */
public class DefaultTimeSourceService implements TimeSourceService {

  private final TimeSource timeSource;

  public DefaultTimeSourceService(TimeSourceConfiguration serviceConfiguration) {
    if (serviceConfiguration != null && serviceConfiguration.getTimeSource() != null) {
      timeSource = serviceConfiguration.getTimeSource();
    } else {
      timeSource = SystemTimeSource.INSTANCE;
    }
  }

  @Override
  public TimeSource getTimeSource() {
    return timeSource;
  }

  @Override
  public void start(ServiceConfiguration<?> config, ServiceProvider serviceProvider) {
    if (config != null) {
      if (((TimeSourceConfiguration)config).getTimeSource() != timeSource) {
        throw new IllegalArgumentException("Got a different configuration on start than on create");
      }
    }
  }

  @Override
  public void stop() {
    // no-op
  }
}
