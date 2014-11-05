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

import org.ehcache.spi.cache.Store;
import org.ehcache.spi.service.ServiceConfiguration;

/**
 * The configured time {@link TimeSource} for a {@link Store} 
 */
public class TimeSourceConfiguration implements ServiceConfiguration<Store.Provider> {

  private final TimeSource timeSource;

  public TimeSourceConfiguration(TimeSource timeSource) {
    this.timeSource = timeSource;
  }
  
  @Override
  public Class<Store.Provider> getServiceType() {
    return Store.Provider.class;
  }

  public TimeSource getTimeSource() {
    return this.timeSource;
  }

}
