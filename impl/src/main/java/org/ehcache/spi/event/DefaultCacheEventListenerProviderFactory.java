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

package org.ehcache.spi.event;

import org.ehcache.config.event.DefaultCacheEventListenerConfiguration;
import org.ehcache.config.event.DefaultCacheEventListenerProviderConfiguration;
import org.ehcache.event.CacheEventListenerProvider;
import org.ehcache.spi.service.ServiceConfiguration;
import org.ehcache.spi.service.ServiceFactory;

/**
 * @author rism
 */
public class DefaultCacheEventListenerProviderFactory implements ServiceFactory<CacheEventListenerProvider> {

  @Override
  public DefaultCacheEventListenerProvider create(ServiceConfiguration<CacheEventListenerProvider> configuration) {
    if (configuration instanceof DefaultCacheEventListenerConfiguration) {
      throw new IllegalArgumentException("DefaultCacheEventListenerConfiguration must not be provided at CacheManager level");
    }
    return new DefaultCacheEventListenerProvider((DefaultCacheEventListenerProviderConfiguration) configuration);
  }

  @Override
  public Class<CacheEventListenerProvider> getServiceType() {
    return CacheEventListenerProvider.class;
  }
}