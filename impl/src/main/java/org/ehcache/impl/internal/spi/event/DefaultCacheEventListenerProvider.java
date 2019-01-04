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

package org.ehcache.impl.internal.spi.event;

import org.ehcache.event.CacheEventListener;
import org.ehcache.core.events.CacheEventListenerProvider;
import org.ehcache.impl.config.event.DefaultCacheEventListenerConfiguration;
import org.ehcache.impl.internal.classes.ClassInstanceProvider;
import org.ehcache.spi.service.ServiceConfiguration;

/**
 * @author rism
 */
public class DefaultCacheEventListenerProvider extends ClassInstanceProvider<String, DefaultCacheEventListenerConfiguration, CacheEventListener<?, ?>> implements CacheEventListenerProvider {

  public DefaultCacheEventListenerProvider() {
    super(null, DefaultCacheEventListenerConfiguration.class);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <K, V> CacheEventListener<K, V> createEventListener(String alias, ServiceConfiguration<CacheEventListenerProvider> serviceConfiguration) {
    return (CacheEventListener<K, V>) newInstance(alias, serviceConfiguration);
  }

  @Override
  public void releaseEventListener(CacheEventListener<?, ?> cacheEventListener) throws Exception {
    releaseInstance(cacheEventListener);
  }
}
