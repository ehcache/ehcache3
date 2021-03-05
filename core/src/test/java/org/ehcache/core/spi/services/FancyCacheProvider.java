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

package org.ehcache.core.spi.services;

import org.ehcache.core.Ehcache;
import org.ehcache.spi.service.ServiceProvider;
import org.ehcache.core.spi.store.CacheProvider;
import org.ehcache.spi.service.Service;
import org.ehcache.spi.service.ServiceConfiguration;

/**
 * FancyCacheProvider
 */
public class FancyCacheProvider implements CacheProvider {

  public int startStopCounter = 0;

  @Override
  public <K, V> Ehcache<K, V> createCache(Class<K> keyClazz, Class<V> valueClazz, ServiceConfiguration<?, ?>... config) {
    return null;
  }

  @Override
  public void releaseCache(final Ehcache<?, ?> resource) {
    //
  }

  @Override
  public void start(final ServiceProvider<Service> serviceProvider) {
    ++startStopCounter;
  }

  @Override
  public void stop() {
    --startStopCounter;
  }
}
