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

import org.ehcache.Cache;
import org.ehcache.spi.ServiceLocator;
import org.ehcache.spi.cache.CacheProvider;
import org.ehcache.spi.service.ServiceConfiguration;
import org.ehcache.internal.util.ServiceUtil;

import java.util.concurrent.Future;

import static org.ehcache.internal.HeapCachingTierResource.wrapCacheWithCachingTierIfConfigured;

/**
 *
 * @author Alex Snaps
 * @author Chris Dennis
 */
public class HeapResource implements CacheProvider {

  private volatile ServiceLocator serviceLocator;

  @Override
  public <K, V> Cache<K, V> createCache(Class<K> keyClazz, Class<V> valueClazz, ServiceConfiguration<?>... config) {
    return wrapCacheWithCachingTierIfConfigured(new HeapCache<K, V>(), keyClazz, valueClazz, serviceLocator, config);
  }

  @Override
  public void releaseCache(Cache<?, ?> resource) {
    //no-op
  }

  @Override
  public Future<?> start(final ServiceLocator serviceLocator) {
    this.serviceLocator = serviceLocator;
    return ServiceUtil.completeFuture();
  }

  @Override
  public Future<?> stop() {
    return ServiceUtil.completeFuture();
  }
}
