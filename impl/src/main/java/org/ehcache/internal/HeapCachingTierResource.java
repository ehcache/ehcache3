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
import org.ehcache.internal.cachingtier.CachingTier;
import org.ehcache.internal.cachingtier.CachingTierProvider;
import org.ehcache.internal.cachingtier.ClockEvictingHeapCachingTier;
import org.ehcache.internal.cachingtier.TieredCache;
import org.ehcache.internal.util.ServiceUtil;
import org.ehcache.spi.service.ServiceConfiguration;

import java.util.concurrent.Future;

import static org.ehcache.spi.ServiceProvider.findAmongst;
import static org.ehcache.spi.ServiceProvider.findSingletonAmongst;

/**
 * @author Alex Snaps
 */
public class HeapCachingTierResource implements CachingTierProvider {

  public static <K, V> Cache<K, V> wrapCacheWithCachingTierIfConfigured(final Cache<K, V> cache, final Class<K> keyClazz, final Class<V> valueClazz, final ServiceLocator serviceProvider, final ServiceConfiguration<?>[] config) {
    if(findAmongst(HeapResourceCacheConfiguration.class, config).size() > 0) {
      return new TieredCache<>(cache, keyClazz, valueClazz, serviceProvider, config);
    } else {
      return cache;
    }
  }

  @Override
  public <K, V> CachingTier<K> createCachingTier(Class<K> keyClazz, Class<V> valueClazz, ServiceLocator serviceProvider, ServiceConfiguration<?>... configs) {
    final HeapResourceCacheConfiguration config = findSingletonAmongst(HeapResourceCacheConfiguration.class, configs);
    return new ClockEvictingHeapCachingTier<>(config.getMaxOnHeapEntryCount());
  }

  @Override
  public void releaseCachingTier(CachingTier<?> resource) {
    //no-op
  }

  @Override
  public Future<?> start() {
    return ServiceUtil.completeFuture();
  }

  @Override
  public Future<?> stop() {
    return ServiceUtil.completeFuture();
  }
}
