/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 */

package org.ehcache.internal;

import org.ehcache.Cache;
import org.ehcache.spi.CacheProvider;
import org.ehcache.spi.ServiceConfiguration;
import org.ehcache.spi.ServiceProvider;
import org.ehcache.internal.util.ServiceUtil;

import java.util.concurrent.Future;

import static org.ehcache.internal.HeapCachingTierResource.wrapCacheWithCachingTierIfConfigured;

/**
 *
 * @author Alex Snaps
 * @author Chris Dennis
 */
public class HeapResource implements CacheProvider {

  @Override
  public <K, V> Cache<K, V> createCache(Class<K> keyClazz, Class<V> valueClazz, ServiceProvider serviceProvider, ServiceConfiguration<?>... config) {
    return wrapCacheWithCachingTierIfConfigured(new HeapCache<K, V>(), keyClazz, valueClazz, serviceProvider, config);
  }

  @Override
  public void releaseCache(Cache<?, ?> resource) {
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
