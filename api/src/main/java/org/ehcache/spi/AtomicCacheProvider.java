/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 */

package org.ehcache.spi;

import org.ehcache.AtomicCache;

/**
 * @author Alex Snaps
 */
public interface AtomicCacheProvider extends CacheProvider {

  @Override
  <K, V> AtomicCache<K, V> createCache(Class<K> keyClazz, Class<V> valueClazz, ServiceProvider serviceProvider, ServiceConfiguration<?>... config);

}
