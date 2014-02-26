/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 */

package org.ehcache.spi;

import org.ehcache.Cache;

/**
 * @author Chris Dennis
 * @author Alex Snaps
 */
public interface CacheProvider extends Service {

  <K, V> Cache<K, V> createCache(Class<K> keyClazz, Class<V> valueClazz, ServiceProvider serviceProvider, ServiceConfiguration<?>... config);

  void releaseCache(Cache<?, ?> resource);
}
