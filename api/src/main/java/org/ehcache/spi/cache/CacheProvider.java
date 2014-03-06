/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 */

package org.ehcache.spi.cache;

import org.ehcache.Cache;
import org.ehcache.internal.ServiceLocator;
import org.ehcache.spi.service.Service;
import org.ehcache.spi.service.ServiceConfiguration;

/**
 * @author Chris Dennis
 * @author Alex Snaps
 */
public interface CacheProvider extends Service {

  <K, V> Cache<K, V> createCache(Class<K> keyClazz, Class<V> valueClazz, ServiceLocator serviceProvider, ServiceConfiguration<?>... config);

  void releaseCache(Cache<?, ?> resource);
}
