/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 */

package org.ehcache.internal.cachingtier;

import org.ehcache.spi.Service;
import org.ehcache.spi.ServiceConfiguration;
import org.ehcache.spi.ServiceProvider;

/**
 *
 * @author cdennis
 */
public interface CachingTierProvider extends Service {
  
  <K, V> CachingTier<K> createCachingTier(Class<K> keyClazz, Class<V> valueClazz, ServiceProvider serviceProvider, ServiceConfiguration<?>... config);

  void releaseCachingTier(CachingTier<?> resource);
}
