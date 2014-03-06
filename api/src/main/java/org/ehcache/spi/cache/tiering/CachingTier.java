package org.ehcache.spi.cache.tiering;

import org.ehcache.internal.ServiceLocator;
import org.ehcache.spi.service.ServiceConfiguration;

/**
 * @author Alex Snaps
 */
public interface CachingTier<K> {

  // Add CachingTier methods

  public interface Provider {

    <K> CachingTier<K> createCachingTier(Class<K> keyClazz, ServiceLocator serviceProvider, ServiceConfiguration<?>... config);

    void releaseCachingTier(CachingTier<?> resource);

  }
}
