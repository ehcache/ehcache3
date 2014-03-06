package org.ehcache.spi.cache.tiering;

import org.ehcache.internal.ServiceLocator;
import org.ehcache.spi.cache.Store;
import org.ehcache.spi.service.ServiceConfiguration;

/**
 * @author Alex Snaps
 */
public interface AuthoritativeTier<K, V> extends Store<K, V> {

  // Add AuthoritativeTier methods here

  public interface Provider {

    <K, V> AuthoritativeTier<K, V> createAuthoritativeTier(Class<K> keyClazz, Class<V> valueClazz, ServiceLocator serviceProvider, ServiceConfiguration<?>... config);

    void releaseAuthoritativeTier(AuthoritativeTier<?, ?> resource);

  }

}
