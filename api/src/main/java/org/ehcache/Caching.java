/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 */

package org.ehcache;

import org.ehcache.config.CacheConfiguration;
import org.ehcache.spi.ServiceConfiguration;
import org.ehcache.spi.ServiceProvider;
import org.ehcache.spi.CacheProvider;

import java.util.Collection;

/**
 * @author Alex Snaps
 */
public class Caching {

  public static <K, V> Cache<K, V> createCache(final CacheConfiguration<K, V> config) {
    return createCache(new ServiceProvider(), config);
  }

  public static <K, V> Cache<K, V> createCache(final ServiceProvider serviceProvider, final CacheConfiguration<K, V> config) {
    CacheProvider cacheProvider = serviceProvider.findService(config.getCacheType());
    Class<K> keyType = config.getKeyType();
    Class<V> valueType = config.getValueType();
    Collection<ServiceConfiguration<?>> serviceConfigs = config.getServiceConfigurations();
    ServiceConfiguration<?>[] serviceConfigArray = serviceConfigs.toArray(new ServiceConfiguration[serviceConfigs.size()]);
    return cacheProvider.createCache(keyType, valueType, serviceProvider, serviceConfigArray);
  }
}
