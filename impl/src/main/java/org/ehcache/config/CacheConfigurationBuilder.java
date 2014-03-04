/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 */

package org.ehcache.config;

import org.ehcache.spi.ServiceConfiguration;
import org.ehcache.spi.CacheProvider;

import java.util.Collection;
import java.util.HashSet;

/**
 * @author Alex Snaps
 */
public class CacheConfigurationBuilder {

  private final Collection<ServiceConfiguration<?>> serviceConfigurations = new HashSet<>();

  public static CacheConfigurationBuilder newCacheConfigurationBuilder() {
    return new CacheConfigurationBuilder();
  }

  public CacheConfigurationBuilder addServiceConfig(ServiceConfiguration<?> configuration) {
    serviceConfigurations.add(configuration);
    return this;
  }

  public CacheConfigurationBuilder removeServiceConfig(ServiceConfiguration<?> configuration) {
    serviceConfigurations.remove(configuration);
    return this;
  }

  public CacheConfigurationBuilder clearAllServiceConfig() {
    serviceConfigurations.clear();
    return this;
  }

  public <K, V> CacheConfiguration<K, V> buildConfig(final Class<? extends CacheProvider> cacheType, Class<K> keyType, Class<V> valueType) {
    return new CacheConfiguration<>(cacheType, keyType, valueType, serviceConfigurations.toArray(
        new ServiceConfiguration<?>[serviceConfigurations.size()]));
  }

    public <K, V> CacheConfiguration<K, V> buildCacheConfig(Class<K> keyType, Class<V> valueType) {
    return buildConfig(CacheProvider.class, keyType, valueType);
  }
}
