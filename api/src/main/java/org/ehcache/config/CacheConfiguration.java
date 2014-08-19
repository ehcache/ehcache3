package org.ehcache.config;

import org.ehcache.spi.service.ServiceConfiguration;

import java.util.Collection;

/**
 * @author Alex Snaps
 */
public interface CacheConfiguration<K, V> {
  Collection<ServiceConfiguration<?>> getServiceConfigurations();

  Class<K> getKeyType();

  Class<V> getValueType();
}
