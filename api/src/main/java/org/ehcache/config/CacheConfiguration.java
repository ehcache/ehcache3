/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 */

package org.ehcache.config;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import org.ehcache.spi.service.ServiceConfiguration;
import org.ehcache.spi.cache.CacheProvider;

/**
 * @author Alex Snaps
 */
public final class CacheConfiguration<K, V> {

  private final Class<K> keyType;
  private final Class<V> valueType;
  private final Collection<ServiceConfiguration<?>> serviceConfigurations;

  public CacheConfiguration(Class<K> keyType, Class<V> valueType, ServiceConfiguration<?>... serviceConfigurations) {
    this.keyType = keyType;
    this.valueType = valueType;
    this.serviceConfigurations = Collections.unmodifiableCollection(Arrays.asList(serviceConfigurations));
  }

  public Collection<ServiceConfiguration<?>> getServiceConfigurations() {
    return serviceConfigurations;
  }

  public Class<K> getKeyType() {
    return keyType;
  }

  public Class<V> getValueType() {
    return valueType;
  }
}
