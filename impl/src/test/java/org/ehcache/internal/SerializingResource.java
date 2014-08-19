/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 */

package org.ehcache.internal;

import org.ehcache.Cache;
import org.ehcache.internal.serialization.SerializationProvider;
import org.ehcache.internal.serialization.Serializer;
import org.ehcache.spi.ServiceLocator;
import org.ehcache.spi.cache.CacheProvider;
import org.ehcache.spi.service.ServiceConfiguration;

/**
 *
 * @author cdennis
 */
public class SerializingResource implements CacheProvider {

  private final ServiceLocator serviceLocator;

  public SerializingResource(final ServiceLocator serviceLocator) {this.serviceLocator = serviceLocator;}

  @Override
  public <K, V> Cache<K, V> createCache(Class<K> keyClazz, Class<V> valueClazz, ServiceConfiguration<?>... config) {
    SerializationProvider serialization = serviceLocator.findService(SerializationProvider.class);
    Serializer<V> serializer = serialization.createSerializer(valueClazz);
    return (Cache<K,V>) new SerializingCache(serializer);
  }

  @Override
  public void releaseCache(Cache<?, ?> resource) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void stop() {
    //no-op
  }
}
