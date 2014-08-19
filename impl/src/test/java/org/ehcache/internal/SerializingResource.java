/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 */

package org.ehcache.internal;

import java.util.concurrent.Future;

import org.ehcache.Cache;
import org.ehcache.internal.serialization.SerializationProvider;
import org.ehcache.internal.serialization.Serializer;
import org.ehcache.spi.ServiceLocator;
import org.ehcache.spi.cache.CacheProvider;
import org.ehcache.spi.service.ServiceConfiguration;
import org.ehcache.internal.util.ServiceUtil;

/**
 *
 * @author cdennis
 */
public class SerializingResource implements CacheProvider {

  private volatile ServiceLocator serviceLocator;

  @Override
  public <K, V> Cache<K, V> createCache(Class<K> keyClazz, Class<V> valueClazz, ServiceConfiguration<?>... config) {
    SerializationProvider serialization = serviceLocator.findService(SerializationProvider.class);
    Serializer<V> serializer = serialization.createSerializer(valueClazz);
    return new SerializingCache(serializer);
  }

  @Override
  public void releaseCache(Cache<?, ?> resource) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public Future<?> start(final ServiceLocator serviceLocator) {
    this.serviceLocator = serviceLocator;
    return ServiceUtil.completeFuture();
  }

  @Override
  public Future<?> stop() {
    return ServiceUtil.completeFuture();
  }
}
