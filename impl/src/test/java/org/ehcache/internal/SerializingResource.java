/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 */

package org.ehcache.internal;

import java.util.concurrent.Future;

import org.ehcache.Cache;
import org.ehcache.internal.serialization.SerializationProvider;
import org.ehcache.internal.serialization.Serializer;
import org.ehcache.spi.CacheProvider;
import org.ehcache.spi.ServiceConfiguration;
import org.ehcache.spi.ServiceProvider;
import org.ehcache.internal.util.ServiceUtil;

/**
 *
 * @author cdennis
 */
public class SerializingResource implements CacheProvider {

  @Override
  public <K, V> Cache<K, V> createCache(Class<K> keyClazz, Class<V> valueClazz, ServiceProvider serviceProvider, ServiceConfiguration<?>... config) {
    SerializationProvider serialization = serviceProvider.findService(SerializationProvider.class);
    Serializer<V> serializer = serialization.createSerializer(valueClazz, serviceProvider);
    return new SerializingCache(serializer);
  }

  @Override
  public void releaseCache(Cache<?, ?> resource) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public Future<?> start() {
    return ServiceUtil.completeFuture();
  }

  @Override
  public Future<?> stop() {
    return ServiceUtil.completeFuture();
  }
}
