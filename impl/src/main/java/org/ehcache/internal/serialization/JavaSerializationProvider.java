/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 */
package org.ehcache.internal.serialization;

import java.io.Serializable;
import java.util.concurrent.Future;

import org.ehcache.internal.ServiceLocator;
import org.ehcache.spi.service.ServiceConfiguration;
import org.ehcache.spi.ServiceProvider;
import org.ehcache.internal.util.ServiceUtil;

/**
 *
 * @author cdennis
 */
public class JavaSerializationProvider implements SerializationProvider {

  @Override
  public <T> Serializer<T> createSerializer(Class<T> clazz, ServiceLocator serviceProvider, ServiceConfiguration<?>... config) {
    if (!Serializable.class.isAssignableFrom(clazz)) {
      throw new IllegalArgumentException();
    }
    return new JavaSerializer();
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
