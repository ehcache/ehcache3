/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 */
package org.ehcache.internal.serialization;

import org.ehcache.internal.ServiceLocator;
import org.ehcache.spi.service.Service;
import org.ehcache.spi.service.ServiceConfiguration;
import org.ehcache.spi.ServiceProvider;

/**
 *
 * @author cdennis
 */
public interface SerializationProvider extends Service {
 
  <T> Serializer<T> createSerializer(Class<T> clazz, ServiceLocator serviceProvider, ServiceConfiguration<?> ... config);
}
