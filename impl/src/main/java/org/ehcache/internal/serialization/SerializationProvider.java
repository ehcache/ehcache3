/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 */
package org.ehcache.internal.serialization;

import org.ehcache.spi.Service;
import org.ehcache.spi.ServiceConfiguration;
import org.ehcache.spi.ServiceProvider;

/**
 *
 * @author cdennis
 */
public interface SerializationProvider extends Service {
 
  <T> Serializer<T> createSerializer(Class<T> clazz, ServiceProvider serviceProvider, ServiceConfiguration<?> ... config);
}
