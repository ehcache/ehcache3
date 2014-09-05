/*
 * Copyright Terracotta, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.ehcache.internal;

import org.ehcache.Ehcache;
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
  public <K, V> Ehcache<K, V> createCache(Class<K> keyClazz, Class<V> valueClazz, ServiceConfiguration<?>... config) {
    SerializationProvider serialization = serviceLocator.findService(SerializationProvider.class);
    Serializer<V> serializer = serialization.createSerializer(valueClazz);
    return new SerializingCache(serializer);
  }

  @Override
  public void releaseCache(Ehcache<?, ?> resource) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void stop() {
    //no-op
  }
}
