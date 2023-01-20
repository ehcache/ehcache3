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
package org.ehcache.spi.serialization;

import org.ehcache.spi.service.Service;
import org.ehcache.spi.service.ServiceConfiguration;

/**
 * A {@link Service} providing {@link Serializer} instances.
 * <p>
 * The {@code CacheManager} {@link org.ehcache.spi.service.ServiceProvider obtains} an instance of this
 * {@code Service} prior to creating any {@code Cache} instances.  Before creating each {@code Cache}
 * instance, the {@code CacheManager} calls the
 * {@link #createKeySerializer(Class, ClassLoader, ServiceConfiguration[])} and
 * {@link #createValueSerializer(Class, ClassLoader, ServiceConfiguration[])} methods to obtain
 * {@code Serializer} instances for the {@code Cache}, either through explicit configuration or from
 * {@code CacheManager} level configuration.
 * <p>
 * Some {@code Cache} configurations make serialization mandatory.  If serialization is mandatory,
 * failure to return a {@code Serializer} from a {@code SerializationProvider}
 * results in a {@code Cache} initialization failure.  For a {@code Cache} in which serialization
 * is not mandatory, failing to return a {@code Serializer} will not cause {@code Cache} initialization
 * failure.
 */
public interface SerializationProvider extends Service {

  /**
   * Creates a key {@link Serializer} with the given parameters.
   *
   * @param clazz the class of the type to serialize to/from
   * @param classLoader the classloader used to load classes during deserialization; may be {@code null}
   * @param configs the {@link ServiceConfiguration} instances available through the
   *                {@link org.ehcache.CacheManager CacheManager}
   * @param <T> the type serialized to serialize to/from
   *
   * @return a {@code Serializer} instance, possibly {@code null}
   *
   * @throws UnsupportedTypeException if a serializer cannot be created for the given type
   */
  <T> Serializer<T> createKeySerializer(Class<T> clazz, ClassLoader classLoader, ServiceConfiguration<?, ?>... configs) throws UnsupportedTypeException;

  /**
   * Creates a value {@link Serializer} with the given parameters.
   *
   * @param clazz the class of the type to serialize to/from
   * @param classLoader the classloader used to load classes during deserialization; may be {@code null}
   * @param configs the {@link ServiceConfiguration} instances available through the
   *                {@link org.ehcache.CacheManager CacheManager}
   * @param <T> the type serialized to serialize to/from
   *
   * @return a {@code Serializer} instance, possibly {@code null}
   *
   * @throws UnsupportedTypeException if a serializer cannot be created for the given type
   */
  <T> Serializer<T> createValueSerializer(Class<T> clazz, ClassLoader classLoader, ServiceConfiguration<?, ?>... configs) throws UnsupportedTypeException;

  /**
   * Releases the given {@link Serializer} instance.
   * If the serializer is obtained from a {@code SerializationProvider} and implements
   * {@link java.io.Closeable Closeable}, the {@link java.io.Closeable#close() close} method is invoked.
   *
   * @param serializer the serializer to be released
   *
   * @throws Exception when the release fails
   */
  void releaseSerializer(Serializer<?> serializer) throws Exception;
}
