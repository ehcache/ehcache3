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
 * Contract to provide a {@link Serializer} for a given type.
 *
 * @author cdennis
 */
public interface SerializationProvider extends Service {

  /**
   * Creates a key {@link Serializer} with the given parameters.
   *
   * @param clazz the class of the type to serialize to/from
   * @param classLoader the classloader to use
   * @param configs specific configuration
   * @param <T> the type serialized to serialize to/from
   * @return a {@code Serializer} instance
   * @throws UnsupportedTypeException if a serializer cannot be created for the given type
   */
  <T> Serializer<T> createKeySerializer(Class<T> clazz, ClassLoader classLoader, ServiceConfiguration<?>... configs) throws UnsupportedTypeException;

  /**
   * Creates a value {@link Serializer} with the given parameters.
   *
   * @param clazz the class of the type to serialize to/from
   * @param classLoader the classloader to use
   * @param configs specific configuration
   * @param <T> the type serialized to serialize to/from
   * @return a {@code Serializer} instance
   * @throws UnsupportedTypeException if a serializer cannot be created for the given type
   */
  <T> Serializer<T> createValueSerializer(Class<T> clazz, ClassLoader classLoader, ServiceConfiguration<?>... configs) throws UnsupportedTypeException;

  /**
   * Releases the given {@link Serializer} instance
   * If the serializer instance is provided by the user, {@link java.io.Closeable#close()}
   * will not be invoked.
   *
   * @param serializer the serializer to be released
   * @throws Exception when the release fails
   */
  void releaseSerializer(Serializer<?> serializer) throws Exception;
}
