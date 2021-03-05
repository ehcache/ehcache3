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

package org.ehcache.impl.config.serializer;

import static java.util.Collections.unmodifiableMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.ehcache.spi.serialization.SerializationProvider;
import org.ehcache.spi.serialization.Serializer;
import org.ehcache.core.spi.service.FileBasedPersistenceContext;
import org.ehcache.spi.service.ServiceCreationConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link ServiceCreationConfiguration} for the default {@link SerializationProvider}.
 */
public class DefaultSerializationProviderConfiguration implements ServiceCreationConfiguration<SerializationProvider, DefaultSerializationProviderConfiguration> {

  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultSerializationProviderConfiguration.class);

  private final Map<Class<?>, Class<? extends Serializer<?>>> defaultSerializers = new LinkedHashMap<>();

  /**
   * Creates a new configuration instance.
   */
  public DefaultSerializationProviderConfiguration() {
    // Default constructor
  }

  /**
   * Copy constructor
   *
   * @param other the other to copy from
   */
  public DefaultSerializationProviderConfiguration(DefaultSerializationProviderConfiguration other) {
    defaultSerializers.putAll(other.defaultSerializers);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Class<SerializationProvider> getServiceType() {
    return SerializationProvider.class;
  }

  /**
   * Adds a new {@link Serializer} mapping for the class {@code serializableClass}
   *
   * @param serializableClass the {@code Class} to add the mapping for
   * @param serializerClass the {@link Serializer} type to use
   * @param <T> the type of instances to be serialized / deserialized
   * @return this configuration object
   *
   * @throws NullPointerException if any argument is null
   * @throws IllegalArgumentException if a mapping for {@code serializableClass} already exists
   */
  public <T> DefaultSerializationProviderConfiguration addSerializerFor(Class<T> serializableClass, Class<? extends Serializer<T>> serializerClass) {
    return addSerializerFor(serializableClass, serializerClass, false);
  }

  /**
   * Adds a new {@link Serializer} mapping for the class {@code serializableClass}
   *
   * @param serializableClass the {@code Class} to add the mapping for
   * @param serializerClass the {@link Serializer} type to use
   * @param overwrite indicates if an existing mapping is to be overwritten
   * @param <T> the type of instances to be serialized / deserialized
   * @return this configuration object
   *
   * @throws NullPointerException if any argument is null
   * @throws IllegalArgumentException if a mapping for {@code serializableClass} already exists and {@code overwrite} is {@code false}
   */
  public <T> DefaultSerializationProviderConfiguration addSerializerFor(Class<T> serializableClass, Class<? extends Serializer<T>> serializerClass, boolean overwrite) {
    if (serializableClass == null) {
      throw new NullPointerException("Serializable class cannot be null");
    }
    if (serializerClass == null) {
      throw new NullPointerException("Serializer class cannot be null");
    }

    if(!isConstructorPresent(serializerClass, ClassLoader.class)) {
      throw new IllegalArgumentException("The serializer: " + serializerClass.getName() + " does not have a constructor that takes in a ClassLoader.");
    }

    if (isConstructorPresent(serializerClass, ClassLoader.class, FileBasedPersistenceContext.class)) {
      LOGGER.warn(serializerClass.getName() + " class has a constructor that takes in a FileBasedPersistenceContext. " +
                  "Support for this constructor has been removed since version 3.2. Consider removing it.");
    }

    if (defaultSerializers.containsKey(serializableClass) && !overwrite) {
      throw new IllegalArgumentException("Duplicate serializer for class : " + serializableClass.getName());
    } else {
      defaultSerializers.put(serializableClass, serializerClass);
    }

    return this;
  }

  private static boolean isConstructorPresent(Class<?> clazz, Class<?>... args) {
    try {
      clazz.getConstructor(args);
      return true;
    } catch (NoSuchMethodException e) {
      return false;
    }
  }

  /**
   * Returns the map of class to serializer class for persistent serializers.
   *
   * @return the map from class to serializer class
   */
  public Map<Class<?>, Class<? extends Serializer<?>>> getDefaultSerializers() {
    return unmodifiableMap(defaultSerializers);
  }

  @Override
  public DefaultSerializationProviderConfiguration derive() {
    return new DefaultSerializationProviderConfiguration(this);
  }

  @Override
  public DefaultSerializationProviderConfiguration build(DefaultSerializationProviderConfiguration configuration) {
    return configuration;
  }
}
