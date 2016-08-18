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

import org.ehcache.spi.persistence.StateRepository;
import org.ehcache.spi.serialization.SerializationProvider;
import org.ehcache.spi.serialization.Serializer;
import org.ehcache.core.spi.service.FileBasedPersistenceContext;
import org.ehcache.spi.service.ServiceCreationConfiguration;

/**
 * {@link ServiceCreationConfiguration} for the default {@link SerializationProvider}.
 */
public class DefaultSerializationProviderConfiguration implements ServiceCreationConfiguration<SerializationProvider> {

  private final Map<Class<?>, Class<? extends Serializer<?>>> transientSerializers = new LinkedHashMap<Class<?>, Class<? extends Serializer<?>>>();
  private final Map<Class<?>, Class<? extends Serializer<?>>> persistentSerializers = new LinkedHashMap<Class<?>, Class<? extends Serializer<?>>>();

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
    transientSerializers.putAll(other.transientSerializers);
    persistentSerializers.putAll(other.persistentSerializers);
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

    boolean transientConstructorPresent;
    boolean persistentConstructorPresent;

    if(transientConstructorPresent = isConstructorPresent(serializerClass, ClassLoader.class)) {
      if (!overwrite && transientSerializers.containsKey(serializableClass)) {
        throw new IllegalArgumentException("Duplicate transient serializer for class : " + serializableClass.getName());
      } else {
        transientSerializers.put(serializableClass, serializerClass);
      }
    }

    if (persistentConstructorPresent = isConstructorPresent(serializerClass, ClassLoader.class, StateRepository.class)) {
      if (!overwrite && persistentSerializers.containsKey(serializableClass)) {
        throw new IllegalArgumentException("Duplicate persistent serializer for class : " + serializableClass.getName());
      } else {
        persistentSerializers.put(serializableClass, serializerClass);
      }
    }

    if (isConstructorPresent(serializerClass, ClassLoader.class, FileBasedPersistenceContext.class)) {
      if (persistentConstructorPresent) {
        throw new IllegalArgumentException("Serializer cannot have constructors taking (ClassLoader, StateRepository) and (ClassLoader, FileBasedPersistenceContext)" +
                                           " - you should remove the second one as it is deprecated since version 3.1.0");
      }
      persistentConstructorPresent = true;
      if (!overwrite && persistentSerializers.containsKey(serializableClass)) {
        throw new IllegalArgumentException("Duplicate persistent serializer for class : " + serializableClass.getName());
      } else {
        persistentSerializers.put(serializableClass, serializerClass);
      }
    }

    if(!transientConstructorPresent && !persistentConstructorPresent) {
      throw new IllegalArgumentException("The serializer: " + serializerClass.getName()
                                         + " does not meet the constructor requirements for either transient or persistent caches.");
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
   * Returns the map of class to serializer class for transient serializers.
   *
   * @return the map from class to serializer class
   */
  public Map<Class<?>, Class<? extends Serializer<?>>> getTransientSerializers() {
    return unmodifiableMap(transientSerializers);
  }

  /**
   * Returns the map of class to serializer class for persistent serializers.
   *
   * @return the map from class to serializer class
   */
  public Map<Class<?>, Class<? extends Serializer<?>>> getPersistentSerializers() {
    return unmodifiableMap(persistentSerializers);
  }
}
