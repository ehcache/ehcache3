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

package org.ehcache.config.serializer;

import static java.util.Collections.unmodifiableMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.ehcache.spi.serialization.SerializationProvider;
import org.ehcache.spi.serialization.Serializer;
import org.ehcache.spi.service.FileBasedPersistenceContext;
import org.ehcache.spi.service.ServiceCreationConfiguration;

public class DefaultSerializationProviderConfiguration implements ServiceCreationConfiguration<SerializationProvider> {

  private final Map<Class<?>, Class<? extends Serializer<?>>> transientSerializers = new LinkedHashMap<Class<?>, Class<? extends Serializer<?>>>();
  private final Map<Class<?>, Class<? extends Serializer<?>>> persistentSerializers = new LinkedHashMap<Class<?>, Class<? extends Serializer<?>>>();

  @Override
  public Class<SerializationProvider> getServiceType() {
    return SerializationProvider.class;
  }

  public <T> DefaultSerializationProviderConfiguration addSerializerFor(Class<T> serializableClass, Class<? extends Serializer<T>> serializerClass) {
    if (serializableClass == null) {
      throw new NullPointerException("Serializable class cannot be null");
    }
    if (serializerClass == null) {
      throw new NullPointerException("Serializer class cannot be null");
    }
    
    boolean transientConstructorPresent = false;
    boolean persistentConstructorPresent = false;
    
    if(transientConstructorPresent = isConstructorPresent(serializerClass, ClassLoader.class)) {
      if (transientSerializers.containsKey(serializableClass)) {
        throw new IllegalArgumentException("Duplicate transient serializer for class : " + serializableClass.getName());
      } else {
        transientSerializers.put(serializableClass, serializerClass);
      }
    }
    
    if(persistentConstructorPresent = isConstructorPresent(serializerClass, ClassLoader.class, FileBasedPersistenceContext.class)) {
      if (persistentSerializers.containsKey(serializableClass)) {
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

  public Map<Class<?>, Class<? extends Serializer<?>>> getTransientSerializers() {
    return unmodifiableMap(transientSerializers);
  }

  public Map<Class<?>, Class<? extends Serializer<?>>> getPersistentSerializers() {
    return unmodifiableMap(persistentSerializers);
  }
}
