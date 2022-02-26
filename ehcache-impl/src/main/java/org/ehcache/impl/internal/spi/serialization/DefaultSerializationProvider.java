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

package org.ehcache.impl.internal.spi.serialization;

import org.ehcache.core.spi.service.ServiceUtils;
import org.ehcache.impl.config.serializer.DefaultSerializationProviderConfiguration;
import org.ehcache.impl.config.serializer.DefaultSerializerConfiguration;
import org.ehcache.impl.serialization.ByteArraySerializer;
import org.ehcache.impl.serialization.CharSerializer;
import org.ehcache.impl.serialization.CompactJavaSerializer;
import org.ehcache.impl.serialization.DoubleSerializer;
import org.ehcache.impl.serialization.FloatSerializer;
import org.ehcache.impl.serialization.IntegerSerializer;
import org.ehcache.impl.serialization.LongSerializer;
import org.ehcache.impl.serialization.StringSerializer;
import org.ehcache.spi.service.ServiceProvider;
import org.ehcache.spi.serialization.SerializationProvider;
import org.ehcache.spi.serialization.Serializer;
import org.ehcache.spi.serialization.UnsupportedTypeException;
import org.ehcache.core.spi.service.FileBasedPersistenceContext;
import org.ehcache.spi.service.Service;
import org.ehcache.spi.service.ServiceConfiguration;
import org.ehcache.core.collections.ConcurrentWeakIdentityHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Ludovic Orban
 */
public class DefaultSerializationProvider implements SerializationProvider {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultSerializationProvider.class);

  protected final Map<Class<?>, Class<? extends Serializer<?>>> serializers;

  final ConcurrentWeakIdentityHashMap<Serializer<?>, AtomicInteger> providedVsCount = new ConcurrentWeakIdentityHashMap<>();
  final Set<Serializer<?>> instantiated = Collections.newSetFromMap(new ConcurrentWeakIdentityHashMap<Serializer<?>, Boolean>());

  public DefaultSerializationProvider(DefaultSerializationProviderConfiguration configuration) {
    if (configuration != null) {
      this.serializers = new LinkedHashMap<>(configuration.getDefaultSerializers());
    } else {
      this.serializers = new LinkedHashMap<>(Collections.<Class<?>, Class<? extends Serializer<?>>>emptyMap());
    }
  }

  @Override
  public <T> Serializer<T> createKeySerializer(Class<T> clazz, ClassLoader classLoader, ServiceConfiguration<?, ?>... configs) throws UnsupportedTypeException {
    DefaultSerializerConfiguration<T> configuration = find(DefaultSerializerConfiguration.Type.KEY, configs);
    Serializer<T> serializer = getUserProvidedSerializer(configuration);
    if (serializer == null) {
      serializer = createSerializer(clazz, classLoader, configuration, configs);
      instantiated.add(serializer);
    }
    updateProvidedInstanceCounts(serializer);
    return serializer;
  }

  @Override
  public <T> Serializer<T> createValueSerializer(Class<T> clazz, ClassLoader classLoader, ServiceConfiguration<?, ?>... configs) throws UnsupportedTypeException {
    DefaultSerializerConfiguration<T> configuration = find(DefaultSerializerConfiguration.Type.VALUE, configs);
    Serializer<T> serializer = getUserProvidedSerializer(configuration);
    if (serializer == null) {
      serializer = createSerializer(clazz, classLoader, configuration, configs);
      instantiated.add(serializer);
    }
    updateProvidedInstanceCounts(serializer);
    return serializer;
  }

  private <T> Serializer<T> createSerializer(Class<T> clazz, ClassLoader classLoader, DefaultSerializerConfiguration<T> config, ServiceConfiguration<?, ?>... configs) throws UnsupportedTypeException {
    Class<? extends Serializer<T>> klazz = getSerializerClassFor(clazz, config);

    try {
      klazz.getConstructor(ClassLoader.class, FileBasedPersistenceContext.class);
      LOG.warn(klazz.getName() + " class has a constructor that takes in a FileBasedPersistenceContext. " +
                  "Support for this constructor has been removed since version 3.2. Consider removing it.");
    } catch (NoSuchMethodException e) {
      // Ideal
    }

    try {
      return constructSerializer(clazz, klazz.getConstructor(ClassLoader.class), classLoader);
    } catch (NoSuchMethodException e) {
      throw new RuntimeException(klazz + " does not have a constructor that takes in a ClassLoader.", e);
    }
  }


  private <T> Class<? extends Serializer<T>> getSerializerClassFor(Class<T> clazz, DefaultSerializerConfiguration<T> config) throws UnsupportedTypeException {
    if (config != null) {
      Class<? extends Serializer<T>> configured = config.getClazz();
      if (configured != null) {
        return configured;
      }
    }

    @SuppressWarnings("unchecked")
    Class<? extends Serializer<T>> direct = (Class<? extends Serializer<T>>) serializers.get(clazz);
    if (direct != null) {
      return direct;
    }
    for (Map.Entry<Class<?>, Class<? extends Serializer<?>>> entry : serializers.entrySet()) {
      if (entry.getKey().isAssignableFrom(clazz)) {
        @SuppressWarnings("unchecked")
        Class<? extends Serializer<T>> type = (Class<? extends Serializer<T>>)entry.getValue();
        return type;
      }
    }
    throw new UnsupportedTypeException("No serializer found for type '" + clazz.getName() + "'");
  }

  private <T> Serializer<T> constructSerializer(Class<T> clazz, Constructor<? extends Serializer<T>> constructor, Object ... args) {
    try {
      Serializer<T> serializer = constructor.newInstance(args);
      LOG.debug("Serializer for <{}> : {}", clazz.getName(), serializer);
      return serializer;
    } catch (InstantiationException | InvocationTargetException | IllegalAccessException e) {
      throw new RuntimeException(e);
    } catch (IllegalArgumentException e) {
      throw new AssertionError(e);
    }
  }

  private void updateProvidedInstanceCounts(Serializer<?> serializer) {
    AtomicInteger currentCount = providedVsCount.putIfAbsent(serializer, new AtomicInteger(1));
    if(currentCount != null) {
      currentCount.incrementAndGet();
    }
  }

  @Override
  public void releaseSerializer(final Serializer<?> serializer) throws IOException {
    AtomicInteger currentCount = providedVsCount.get(serializer);
    if(currentCount != null) {
      if(currentCount.decrementAndGet() < 0) {
        currentCount.incrementAndGet();
        throw new IllegalArgumentException("Given serializer:" + serializer.getClass().getName() + " is not managed by this provider");
      }
    } else {
      throw new IllegalArgumentException("Given serializer:" + serializer.getClass().getName() + " is not managed by this provider");
    }

    if(instantiated.remove(serializer)) {
      if (serializer instanceof Closeable) {
        ((Closeable) serializer).close();
      }
    }
  }

  @Override
  public void start(ServiceProvider<Service> serviceProvider) {
    addDefaultSerializerIfNoneRegistered(serializers, Serializable.class, CompactJavaSerializer.<Serializable>asTypedSerializer());
    addDefaultSerializerIfNoneRegistered(serializers, Long.class, LongSerializer.class);
    addDefaultSerializerIfNoneRegistered(serializers, Integer.class, IntegerSerializer.class);
    addDefaultSerializerIfNoneRegistered(serializers, Float.class, FloatSerializer.class);
    addDefaultSerializerIfNoneRegistered(serializers, Double.class, DoubleSerializer.class);
    addDefaultSerializerIfNoneRegistered(serializers, Character.class, CharSerializer.class);
    addDefaultSerializerIfNoneRegistered(serializers, String.class, StringSerializer.class);
    addDefaultSerializerIfNoneRegistered(serializers, byte[].class, ByteArraySerializer.class);
  }

  @Override
  public void stop() {
    // no-op
  }

  private static <T> void addDefaultSerializerIfNoneRegistered(Map<Class<?>, Class<? extends Serializer<?>>> serializers, Class<T> clazz, Class<? extends Serializer<T>> serializerClass) {
    if (!serializers.containsKey(clazz)) {
      serializers.put(clazz, serializerClass);
    }
  }

  private static <T> Serializer<T> getUserProvidedSerializer(DefaultSerializerConfiguration<T> conf) {
    if(conf != null) {
      Serializer<T> instance = conf.getInstance();
      if(instance != null) {
        return instance;
      }
    }
    return null;
  }

  @SuppressWarnings("unchecked")
  private static <T> DefaultSerializerConfiguration<T> find(DefaultSerializerConfiguration.Type type, ServiceConfiguration<?, ?>... serviceConfigurations) {
    DefaultSerializerConfiguration<T> result = null;

    @SuppressWarnings("rawtypes")
    Collection<DefaultSerializerConfiguration<?>> serializationProviderConfigurations =
      (Collection) ServiceUtils.findAmongst(DefaultSerializerConfiguration.class, (Object[]) serviceConfigurations);
    for (DefaultSerializerConfiguration<?> serializationProviderConfiguration : serializationProviderConfigurations) {
      if (serializationProviderConfiguration.getType() == type) {
        if (result != null) {
          throw new IllegalArgumentException("Duplicate " + type + " serialization provider : " + serializationProviderConfiguration);
        }
        result = (DefaultSerializerConfiguration<T>) serializationProviderConfiguration;
      }
    }

    return result;
  }
}
