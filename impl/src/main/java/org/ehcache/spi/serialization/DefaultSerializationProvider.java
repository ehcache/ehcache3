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

import org.ehcache.config.serializer.DefaultSerializationProviderConfiguration;
import org.ehcache.config.serializer.DefaultSerializationProviderFactoryConfiguration;
import org.ehcache.internal.classes.ClassInstanceProvider;
import org.ehcache.internal.serialization.JavaSerializer;
import org.ehcache.spi.ServiceLocator;
import org.ehcache.spi.ServiceProvider;
import org.ehcache.spi.service.ServiceConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Map;

/**
 * @author Ludovic Orban
 */
public class DefaultSerializationProvider extends ClassInstanceProvider<Serializer<?>> implements SerializationProvider {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultSerializationProvider.class);

  public DefaultSerializationProvider() {
    super(DefaultSerializationProviderFactoryConfiguration.class, (Class) DefaultSerializationProviderConfiguration.class);
  }

  @Override
  public <T> Serializer<T> createSerializer(Class<T> clazz, ClassLoader classLoader, ServiceConfiguration<?>... configs) {
    DefaultSerializationProviderConfiguration config = ServiceLocator.findSingletonAmongst(DefaultSerializationProviderConfiguration.class, configs);
    String alias = (config != null ? null : clazz.getName());
    Serializer<T> serializer = (Serializer<T>) newInstance(alias, config, new Arg(ClassLoader.class, classLoader));
    if (serializer == null) {
      throw new IllegalArgumentException("No serializer found for type '" + alias + "'");
    }
    LOG.info("Serializer for <{}> : {}", clazz.getName(), serializer);
    return serializer;
  }

  @Override
  protected Class<? extends Serializer<?>> getPreconfigured(String alias) {
    Class<? extends Serializer<?>> direct = preconfiguredLoaders.get(alias);
    if (direct != null) {
      return direct;
    }
    Class<?> targetSerializedClass;
    try {
      targetSerializedClass = Class.forName(alias);
    } catch (ClassNotFoundException cnfe) {
      throw new IllegalArgumentException("Configured type class '" + alias + "' not found", cnfe);
    }
    for (Map.Entry<String, Class<? extends Serializer<?>>> entry : preconfiguredLoaders.entrySet()) {
      try {
        Class<?> configuredSerializedClass = Class.forName(entry.getKey());
        if (configuredSerializedClass.isAssignableFrom(targetSerializedClass)) {
          return entry.getValue();
        }
      } catch (ClassNotFoundException cnfe) {
        throw new IllegalArgumentException("Configured type class '" + entry.getKey() + "' for serializer '" + entry.getValue() + "' not found", cnfe);
      }
    }
    return null;
  }

  @Override
  public void start(ServiceConfiguration<?> config, ServiceProvider serviceProvider) {
    super.start(config, serviceProvider);
    // add java.io.Serializable at the end of the map if it wasn't already there
    if (!preconfiguredLoaders.containsKey(Serializable.class.getName())) {
      preconfiguredLoaders.put(Serializable.class.getName(), (Class) JavaSerializer.class);
    }
  }
}
