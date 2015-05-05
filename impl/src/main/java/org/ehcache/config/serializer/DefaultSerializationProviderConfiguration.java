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

import org.ehcache.internal.classes.ClassInstanceProviderConfig;
import org.ehcache.spi.ServiceLocator;
import org.ehcache.spi.serialization.DefaultSerializationProvider;
import org.ehcache.spi.serialization.Serializer;
import org.ehcache.spi.service.ServiceConfiguration;

import java.util.Collection;

public class DefaultSerializationProviderConfiguration<T> extends ClassInstanceProviderConfig<Serializer<T>> implements ServiceConfiguration<DefaultSerializationProvider> {

  private final Type type;

  public DefaultSerializationProviderConfiguration(Class<? extends Serializer<T>> clazz, Type type) {
    super(clazz);
    this.type = type;
  }

  @Override
  public Class<DefaultSerializationProvider> getServiceType() {
    return DefaultSerializationProvider.class;
  }

  public Type getType() {
    return type;
  }

  public enum Type {
    KEY,
    VALUE,
  }

  public static DefaultSerializationProviderConfiguration find(Type type, ServiceConfiguration<?>... serviceConfigurations) {
    DefaultSerializationProviderConfiguration result = null;

    Collection<DefaultSerializationProviderConfiguration> serializationProviderConfigurations = ServiceLocator.findAmongst(DefaultSerializationProviderConfiguration.class, serviceConfigurations);
    for (DefaultSerializationProviderConfiguration serializationProviderConfiguration : serializationProviderConfigurations) {
      if (serializationProviderConfiguration.getType() == type) {
        if (result != null) {
          throw new IllegalArgumentException("Duplicate " + type + " serialization provider : " + serializationProviderConfiguration);
        }
        result = serializationProviderConfiguration;
      }
    }

    return result;
  }

}
