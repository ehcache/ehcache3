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

import org.ehcache.internal.classes.ClassInstanceProviderFactoryConfiguration;
import org.ehcache.spi.serialization.DefaultSerializationProvider;
import org.ehcache.spi.serialization.Serializer;
import org.ehcache.spi.service.ServiceConfiguration;

public class DefaultSerializationProviderFactoryConfiguration extends ClassInstanceProviderFactoryConfiguration<Serializer<?>> implements ServiceConfiguration<DefaultSerializationProvider> {

  @Override
  public Class<DefaultSerializationProvider> getServiceType() {
    return DefaultSerializationProvider.class;
  }

  public <T> DefaultSerializationProviderFactoryConfiguration addSerializerFor(Class<T> serializableClass, Class<? extends Serializer<T>> serializerClass) {
    if (serializableClass == null) {
      throw new NullPointerException("Serializable class cannot be null");
    }
    if (serializerClass == null) {
      throw new NullPointerException("Serializer class cannot be null");
    }
    String alias = serializableClass.getName();
    if (getDefaults().containsKey(alias)) {
      throw new IllegalArgumentException("Duplicate serializer for class : " + alias);
    }
    getDefaults().put(alias, serializerClass);
    return this;
  }
}
