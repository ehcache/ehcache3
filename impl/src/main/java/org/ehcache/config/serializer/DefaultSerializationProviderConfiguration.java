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

import org.ehcache.config.SerializationProviderConfiguration;
import org.ehcache.internal.classes.ClassInstanceProviderConfiguration;
import org.ehcache.spi.serialization.DefaultSerializationProvider;
import org.ehcache.spi.serialization.Serializer;

/**
 * @author Ludovic Orban
 */
public class DefaultSerializationProviderConfiguration<T> extends ClassInstanceProviderConfiguration<Serializer<T>> implements SerializationProviderConfiguration<DefaultSerializationProvider> {

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

}
