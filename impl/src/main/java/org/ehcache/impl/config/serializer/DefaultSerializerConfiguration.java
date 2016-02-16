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

import org.ehcache.core.config.serializer.SerializerConfiguration;
import org.ehcache.impl.internal.classes.ClassInstanceConfiguration;
import org.ehcache.spi.serialization.SerializationProvider;
import org.ehcache.spi.serialization.Serializer;

/**
 * @author Ludovic Orban
 */
public class DefaultSerializerConfiguration<T> extends ClassInstanceConfiguration<Serializer<T>> implements SerializerConfiguration<SerializationProvider> {

  private final Type type;

  public DefaultSerializerConfiguration(Class<? extends Serializer<T>> clazz, Type type) {
    super(clazz);
    this.type = type;
  }

  public DefaultSerializerConfiguration(Serializer<T> serializer, Type type) {
    super(serializer);
    this.type = type;
  }

  @Override
  public Class<SerializationProvider> getServiceType() {
    return SerializationProvider.class;
  }

  public Type getType() {
    return type;
  }

}
