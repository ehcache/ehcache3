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

import org.ehcache.impl.internal.classes.ClassInstanceConfiguration;
import org.ehcache.spi.serialization.SerializationProvider;
import org.ehcache.spi.serialization.Serializer;
import org.ehcache.spi.service.ServiceConfiguration;

/**
 * {@link ServiceConfiguration} for the default {@link SerializationProvider}.
 * <p>
 * This class overrides the default {@link ServiceConfiguration#compatibleWith(ServiceConfiguration)} implementation
 * to allow for independent configuration of the key and value serializers.
 */
public class DefaultSerializerConfiguration<T> extends ClassInstanceConfiguration<Serializer<T>> implements ServiceConfiguration<SerializationProvider, Void> {

  private final Type type;

  /**
   * Creates a new serializer configuration with the given {@link Serializer} class and {@link Type type}.
   *
   * @param clazz the serializer class
   * @param type the serializer type
   */
  public DefaultSerializerConfiguration(Class<? extends Serializer<T>> clazz, Type type) {
    super(clazz);
    this.type = type;
  }

  /**
   * Creates a new serializer configuration with the given {@link Serializer} instance and {@link Type type}.
   *
   * @param serializer the serializer instance
   * @param type the serializer type
   */
  public DefaultSerializerConfiguration(Serializer<T> serializer, Type type) {
    super(serializer);
    this.type = type;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Class<SerializationProvider> getServiceType() {
    return SerializationProvider.class;
  }

  /**
   * Get the type of the serializer configured
   *
   * @return the type
   */
  public Type getType() {
    return type;
  }

  @Override
  public boolean compatibleWith(ServiceConfiguration<?, ?> other) {
    if (other instanceof DefaultSerializerConfiguration<?>) {
      return !getType().equals(((DefaultSerializerConfiguration) other).getType());
    } else {
      return ServiceConfiguration.super.compatibleWith(other);
    }
  }

  /**
   * Serialization provider types
   */
  public enum Type {
    /**
     * Serializer type for the key
     */
    KEY,
    /**
     * Serializer type for the value
     */
    VALUE,
  }
}
