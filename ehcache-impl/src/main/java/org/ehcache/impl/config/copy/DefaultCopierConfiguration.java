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

package org.ehcache.impl.config.copy;

import org.ehcache.impl.internal.classes.ClassInstanceConfiguration;
import org.ehcache.spi.copy.Copier;
import org.ehcache.spi.copy.CopyProvider;
import org.ehcache.spi.service.ServiceConfiguration;

/**
 * {@link ServiceConfiguration} for the default {@link CopyProvider} implementation.
 * <p>
 * Enables configuring a {@link Copier} for the key or value of a given cache.
 * <p>
 * This class overrides the default {@link ServiceConfiguration#compatibleWith(ServiceConfiguration)} implementation
 * to allow for independent configuration of the key and value copiers.
 *
 * @param <T> the type which the configured copier can handle
 */
public class DefaultCopierConfiguration<T> extends ClassInstanceConfiguration<Copier<T>> implements ServiceConfiguration<CopyProvider, Void> {

  private final Type type;

  /**
   * Creates a new configuration with the given {@link Copier} class of the provided {@link Type}.
   *
   * @param clazz the copier class
   * @param type the copier type - key or value
   */
  public DefaultCopierConfiguration(Class<? extends Copier<T>> clazz, Type type) {
    super(clazz);
    this.type = type;
  }

  /**
   * Creates a new configuration with the given {@link Copier} instance of the provided {@link Type}.
   *
   * @param instance the copier instance
   * @param type the copier type - key or value
   */
  public DefaultCopierConfiguration(Copier<T> instance, Type type) {
    super(instance);
    this.type = type;
  }

  DefaultCopierConfiguration(Class<? extends Copier<T>> copierClass) {
    super(copierClass);
    this.type = null;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Class<CopyProvider> getServiceType() {
    return CopyProvider.class;
  }

  @Override
  public boolean compatibleWith(ServiceConfiguration<?, ?> other) {
    if (other instanceof DefaultCopierConfiguration<?>) {
      return !getType().equals(((DefaultCopierConfiguration) other).getType());
    } else {
      return ServiceConfiguration.super.compatibleWith(other);
    }
  }

  /**
   * Returns the {@link Type} of this configuration
   *
   * @return the copier type - key or value
   */
  public Type getType() {
    return type;
  }

  /**
   * Copy provider types
   */
  public enum Type {
    /**
     * Indicates a key copier configuration
     */
    KEY,
    /**
     * Indicates a value copier configuration
     */
    VALUE,
  }
}
