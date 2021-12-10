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

import org.ehcache.impl.internal.classes.ClassInstanceProviderConfiguration;
import org.ehcache.spi.copy.Copier;
import org.ehcache.spi.copy.CopyProvider;
import org.ehcache.spi.service.ServiceCreationConfiguration;

/**
 * {@link ServiceCreationConfiguration} for the default {@link CopyProvider} implementation.
 * <p>
 * Enables configuring {@link Class} - {@link Copier} pairs that will be selected unless cache level configurations
 * are provided.
 */
public class DefaultCopyProviderConfiguration extends ClassInstanceProviderConfiguration<Class<?>, DefaultCopierConfiguration<?>> implements ServiceCreationConfiguration<CopyProvider, DefaultCopyProviderConfiguration> {

  /**
   * Default constructor.
   */
  public DefaultCopyProviderConfiguration() {
    // Default constructor
  }

  /**
   * Copy constructor
   *
   * @param other the instance to copy
   */
  public DefaultCopyProviderConfiguration(DefaultCopyProviderConfiguration other) {
    getDefaults().putAll(other.getDefaults());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Class<CopyProvider> getServiceType() {
    return CopyProvider.class;
  }

  /**
   * Adds a new {@code Class} - {@link Copier} pair to this configuration object
   *
   * @param clazz the {@code Class} for which this copier is
   * @param copierClass the {@link Copier} type to use
   * @param <T> the type of objects the copier will deal with
   *
   * @return this configuration instance
   *
   * @throws NullPointerException if any argument is null
   * @throws IllegalArgumentException in a case a mapping for {@code clazz} already exists
   */
  public <T> DefaultCopyProviderConfiguration addCopierFor(Class<T> clazz, Class<? extends Copier<T>> copierClass) {
    return addCopierFor(clazz, copierClass, false);
  }

  /**
   * Adds a new {@code Class} - {@link Copier} pair to this configuration object
   *
   * @param clazz the {@code Class} for which this copier is
   * @param copierClass the {@link Copier} type to use
   * @param overwrite indicates if an existing mapping is to be overwritten
   * @param <T> the type of objects the copier will deal with
   *
   * @return this configuration instance
   *
   * @throws NullPointerException if any argument is null
   * @throws IllegalArgumentException in a case a mapping for {@code clazz} already exists and {@code overwrite} is {@code false}
   */
  public <T> DefaultCopyProviderConfiguration addCopierFor(Class<T> clazz, Class<? extends Copier<T>> copierClass, boolean overwrite) {
    if (clazz == null) {
      throw new NullPointerException("Copy target class cannot be null");
    }
    if (copierClass == null) {
      throw new NullPointerException("Copier class cannot be null");
    }
    if (!overwrite && getDefaults().containsKey(clazz)) {
      throw new IllegalArgumentException("Duplicate copier for class : " + clazz);
    }
    getDefaults().put(clazz, new DefaultCopierConfiguration<>(copierClass));
    return this;
  }

  @Override
  public DefaultCopyProviderConfiguration derive() {
    return new DefaultCopyProviderConfiguration(this);
  }

  @Override
  public DefaultCopyProviderConfiguration build(DefaultCopyProviderConfiguration configuration) {
    return configuration;
  }
}
