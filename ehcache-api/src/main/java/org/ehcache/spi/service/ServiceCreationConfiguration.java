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

package org.ehcache.spi.service;

/**
 * A configuration type used when creating a {@link Service}.
 *
 * @param <T> the service type this configuration works with
 * @param <R> the type of the detached representation
 */
public interface ServiceCreationConfiguration<T extends Service, R> {

  /**
   * Indicates which service consumes this configuration at creation.
   *
   * @return the service type
   */
  Class<T> getServiceType();

  /**
   * Derive a detached representation from this configuration
   *
   * @return a detached representation
   * @throws UnsupportedOperationException if the configuration has no representation
   */
  default R derive() throws UnsupportedOperationException {
    throw new UnsupportedOperationException();
  }

  /**
   * Construct a new configuration from the given detached representation.
   *
   * @param representation a detached representation
   * @return a new configuration
   * @throws UnsupportedOperationException if the configuration has no representation
   */
  default ServiceCreationConfiguration<T, ?> build(R representation) throws UnsupportedOperationException {
    throw new UnsupportedOperationException();
  }

  /**
   * Returns true if this configuration can co-exist with {@code other} in the same manager configuration.
   * <p>
   * The default implementation of {@code compatibleWith} (as used by many of the implementations) considers any
   * instance of the same type (or a sub-type) to be incompatible with this instance.
   *
   * @param other other service creation configuration
   * @return {@code true} if the two configurations are compatible
   */
  default boolean compatibleWith(ServiceCreationConfiguration<?, ?> other) {
    return !getClass().isInstance(other);
  };
}
