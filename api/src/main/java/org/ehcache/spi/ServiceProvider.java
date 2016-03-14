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

package org.ehcache.spi;

import org.ehcache.spi.service.Service;

import java.util.Collection;

/**
 * This acts as a repository for {@link Service} instances, that can be used to
 * look them up by type.
 *
 * @param <T> A bound on service types this provider can return
 */
public interface ServiceProvider<T extends Service> {

  /**
   * Will look up the {@link Service} of the {@code serviceType}.
   * The returned service will be started or not depending on the started state of the {@code ServiceProvider}.
   *
   * @param serviceType the {@code class} of the service being looked up
   * @param <U> The actual {@link Service} type
   * @return the service instance for {@code T} type, or {@code null} if it couldn't be located
   *
   * @throws IllegalArgumentException if {@code serviceType} is marked with the
   *        {@link org.ehcache.spi.service.PluralService PluralService} annotation
   */
  <U extends T> U getService(Class<U> serviceType);

  /**
   * Looks up all {@link Service} instances registered to support the {@code serviceType} supplied.
   * This method must be used for any service type marked with the
   * {@link org.ehcache.spi.service.PluralService PluralService} annotation.
   *
   * @param serviceType the {@code class} of the service being looked up
   * @param <U> the actual {@link Service} type
   * @return a collection of the registered services implementing {@code serviceType}; the
   *     collection is empty if no services are registered for {@code serviceType}
   */
  <U extends T> Collection<U> getServicesOfType(Class<U> serviceType);
}
