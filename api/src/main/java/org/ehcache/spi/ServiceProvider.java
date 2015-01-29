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
import org.ehcache.spi.service.ServiceConfiguration;

/**
 *
 * This acts as a repository for {@link Service} instances, that can be use to
 * look them up by type, or their {@link ServiceConfiguration} type.
 *
 * @author Alex Snaps
 */
public interface ServiceProvider {

  /**
   * Will look up the {@link Service} configured by the {@code config} type. Should the {@link Service} not yet started,
   * it will be started, passed that {@link ServiceConfiguration} instance. Otherwise it is only used to find the
   * matching {@link Service} instance.
   *
   * @param config The type configuring the Service being looked up
   * @param <T> The actual {@link Service} implementation
   * @return the service instance for {@code T} type, or null if it couldn't be located
   */
  <T extends Service> T findServiceFor(ServiceConfiguration<T> config);

  /**
   * Will look up the {@link Service} of the {@code serviceType} type. Should the {@link Service} not yet started,
   * it will be started with a {@code null} {@link ServiceConfiguration} passed to its
   * {@link Service#start(org.ehcache.spi.service.ServiceConfiguration, ServiceProvider)} method.
   *
   * @param serviceType the Class of the instance being looked up
   * @param <T> The actual {@link Service} implementation
   * @return the service instance for {@code T} type, or null if it couldn't be located
   */
  <T extends Service> T findService(Class<T> serviceType);
}
