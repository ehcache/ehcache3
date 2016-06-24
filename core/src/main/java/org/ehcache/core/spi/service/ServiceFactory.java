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

package org.ehcache.core.spi.service;

import org.ehcache.spi.service.ServiceProvider;
import org.ehcache.spi.service.Service;
import org.ehcache.spi.service.ServiceCreationConfiguration;

/**
 * A factory abstraction that can create {@link Service} instances.
 */
public interface ServiceFactory<T extends Service> {

  /**
   * Creates an instance of the service using the passed in {@link ServiceCreationConfiguration}.
   * <P>
   *   Note that a {@code null} configuration may be supported or even required by a service implementation.
   * </P>
   * @param configuration the creation configuration, can be {@code null} for some services
   *
   * @return the new service, not {@link Service#start(ServiceProvider) started}
   */
  T create(ServiceCreationConfiguration<T> configuration);

  /**
   * Queries a {@code ServiceFactory} to know which {@link Service} type it produces.
   *
   * @return the class of the produced service.
   */
  Class<T> getServiceType();
}
