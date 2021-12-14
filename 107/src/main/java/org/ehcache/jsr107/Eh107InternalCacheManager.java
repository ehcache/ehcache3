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

package org.ehcache.jsr107;

import org.ehcache.config.Configuration;
import org.ehcache.core.EhcacheManager;
import org.ehcache.spi.service.Service;
import org.ehcache.spi.service.ServiceProvider;

import java.util.Collection;

/**
 * Class which has for only purpose to allow to retrieve the {@code ServiceLocator}.
 *
 * @author Henri Tremblay
 */
class Eh107InternalCacheManager extends EhcacheManager {

  Eh107InternalCacheManager(Configuration config, Collection<Service> services, boolean useLoaderInAtomics) {
    super(config, services, useLoaderInAtomics);
  }

  ServiceProvider<Service> getServiceProvider() {
    return serviceLocator;
  }
}
