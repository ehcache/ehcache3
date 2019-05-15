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

package org.ehcache.core.spi.store.heap;

import org.ehcache.config.ResourceUnit;
import org.ehcache.spi.service.Service;
import org.ehcache.spi.service.ServiceConfiguration;

/**
 * {@link Service} responsible for providing {@link SizeOfEngine}.
 */
public interface SizeOfEngineProvider extends Service {

  /**
   * Creates a {@link SizeOfEngine} which will size objects.
   * <p>
   * Implementations may have configuration options that will be expressed as {@link ServiceConfiguration} and used
   * to specify behavior of the returned engine.
   *
   * @param resourceUnit type of the unit used to size the store
   * @param serviceConfigs Array of {@link ServiceConfiguration}s
   * @return {@link SizeOfEngine} instance
   */

  SizeOfEngine createSizeOfEngine(ResourceUnit resourceUnit, ServiceConfiguration<?, ?>... serviceConfigs);
}
