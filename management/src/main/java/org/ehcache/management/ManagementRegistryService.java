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
package org.ehcache.management;

import org.ehcache.spi.service.Service;
import org.terracotta.management.registry.ManagementRegistry;

/**
 * Repository of objects exposing capabilities via the
 * management and monitoring facilities.
 * <p/>
 * A ManagementRegistry manages one and only one cache manager.
 * If you need to manage or monitor several cache managers at a time, you can use the {@link SharedManagementService} and register it into several cache managers.
 */
public interface ManagementRegistryService extends ManagementRegistry, Service {

  /**
   * @return This registry configuration
   */
  ManagementRegistryServiceConfiguration getConfiguration();

}
