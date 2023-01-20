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
import org.terracotta.management.capabilities.Capability;
import org.terracotta.management.context.Context;
import org.terracotta.management.context.ContextContainer;
import org.terracotta.management.registry.CapabilityManagementSupport;

import java.util.Collection;
import java.util.Map;

/**
 * Special version of {@link ManagementRegistryService} which can be used across several {@link org.ehcache.CacheManager}.
 * <p/>
 * This can be helpful in the case you want to access from one service all statistics, capabilities, etc of several cache managers.
 *
 * @author Mathieu Carbou
 */
public interface SharedManagementService extends CapabilityManagementSupport, Service {

  /**
   * Get the management contexts required to make use of the
   * registered objects' capabilities.
   *
   * @return a collection of contexts.
   */
  Map<Context, ContextContainer> getContextContainers();

  /**
   * Get the management capabilities of all the registered objects across several cache managers.
   *
   * @return a map of capabilities, where the key is the alias of the cache manager
   */
  Map<Context, Collection<Capability>> getCapabilities();

}
