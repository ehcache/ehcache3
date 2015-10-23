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

import org.ehcache.management.providers.ManagementProvider;

import java.util.Collection;

/**
 * @author Mathieu Carbou
 */
public interface CapabilityManagementSupport {

  /**
   * Query based on a capability of this management registry, such as collecting statistics or calling some actions
   *
   * @param capabilityName The capability to work with
   * @return An intermediary class enabling the access of methods based on a capability
   */
  CapabilityManagement withCapability(String capabilityName);

  /**
   * List all management providers installed for a specific capability across all cache managers
   *
   * @param capabilityName The capability name
   * @return The list of management providers installed
   */
  Collection<ManagementProvider<?>> getManagementProvidersByCapability(String capabilityName);

}
