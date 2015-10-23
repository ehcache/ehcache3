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
import org.terracotta.management.context.ContextContainer;
import org.terracotta.management.stats.Statistic;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Special version of {@link ManagementRegistry} which can be used across several {@link org.ehcache.CacheManager}.
 * <p/>
 * This can be helpful in the case you want to access from one service all statistics, capabilities, etc of several cache managers.
 *
 * @author Mathieu Carbou
 */
public interface SharedManagementService extends Service {

  /**
   * Get the management contexts required to make use of the
   * registered objects' capabilities.
   *
   * @return a collection of contexts.
   */
  Collection<ContextContainer> getContexts();

  /**
   * Get the management capabilities of the registered objects across several cache managers.
   *
   * @return a map of capabilities, where the key is the alias of the cache manager
   */
  Map<String, Collection<Capability>> getCapabilities();

  /**
   * Collect statistics from a managed object's capability and several contexts at once.
   *
   * @param contextList    the capability's context list.
   * @param capabilityName the capability name.
   * @param statisticNames the statistic names.
   * @return a list of collection of statistics, with the same order and indexes of the context list
   */
  List<Collection<Statistic<?>>> collectStatistics(List<Map<String, String>> contextList, String capabilityName, String... statisticNames);

  /**
   * Call an action on several managed object's capability, based on the contexts.
   *
   * @param contextList    a list of the capability's context.
   * @param capabilityName the capability name.
   * @param methodName     the action's method name.
   * @param argClassNames  the action method's argument class names.
   * @param args           the action method's arguments.
   * @return the list of action method's return value for each context passed, in same order and with same indexes.
   */
  List<Object> callAction(List<Map<String, String>> contextList, String capabilityName, String methodName, String[] argClassNames, Object[] args);

}
