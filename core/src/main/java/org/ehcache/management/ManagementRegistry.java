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

import java.util.Collection;
import java.util.Map;

/**
 * Repository of objects exposing capabilities via the
 * management and monitoring facilities.
 *
 * @author Ludovic Orban
 */
public interface ManagementRegistry extends Service {

  /**
   * Register an object in the management registry.
   *
   * @param managedType the managed object's class.
   * @param managedObject  the managed object.
   * @param <T> the type.
   */
  <T> void register(Class<T> managedType, T managedObject);

  /**
   * Unregister an object from the management registry.
   *
   * @param managedType the managed object's class.
   * @param managedObject  the managed object.
   * @param <T> the type.
   */
  <T> void unregister(Class<T> managedType, T managedObject);

  /**
   * Get the management capabilities of the registered objects.
   *
   * @param <T> the capability type.
   * @return a collection of capabilities.
   */
  <T> Collection<T> capabilities();

  /**
   * Get the management contexts required to make use of the
   * registered objects' capabilities.
   *
   * @param <T> the context type.
   * @return a collection of contexts.
   */
  <T> Collection<T> contexts();

  /**
   * Collect statistics from a managed object's capability.
   *
   * @param context the capability's context.
   * @param capabilityName the capability name.
   * @param statisticNames the statistic names.
   * @param <T> the statistics' type.
   * @return a collection of statistics.
   */
  <T> Collection<T> collectStatistics(Map<String, String> context, String capabilityName, String... statisticNames);

  /**
   * Call an action of a managed object's capability.
   *
   * @param context the capability's context.
   * @param capabilityName the capability name.
   * @param methodName the action's method name.
   * @param argClassNames the action method's argument class names.
   * @param args the action method's arguments.
   * @param <T> the returned type.
   * @return the action method's return value.
   */
  <T> T callAction(Map<String, String> context, String capabilityName, String methodName, String[] argClassNames, Object[] args);

}
