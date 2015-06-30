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
package org.ehcache.management.providers;

import org.terracotta.management.capabilities.context.CapabilityContext;
import org.terracotta.management.capabilities.descriptors.Descriptor;
import org.terracotta.management.stats.Statistic;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * Interface to a provider of management capabilities for certain object class.
 *
 * @author Ludovic Orban
 */
public interface ManagementProvider<T> {

  /**
   * The class of managed objects.
   *
   * @return a class.
   */
  Class<T> managedType();

  /**
   * Register an object for management in the current provider.
   *
   * @param managedObject the object to manage.
   */
  void register(T managedObject);

  /**
   * Unregister a managed object from the current provider.
   *
   * @param managedObject the managed object.
   */
  void unregister(T managedObject);

  /**
   * Get the set of capability descriptors the current provider provides.
   *
   * @return the set of capability descriptors.
   */
  Set<Descriptor> descriptions();

  /**
   * Get the context that the provided capabilities need to run.
   *
   * @return the context requirements.
   */
  CapabilityContext capabilityContext();

  /**
   * Collect statistics, if the provider supports this.
   *
   * @param context the context.
   * @param statisticNames the statistic names to collect.
   * @return the statistic values.
   */
  Collection<Statistic<?>> collectStatistics(Map<String, String> context, String[] statisticNames);

  /**
   * Call an action, if the provider supports this.
   *
   * @param context the context.
   * @param methodName the method name.
   * @param argClassNames the class names of the method arguments.
   * @param args the method arguments.
   * @return the action's return value.
   */
  Object callAction(Map<String, String> context, String methodName, String[] argClassNames, Object[] args);
}
