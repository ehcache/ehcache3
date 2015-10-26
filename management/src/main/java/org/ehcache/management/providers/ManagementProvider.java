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

import org.terracotta.management.capabilities.Capability;
import org.terracotta.management.capabilities.context.CapabilityContext;
import org.terracotta.management.capabilities.descriptors.Descriptor;
import org.terracotta.management.stats.Statistic;

import java.util.Collection;
import java.util.List;
import java.util.Map;

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
  Collection<Descriptor> getDescriptors();

  /**
   * Get the context that the provided capabilities need to run.
   *
   * @return the context requirements.
   */
  CapabilityContext getCapabilityContext();

  /**
   * @return The full capability of this management provider
   */
  Capability getCapability();

  /**
   * @return The name of this capability
   */
  String getCapabilityName();

  /**
   * Collect statistics, if the provider supports this.
   *
   * @param context the context.
   * @param statisticNames the statistic names to collect.
   * @param since The unix time in ms from where to return the statistics for statistics based on samples.
   * @return the statistic values.
   */
  List<Statistic<?>> collectStatistics(Map<String, String> context, Collection<String> statisticNames, long since);

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

  /**
   * Check wheter this management provider supports the given context
   * 
   * @param context The management context, passed from the {@link org.ehcache.management.ManagementRegistry} methods
   * @return true if the context is supported by this management provider
   */
  boolean supports(Map<String, String> context);

  /**
   * Closes the management provider. Called when cache manager is closing.
   */
  void close();
}
