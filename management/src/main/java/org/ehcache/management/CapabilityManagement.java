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

import java.util.Collection;

/**
 * @author Mathieu Carbou
 */
public interface CapabilityManagement {

  /**
   * Create a query builder to collect statistics
   *
   * @param statisticName The statistic name to collec
   * @return a builder for the query
   */
  StatisticQuery.Builder queryStatistic(String statisticName);

  /**
   * Create a query builder to collect statistics
   *
   * @param statisticNames The statistic names to collect
   * @return a builder for the query
   */

  StatisticQuery.Builder queryStatistics(Collection<String> statisticNames);

  /**
   * Call an action of a managed object's capability.
   *
   * @param methodName the action's method name.
   * @param parameters the action method's parameters (objects and class names)
   * @param returnType The expected return type
   * @return the action method's return value.
   */
  <T> CallQuery.Builder<T> call(String methodName, Class<T> returnType, Parameter... parameters);

  /**
   * Call an action of a managed object's capability. We do not care about the return type.
   *
   * @param methodName the action's method name.
   * @param parameters the action method's parameters (objects and class names)
   * @return the action method's return value.
   */
  CallQuery.Builder<Void> call(String methodName, Parameter... parameters);

}
