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
package org.ehcache.management.registry;

import org.ehcache.management.CallQuery;
import org.ehcache.management.CapabilityBasedQuery;
import org.ehcache.management.CapabilityManagement;
import org.ehcache.management.ContextualResult;
import org.ehcache.management.ContextualStatistics;
import org.ehcache.management.StatisticQuery;
import org.ehcache.management.providers.ManagementProvider;
import org.terracotta.management.stats.Statistic;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

/**
 * @author Mathieu Carbou
 */
final class DefaultCapabilityBasedQuery implements CapabilityBasedQuery {

  private final String capabilityName;
  private final CapabilityManagement capabilityManagement;

  DefaultCapabilityBasedQuery(CapabilityManagement capabilityManagement, String capabilityName) {
    this.capabilityManagement = capabilityManagement;
    this.capabilityName = capabilityName;
  }

  @Override
  public StatisticQuery.Builder queryStatistic(String statisticName) {
    return new StatisticQueryBuilder(capabilityManagement, capabilityName, Collections.singletonList(statisticName));
  }

  @Override
  public StatisticQuery.Builder queryStatistics(String... statisticNames) {
    return new StatisticQueryBuilder(capabilityManagement, capabilityName, Arrays.asList(statisticNames));
  }

  @Override
  public StatisticQuery.Builder queryStatistics(Collection<String> statisticNames) {
    return new StatisticQueryBuilder(capabilityManagement, capabilityName, statisticNames);
  }

  @Override
  public CallQuery.Builder call(String methodName, String[] argClassNames, Object[] args) {
    return new CallQueryBuilder(capabilityManagement, capabilityName, methodName, argClassNames, args);
  }

  @Override
  public CallQuery.Builder call(String methodName) {
    return call(methodName, new String[0], new Object[0]);
  }

  static class StatisticQueryBuilder implements StatisticQuery, StatisticQuery.Builder {

    private final CapabilityManagement capabilityManagement;
    private final String capabilityName;
    private final Collection<String> statisticNames;
    private final List<Map<String, String>> contexts = new ArrayList<Map<String, String>>();
    private volatile long since = 0;

    StatisticQueryBuilder(CapabilityManagement capabilityManagement, String capabilityName, Collection<String> statisticNames) {
      this.capabilityManagement = capabilityManagement;
      this.capabilityName = capabilityName;
      this.statisticNames = Collections.unmodifiableCollection(new LinkedHashSet<String>(statisticNames));
    }

    @Override
    public StatisticQuery build() {
      return this;
    }

    @Override
    public StatisticQuery.Builder on(Map<String, String> context) {
      this.contexts.add(Collections.unmodifiableMap(context));
      return this;
    }

    @Override
    public Builder on(List<Map<String, String>> contexts) {
      for (Map<String, String> context : contexts) {
        on(context);
      }
      return this;
    }

    @Override
    public StatisticQuery.Builder since(long unixTimestampMs) {
      this.since = unixTimestampMs;
      return this;
    }

    @Override
    public String getCapabilityName() {
      return capabilityName;
    }

    @Override
    public List<Map<String, String>> getContexts() {
      return Collections.unmodifiableList(contexts);
    }

    @Override
    public Collection<String> getStatisticNames() {
      return statisticNames;
    }

    @Override
    public long getSince() {
      return since;
    }

    @Override
    public List<ContextualStatistics> execute() {
      // pre-validation
      for (Map<String, String> context : contexts) {
        if (context.get("cacheManagerName") == null) {
          throw new IllegalArgumentException("Missing cache manager name from context " + context + " in context list " + contexts);
        }
      }

      List<ContextualStatistics> contextualStatistics = new ArrayList<ContextualStatistics>(contexts.size());
      Collection<ManagementProvider<?>> managementProviders = capabilityManagement.getManagementProvidersByCapability(capabilityName);

      for (Map<String, String> context : contexts) {
        List<Statistic<?>> statistics = new ArrayList<Statistic<?>>();
        for (ManagementProvider<?> managementProvider : managementProviders) {
          if (managementProvider.supports(context)) {
            statistics.addAll(managementProvider.collectStatistics(context, statisticNames, since));
          }
        }
        contextualStatistics.add(new ContextualStatistics(context, statistics));
      }

      return contextualStatistics;
    }

  }

  static class CallQueryBuilder implements CallQuery, CallQuery.Builder {

    private final CapabilityManagement capabilityManagement;
    private final String capabilityName;
    private final String methodName;
    private final String[] argClassNames;
    private final Object[] args;
    private final List<Map<String, String>> contexts = new ArrayList<Map<String, String>>();

    CallQueryBuilder(CapabilityManagement capabilityManagement, String capabilityName, String methodName, String[] argClassNames, Object[] args) {
      this.capabilityManagement = capabilityManagement;
      this.capabilityName = capabilityName;
      this.methodName = methodName;
      this.argClassNames = argClassNames;
      this.args = args;
    }

    @Override
    public CallQuery build() {
      return this;
    }

    @Override
    public CallQuery.Builder on(Map<String, String> context) {
      this.contexts.add(Collections.unmodifiableMap(context));
      return this;
    }

    @Override
    public Builder on(List<Map<String, String>> contexts) {
      for (Map<String, String> context : contexts) {
        on(context);
      }
      return this;
    }

    @Override
    public String getCapabilityName() {
      return capabilityName;
    }

    @Override
    public List<Map<String, String>> getContexts() {
      return Collections.unmodifiableList(contexts);
    }

    @Override
    public String getMethodName() {
      return methodName;
    }

    @Override
    public String[] getParameterClassNames() {
      return argClassNames;
    }

    @Override
    public Object[] getParameters() {
      return args;
    }

    @Override
    public List<ContextualResult> execute() {
      // pre-validation
      for (Map<String, String> context : contexts) {
        if (context.get("cacheManagerName") == null) {
          throw new IllegalArgumentException("Missing cache manager name from context " + context + " in context list " + contexts);
        }
      }

      List<ContextualResult> contextualResults = new ArrayList<ContextualResult>(contexts.size());
      Collection<ManagementProvider<?>> managementProviders = capabilityManagement.getManagementProvidersByCapability(capabilityName);

      for (Map<String, String> context : contexts) {
        ContextualResult result = ContextualResult.noResult(context);
        for (ManagementProvider<?> managementProvider : managementProviders) {
          if (managementProvider.supports(context)) {
            // just suppose there is only one manager handling calls - should be
            result = ContextualResult.result(context, managementProvider.callAction(context, methodName, argClassNames, args));
            break;
          }
        }
        contextualResults.add(result);
      }

      return contextualResults;
    }

  }
}
