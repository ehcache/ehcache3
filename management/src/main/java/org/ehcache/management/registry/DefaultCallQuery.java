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
import org.ehcache.management.CapabilityManagementSupport;
import org.ehcache.management.ResultSet;
import org.ehcache.management.providers.ManagementProvider;
import org.terracotta.management.call.ContextualReturn;
import org.terracotta.management.call.Parameter;
import org.terracotta.management.context.Context;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @author Mathieu Carbou
 */
final class DefaultCallQuery<T> implements CallQuery<T> {

  private final CapabilityManagementSupport capabilityManagement;
  private final String capabilityName;
  private final String methodName;
  private final Parameter[] parameters;
  private final Collection<Context> contexts;
  private final Class<T> returnType;

  DefaultCallQuery(CapabilityManagementSupport capabilityManagement, String capabilityName, String methodName, Class<T> returnType, Parameter[] parameters, Collection<Context> contexts) {
    this.capabilityManagement = capabilityManagement;
    this.capabilityName = capabilityName;
    this.methodName = methodName;
    this.parameters = parameters;
    this.contexts = Collections.unmodifiableCollection(new ArrayList<Context>(contexts));
    this.returnType = returnType;
  }

  @Override
  public Class<T> getReturnType() {
    return returnType;
  }

  @Override
  public String getCapabilityName() {
    return capabilityName;
  }

  @Override
  public Collection<Context> getContexts() {
    return contexts;
  }

  @Override
  public String getMethodName() {
    return methodName;
  }

  @Override
  public Parameter[] getParameters() {
    return parameters;
  }

  @Override
  public ResultSet<ContextualReturn<T>> execute() {
    // pre-validation
    for (Context context : contexts) {
      if (context.get("cacheManagerName") == null) {
        throw new IllegalArgumentException("Missing cache manager name from context " + context + " in context list " + contexts);
      }
    }

    Map<Context, ContextualReturn<T>> contextualResults = new LinkedHashMap<Context, ContextualReturn<T>>(contexts.size());
    Collection<ManagementProvider<?>> managementProviders = capabilityManagement.getManagementProvidersByCapability(capabilityName);

    for (Context context : contexts) {
      ContextualReturn<T> result = ContextualReturn.<T>empty(context);
      for (ManagementProvider<?> managementProvider : managementProviders) {
        if (managementProvider.supports(context)) {
          // just suppose there is only one manager handling calls - should be
          result = ContextualReturn.of(context, managementProvider.callAction(context, methodName, returnType, parameters));
          break;
        }
      }
      contextualResults.put(context, result);
    }

    return new DefaultResultSet<ContextualReturn<T>>(contextualResults);
  }

}
