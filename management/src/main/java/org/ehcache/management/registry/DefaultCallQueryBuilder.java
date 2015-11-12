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
import org.terracotta.management.call.Parameter;
import org.terracotta.management.context.Context;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * @author Mathieu Carbou
 */
final class DefaultCallQueryBuilder<T> implements CallQuery.Builder<T> {

  private final CapabilityManagementSupport capabilityManagement;
  private final String capabilityName;
  private final String methodName;
  private final Parameter[] parameters;
  private final Collection<Context> contexts;
  private final Class<T> returnType;

  DefaultCallQueryBuilder(CapabilityManagementSupport capabilityManagement, String capabilityName, String methodName, Class<T> returnType, Parameter... parameters) {
    this(capabilityManagement, capabilityName, methodName, returnType, parameters, Collections.<Context>emptyList());
  }

  private DefaultCallQueryBuilder(CapabilityManagementSupport capabilityManagement, String capabilityName, String methodName, Class<T> returnType, Parameter[] parameters, Collection<Context> contexts) {
    this.capabilityManagement = capabilityManagement;
    this.capabilityName = capabilityName;
    this.methodName = methodName;
    this.parameters = parameters;
    this.contexts = contexts;
    this.returnType = returnType;
  }

  @Override
  public CallQuery<T> build() {
    return new DefaultCallQuery<T>(capabilityManagement, capabilityName, methodName, returnType, parameters, contexts);
  }

  @Override
  public CallQuery.Builder<T> on(Context context) {
    if (!contexts.contains(context)) {
      List<Context> contexts = new ArrayList<Context>(this.contexts);
      contexts.add(context);
      return new DefaultCallQueryBuilder<T>(capabilityManagement, capabilityName, methodName, returnType, parameters, contexts);
    }
    return this;
  }

  @Override
  public CallQuery.Builder<T> on(Collection<? extends Context> contexts) {
    CallQuery.Builder<T> newBuilder = this;
    for (Context context : contexts) {
      newBuilder = newBuilder.on(context);
    }
    return newBuilder;
  }

}
