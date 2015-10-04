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

import org.ehcache.management.annotations.Exposed;
import org.ehcache.management.annotations.Named;
import org.ehcache.management.providers.ManagementProvider;
import org.ehcache.util.ConcurrentWeakIdentityHashMap;
import org.terracotta.management.capabilities.descriptors.CallDescriptor;
import org.terracotta.management.capabilities.descriptors.Descriptor;
import org.terracotta.management.stats.Statistic;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

/**
 * An abstract ManagementProvider with the base logic for handling actions.
 *
 * @author Ludovic Orban
 */
public abstract class AbstractActionProvider<T, A> implements ManagementProvider<T> {

  protected final ConcurrentMap<T, A> actions = new ConcurrentWeakIdentityHashMap<T, A>();

  @Override
  public final void register(T contextObject) {
    actions.putIfAbsent(contextObject, createActionWrapper(contextObject));
  }

  protected abstract A createActionWrapper(T contextObject);

  @Override
  public final void unregister(T contextObject) {
    actions.remove(contextObject);
  }

  @Override
  public final Set<Descriptor> descriptions() {
    return listManagementCapabilities();
  }

  private Set<Descriptor> listManagementCapabilities() {
    Set<Descriptor> capabilities = new HashSet<Descriptor>();

    Collection actions = this.actions.values();
    for (Object action : actions) {
      Class<?> actionClass = action.getClass();
      Method[] methods = actionClass.getMethods();

      for (Method method : methods) {
        Annotation[] declaredAnnotations = method.getDeclaredAnnotations();
        boolean expose = false;
        for (Annotation declaredAnnotation : declaredAnnotations) {
          if (declaredAnnotation.annotationType() == Exposed.class) {
            expose = true;
            break;
          }
        }
        if (!expose) {
          continue;
        }

        String methodName = method.getName();
        Class<?> returnType = method.getReturnType();

        Class<?>[] parameterTypes = method.getParameterTypes();
        List<String> parameterNames = new ArrayList<String>();

        for (int i = 0; i < parameterTypes.length; i++) {
          Annotation[] parameterAnnotations = method.getParameterAnnotations()[i];
          boolean named = false;
          for (Annotation parameterAnnotation : parameterAnnotations) {
            if (parameterAnnotation instanceof Named) {
              Named namedAnnotation = (Named) parameterAnnotation;
              parameterNames.add(namedAnnotation.value());
              named = true;
              break;
            }
          }
          if (!named) {
            parameterNames.add("arg" + i);
          }
        }

        List<CallDescriptor.Parameter> parameters = new ArrayList<CallDescriptor.Parameter>();
        for (int i = 0; i < parameterTypes.length; i++) {
          parameters.add(new CallDescriptor.Parameter(parameterNames.get(i), parameterTypes[i].getName()));
        }

        capabilities.add(new CallDescriptor(methodName, returnType.getName(), parameters));
      }
    }

    return capabilities;
  }

  @Override
  public final <T extends Statistic<?>> Collection<T> collectStatistics(Map<String, String> context, String[] statisticNames) {
    throw new UnsupportedOperationException("Not a statistics provider : " + getClass().getName());
  }
}
