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
package org.ehcache.management.providers.actions;

import org.ehcache.management.annotations.Exposed;
import org.ehcache.management.annotations.Named;
import org.ehcache.management.providers.CacheBindingManagementProviderSkeleton;
import org.ehcache.management.registry.CacheBinding;
import org.ehcache.management.utils.ClassLoadingHelper;
import org.terracotta.management.capabilities.ActionsCapability;
import org.terracotta.management.capabilities.Capability;
import org.terracotta.management.capabilities.context.CapabilityContext;
import org.terracotta.management.capabilities.descriptors.CallDescriptor;
import org.terracotta.management.capabilities.descriptors.Descriptor;

import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author Ludovic Orban
 */
@Named("ActionsCapability")
public class EhcacheActionProvider extends CacheBindingManagementProviderSkeleton<EhcacheActionWrapper> {

  public EhcacheActionProvider(String cacheManagerAlias) {
    super(cacheManagerAlias);
  }

  @Override
  protected EhcacheActionWrapper createManagedObject(CacheBinding cacheBinding) {
    return new EhcacheActionWrapper(cacheBinding.getCache());
  }

  @Override
  protected Capability createCapability(String name, CapabilityContext context, Collection<Descriptor> descriptors) {
    return new ActionsCapability(name, descriptors, context);
  }

  @Override
  public final Set<Descriptor> getDescriptors() {
    Set<Descriptor> capabilities = new HashSet<Descriptor>();

    Collection<?> actions = managedObjects.values();
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
  public Object callAction(Map<String, String> context, String methodName, String[] argClassNames, Object[] args) {
    Map.Entry<CacheBinding, ?> entry = findManagedObject(context);
    if (entry != null) {
      Object managedObject = entry.getValue();
      ClassLoader classLoader = entry.getKey().getCache().getRuntimeConfiguration().getClassLoader();
      try {
        Method method = managedObject.getClass().getMethod(methodName, ClassLoadingHelper.toClasses(classLoader, argClassNames));
        return method.invoke(managedObject, args);
      } catch (NoSuchMethodException e) {
        throw new IllegalArgumentException("No such method : " + methodName + " with arg(s) " + Arrays.toString(argClassNames), e);
      } catch (IllegalAccessException e) {
        throw new RuntimeException(e);
      } catch (InvocationTargetException e) {
        throw new RuntimeException(e);
      }
    }
    throw new IllegalArgumentException("No such managed object for context : " + context);
  }

}
