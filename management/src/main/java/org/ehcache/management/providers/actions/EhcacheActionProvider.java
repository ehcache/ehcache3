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

import org.ehcache.Ehcache;
import org.ehcache.management.providers.AbstractActionProvider;
import org.ehcache.management.utils.ClassLoadingHelper;
import org.ehcache.management.utils.ContextHelper;
import org.terracotta.management.capabilities.context.CapabilityContext;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Map;

/**
 * @author Ludovic Orban
 */
public class EhcacheActionProvider extends AbstractActionProvider<Ehcache, EhcacheActionWrapper> {

  @Override
  public Class<Ehcache> managedType() {
    return Ehcache.class;
  }

  @Override
  protected EhcacheActionWrapper createActionWrapper(Ehcache ehcache) {
    return new EhcacheActionWrapper(ehcache);
  }

  @Override
  public CapabilityContext capabilityContext() {
    return new CapabilityContext(Arrays.asList(new CapabilityContext.Attribute("cacheManagerName", true), new CapabilityContext.Attribute("cacheName", true)));
  }

  @Override
  public Object callAction(Map<String, String> context, String methodName, String[] argClassNames, Object[] args) {
    String cacheManagerName = context.get("cacheManagerName");
    if (cacheManagerName == null) {
      throw new IllegalArgumentException("Missing cache manager name from context");
    }
    String cacheName = context.get("cacheName");
    if (cacheName == null) {
      throw new IllegalArgumentException("Missing cache name from context");
    }

    for (Map.Entry<Ehcache, EhcacheActionWrapper> entry : actions.entrySet()) {
      if (!findCacheManagerName(entry).equals(cacheManagerName) ||
          !findCacheName(entry).equals(cacheName)) {
        continue;
      }

      try {
        EhcacheActionWrapper ehcacheActionWrapper = entry.getValue();
        ClassLoader classLoader = entry.getKey().getRuntimeConfiguration().getClassLoader();
        Method method = ehcacheActionWrapper.getClass().getMethod(methodName, ClassLoadingHelper.toClasses(classLoader, argClassNames));
        return method.invoke(ehcacheActionWrapper, args);
      } catch (NoSuchMethodException e) {
        throw new IllegalArgumentException("No such method : " + methodName + " with arg(s) " + Arrays.toString(argClassNames), e);
      } catch (IllegalAccessException e) {
        throw new RuntimeException(e);
      } catch (InvocationTargetException e) {
        throw new RuntimeException(e);
      }
    }

    throw new IllegalArgumentException("No such cache manager / cache pair : [" + cacheManagerName + " / " + cacheName + "]");
  }

  String findCacheName(Map.Entry<Ehcache, EhcacheActionWrapper> entry) {
    return ContextHelper.findCacheName(entry.getKey());
  }

  String findCacheManagerName(Map.Entry<Ehcache, EhcacheActionWrapper> entry) {
    return ContextHelper.findCacheManagerName(entry.getKey());
  }

}
