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

import org.ehcache.management.ManagementRegistryServiceConfiguration;
import org.ehcache.management.providers.CacheBinding;
import org.ehcache.management.providers.ExposedCacheBinding;
import org.terracotta.management.registry.action.Exposed;
import org.terracotta.management.registry.action.Named;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

// must be public for reflexion management calls
public class EhcacheActionWrapper extends ExposedCacheBinding {

  EhcacheActionWrapper(ManagementRegistryServiceConfiguration registryServiceConfiguration, CacheBinding cacheBinding) {
    super(registryServiceConfiguration, cacheBinding);
  }

  @Exposed
  public void clear() {
    cacheBinding.getCache().clear();
  }

  @SuppressWarnings("unchecked")
  @Exposed
  public Object get(@Named("key") Object key) {
    Object convertedKey = convert(key, cacheBinding.getCache().getRuntimeConfiguration().getKeyType());
    return cacheBinding.getCache().get(convertedKey);
  }

  @SuppressWarnings("unchecked")
  @Exposed
  public void remove(@Named("key") Object key) {
    Object convertedKey = convert(key, cacheBinding.getCache().getRuntimeConfiguration().getKeyType());
    cacheBinding.getCache().remove(convertedKey);
  }

  @SuppressWarnings("unchecked")
  @Exposed
  public void put(@Named("key") Object key, @Named("value") Object value) {
    Object convertedKey = convert(key, cacheBinding.getCache().getRuntimeConfiguration().getKeyType());
    Object convertedValue = convert(value, cacheBinding.getCache().getRuntimeConfiguration().getValueType());
    cacheBinding.getCache().put(convertedKey, convertedValue);
  }

  private static Object convert(Object srcObj, Class<?> destClazz) {
    if (srcObj == null || destClazz.isInstance(srcObj)) {
      return srcObj;
    }
    try {
      Constructor<?> constructor = destClazz.getConstructor(srcObj.getClass());
      return constructor.newInstance(srcObj);
    } catch (NoSuchMethodException e) {
      throw new IllegalArgumentException("No conversion possible from " + srcObj.getClass().getName() + " to " + destClazz.getName(), e);
    } catch (InstantiationException e) {
      throw new IllegalArgumentException("Conversion error from " + srcObj.getClass().getName() + " to " + destClazz.getName(), e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    } catch (InvocationTargetException e) {
      throw new RuntimeException(e);
    }
  }

}
