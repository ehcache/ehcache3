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

package org.ehcache.core.spi.service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

/**
 * ServiceUtils
 */
public class ServiceUtils {

  private ServiceUtils() {
    // No instance possible
  }

  public static <T> Collection<T> findAmongst(Class<T> clazz, Collection<?> instances) {
    return findAmongst(clazz, instances.toArray());
  }

  public static <T> Collection<T> findAmongst(Class<T> clazz, Object ... instances) {
    Collection<T> matches = new ArrayList<T>();
    for (Object instance : instances) {
      if (instance != null && clazz.isAssignableFrom(instance.getClass())) {
        matches.add(clazz.cast(instance));
      }
    }
    return Collections.unmodifiableCollection(matches);
  }

  public static <T> T findSingletonAmongst(Class<T> clazz, Collection<?> instances) {
    return findSingletonAmongst(clazz, instances.toArray());
  }

  public static <T> T findSingletonAmongst(Class<T> clazz, Object ... instances) {
    final Collection<T> matches = findAmongst(clazz, instances);
    if (matches.isEmpty()) {
      return null;
    } else if (matches.size() == 1) {
      return matches.iterator().next();
    } else {
      throw new IllegalArgumentException("More than one " + clazz.getName() + " found");
    }
  }
}
