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
package org.ehcache.management.utils;

/**
 * @author Ludovic Orban
 */
public abstract class ClassLoadingHelper {

  private ClassLoadingHelper() {
  }

  public static Class<?>[] toClasses(ClassLoader classLoader, String[] argClassNames) {
    Class<?>[] result = new Class[argClassNames.length];
    for (int i = 0; i < argClassNames.length; i++) {
      String argClassName = argClassNames[i];
      try {
        result[i] = Class.forName(argClassName, true, classLoader);
      } catch (ClassNotFoundException e) {
        throw new IllegalArgumentException("No such class name : " + argClassName, e);
      }
    }
    return result;
  }

}
