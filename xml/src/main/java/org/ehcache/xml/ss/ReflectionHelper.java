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
package org.ehcache.xml.ss;

import org.ehcache.spi.service.Service;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class ReflectionHelper {

  static Service instantiate(Class<? extends Service> clazz, List<String> unparsedArgs) throws Exception {
    List<Constructor<?>> potentialCtors = new ArrayList<>();

    for (Constructor<?> constructor : clazz.getConstructors()) {
      if (constructor.getParameterTypes().length == unparsedArgs.size()) {
        potentialCtors.add(constructor);
      }
    }

    Constructor<?> targetCtor = null;
    List<Object> targetArgs = new ArrayList<>();

    for (Constructor<?> potentialCtor : potentialCtors) {
      Class<?>[] parameterTypes = potentialCtor.getParameterTypes();
      targetCtor = potentialCtor;

      for (int i = 0; i < parameterTypes.length; i++) {
        Class<?> parameterType = parameterTypes[i];
        String unparsedArg = unparsedArgs.get(i);

        if (parameterType.isAssignableFrom(String.class)) {
          // this ctor arg accepts String
          targetArgs.add(unparsedArg);
        } else {
          Constructor<?> convertFromStringCtor = null;
          try {
            convertFromStringCtor = parameterType.getConstructor(String.class);
          } catch (NoSuchMethodException | SecurityException e) {
            // ignore, no such ctor
          }
          if (convertFromStringCtor != null) {
            // this ctor arg accepts a type that has a String ctor
            Object o = convertFromStringCtor.newInstance(unparsedArg);
            targetArgs.add(o);
          } else if (parameterType.isPrimitive()) {
            targetArgs.add(convertToPrimitive(parameterType, unparsedArg));
          } else {
            // unusable ctor
            targetArgs.clear();
            targetCtor = null;
            break;
          }
        }
      }
    }

    if (targetCtor != null) {
      return (Service) targetCtor.newInstance(targetArgs.toArray());
    }

    throw new IllegalArgumentException("Cannot instantiate '" + clazz.getName() + "' with args " + unparsedArgs);
  }


  private static Object convertToPrimitive(Class<?> primitiveType, String valueAsString) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
    Class<?> wrapperType = PRIMITIVES_TO_WRAPPERS.get(primitiveType);
    Method valueOfMethod = wrapperType.getMethod("valueOf", String.class);
    return valueOfMethod.invoke(wrapperType, valueAsString);
  }

  private static final Map<Class<?>, Class<?>> PRIMITIVES_TO_WRAPPERS = new HashMap<>();

  static {
    PRIMITIVES_TO_WRAPPERS.put(boolean.class, Boolean.class);
    PRIMITIVES_TO_WRAPPERS.put(byte.class, Byte.class);
    PRIMITIVES_TO_WRAPPERS.put(char.class, Character.class);
    PRIMITIVES_TO_WRAPPERS.put(double.class, Double.class);
    PRIMITIVES_TO_WRAPPERS.put(float.class, Float.class);
    PRIMITIVES_TO_WRAPPERS.put(int.class, Integer.class);
    PRIMITIVES_TO_WRAPPERS.put(long.class, Long.class);
    PRIMITIVES_TO_WRAPPERS.put(short.class, Short.class);
    PRIMITIVES_TO_WRAPPERS.put(void.class, Void.class);
  }

}
