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
import org.ehcache.spi.service.ServiceProvider;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SimpleServiceProvider implements Service {
  private final SimpleServiceConfiguration configuration;
  private volatile Service startedService;

  public SimpleServiceProvider(SimpleServiceConfiguration configuration) {
    this.configuration = configuration;
  }

  @Override
  public void start(ServiceProvider<Service> serviceProvider) {
    try {
      startedService = instantiate();
      startedService.start(serviceProvider);
    } catch (Exception e) {
      throw new RuntimeException("Error instantiating simple service", e);
    }
  }

  @Override
  public void stop() {
    startedService.stop();
    startedService = null;
  }

  private Service instantiate() throws Exception {
    List<Constructor<?>> potentialCtors = new ArrayList<>();

    for (Constructor<?> constructor : configuration.getClazz().getConstructors()) {
      if (constructor.getParameterTypes().length == configuration.getUnparsedArgs().size()) {
        potentialCtors.add(constructor);
      }
    }

    Constructor<?> targetCtor = null;
    List<Object> targetArgs = new ArrayList<>();

    for (Constructor<?> potentialCtor : potentialCtors) {
      Class<?>[] parameterTypes = potentialCtor.getParameterTypes();

      for (int i = 0; i < parameterTypes.length; i++) {
        Class<?> parameterType = parameterTypes[i];
        String unparsedArg = configuration.getUnparsedArgs().get(i);

        if (parameterType.isAssignableFrom(String.class)) {
          // this ctor arg accepts String
          targetArgs.add(unparsedArg);
          targetCtor = potentialCtor;
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
            targetCtor = potentialCtor;
          } else if (parameterType.isPrimitive()) {
            targetArgs.add(convertToPrimitive(parameterType, unparsedArg));
            targetCtor = potentialCtor;
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

    throw new IllegalArgumentException("Cannot instantiate '" + configuration.getClazz().getName() + "' with args " + configuration.getUnparsedArgs());
  }

  private Object convertToPrimitive(Class<?> primitiveType, String valueAsString) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
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
