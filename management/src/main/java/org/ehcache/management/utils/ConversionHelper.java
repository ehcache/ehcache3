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

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

/**
 * @author Ludovic Orban
 */
public abstract class ConversionHelper {

  private ConversionHelper() {
  }

  public static Object convert(Object srcObj, Class<?> destClazz) {
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
