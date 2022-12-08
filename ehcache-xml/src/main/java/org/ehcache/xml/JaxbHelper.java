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

package org.ehcache.xml;

import javax.xml.bind.annotation.XmlElement;
import java.lang.reflect.Field;

/**
 * @author Ludovic Orban
 */
public final class JaxbHelper {

  public static String findDefaultValue(Object jaxbObject, String fieldName) {
    Field declaredField = null;
    Class<?> clazz = jaxbObject.getClass();
    while (true) {
      try {
        declaredField = clazz.getDeclaredField(fieldName);
        break;
      } catch (NoSuchFieldException nsfe) {
        clazz = clazz.getSuperclass();
        if (clazz.equals(Object.class)) {
          break;
        }
      }
    }

    if (declaredField == null) {
      throw new IllegalArgumentException("No such field '" + fieldName + "' in JAXB class " + jaxbObject.getClass().getName());
    }

    XmlElement annotation = declaredField.getAnnotation(XmlElement.class);
    return annotation != null ? annotation.defaultValue() : null;
  }

}
