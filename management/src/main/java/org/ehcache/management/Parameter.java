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
package org.ehcache.management;

/**
 * Represents a method parameter
 *
 * @author Mathieu Carbou
 */
public final class Parameter {

  private final Object value;
  private final String className;

  public Parameter(Object value) {
    this.value = value;
    this.className = value.getClass().getName();
  }

  public Parameter(Object value, String className) {
    this.value = value;
    this.className = className;
  }

  public Object getValue() {
    return value;
  }

  public String getClassName() {
    return className;
  }
}
