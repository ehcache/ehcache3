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

package org.ehcache.impl.internal.classes;

import static java.util.Arrays.asList;
import java.util.List;

/**
 * @author Alex Snaps
 */
public class ClassInstanceConfiguration<T> {

  private final Class<? extends T> clazz;
  private final List<Object> arguments;

  private final T instance;

  public ClassInstanceConfiguration(Class<? extends T> clazz, Object... arguments) {
    this.clazz = clazz;
    this.arguments = asList(arguments.clone());
    this.instance = null;
  }

  public ClassInstanceConfiguration(T instance) {
    this.instance = instance;
    @SuppressWarnings("unchecked")
    Class<? extends T> instanceClass = (Class<? extends T>) instance.getClass();
    this.clazz = instanceClass;
    this.arguments = null;
  }

  protected ClassInstanceConfiguration(ClassInstanceConfiguration<T> configuration) {
    this.instance = configuration.getInstance();
    this.clazz = configuration.getClazz();
    if (instance == null) {
      this.arguments = asList(configuration.getArguments());
    } else {
      this.arguments = null;
    }
  }

  public Class<? extends T> getClazz() {
    return clazz;
  }

  public Object[] getArguments() {
    return arguments.toArray();
  }

  public T getInstance() {
    return instance;
  }
}
