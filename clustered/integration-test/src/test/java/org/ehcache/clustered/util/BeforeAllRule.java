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
package org.ehcache.clustered.util;

import org.junit.rules.ExternalResource;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.lang.reflect.Method;
import java.util.Comparator;
import java.util.List;
import java.util.WeakHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author Mathieu Carbou
 */
public class BeforeAllRule extends ExternalResource {

  private static WeakHashMap<Class<?>, Boolean> ran = new WeakHashMap<>();

  private final Object test;

  public BeforeAllRule(Object test) {
    this.test = test;
  }

  @Override
  public Statement apply(Statement base, Description description) {
    ran.putIfAbsent(description.getTestClass(), Boolean.FALSE);
    return super.apply(base, description);
  }

  @Override
  protected void before() throws Throwable {
    if (ran.replace(test.getClass(), Boolean.FALSE, Boolean.TRUE)) {
      List<Method> list = Stream.of(test.getClass().getMethods())
        .filter(m -> m.isAnnotationPresent(BeforeAll.class))
        .sorted(Comparator.comparing(Method::getName))
        .collect(Collectors.toList());
      for (Method method : list) {
        method.invoke(test);
      }
    }
  }

}
