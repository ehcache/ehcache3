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
package org.ehcache.testing;

import org.junit.runner.Description;
import org.junit.runner.Request;
import org.junit.runner.manipulation.Filter;
import org.junit.runner.notification.RunNotifier;
import org.junit.runners.ParentRunner;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.TestClass;

import java.io.IOException;
import java.io.InputStream;
import java.lang.annotation.Annotation;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.reflect.Modifier;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

public class ExternalTests extends ParentRunner<Request> {

  private final List<Request> children;

  public ExternalTests(Class<?> testClass) throws InitializationError, IOException, ClassNotFoundException {
    super(testClass);
    this.children = singletonList(parseRequest(getTestClass(), parseFilter(getTestClass())));
  }

  @Override
  protected List<Request> getChildren() {
    return children;
  }

  @Override
  protected Description describeChild(Request child) {
    return child.getRunner().getDescription();
  }

  @Override
  protected void runChild(Request child, RunNotifier notifier) {
    child.getRunner().run(notifier);
  }

  private static Filter parseFilter(TestClass testClass) {
    return groupAnnotations(testClass, Ignore.class, Ignores.class).stream().map(IgnoreFilter::ignore).reduce(Filter.ALL, Filter::intersect);
  }

  private static class IgnoreFilter extends Filter {

    private final Ignore ignore;

    public static Filter ignore(Ignore ignore) {
      return new IgnoreFilter(ignore);
    }

    private IgnoreFilter(Ignore ignore) {
      this.ignore = ignore;
    }

    @Override
    public boolean shouldRun(Description description) {
      if (ignore.value().equals(description.getTestClass())) {
        if (ignore.method().isEmpty()) {
          return false;
        } else {
          return !ignore.method().equals(description.getMethodName());
        }
      } else {
        return true;
      }
    }

    @Override
    public String describe() {
      if (ignore.method().isEmpty()) {
        return "Ignore " + ignore.value();
      } else {
        return "Ignore " + ignore.value() + "#" + ignore.method();
      }
    }
  }

  private static Request parseRequest(TestClass testClass, Filter filter) throws IOException, ClassNotFoundException {
    List<From> froms = groupAnnotations(testClass, From.class, Froms.class);
    List<Test> tests = groupAnnotations(testClass, Test.class, Tests.class);

    List<Class<?>> classes = new ArrayList<>();

    for (From from : froms) {
      URL location = from.value().getProtectionDomain().getCodeSource().getLocation();
      try (InputStream is = location.openStream(); JarInputStream jis = new JarInputStream(is)) {
        while (true) {
          JarEntry entry = jis.getNextJarEntry();
          if (entry == null) {
            break;
          } else if (entry.getName().endsWith("Test.class")) {
            classes.add(Class.forName(entry.getName().replace(".class", "").replace('/', '.')));
          }
        }
      }
    }
    for (Test test : tests) {
      classes.add(test.value());
    }

    return Request.classes(classes.stream()
      .filter(c -> Modifier.isPublic(c.getModifiers()) && !Modifier.isAbstract(c.getModifiers()))
      .filter(c -> !c.getSimpleName().startsWith("Abstract")).toArray(Class[]::new))
      .filterWith(filter);

  }

  @SuppressWarnings("unchecked")
  private static <T extends Annotation, WT extends Annotation> List<T> groupAnnotations(TestClass testClass, Class<T> annoType, Class<WT> wrapperType) {
    try {
      List<T> annotations = new ArrayList<>();

      WT testsAnn = testClass.getAnnotation(wrapperType);
      if (testsAnn != null) {
        annotations.addAll(asList((T[]) wrapperType.getMethod("value").invoke(testsAnn)));
      }

      T singularAnn = testClass.getAnnotation(annoType);
      if (singularAnn != null) {
        annotations.add(singularAnn);
      }
      return annotations;
    } catch (ReflectiveOperationException e) {
      throw new IllegalArgumentException(e);
    }
  }


  @Retention(RetentionPolicy.RUNTIME)
  @Repeatable(Tests.class)
  public @interface Test {

    Class<?> value();
  }
  @Retention(RetentionPolicy.RUNTIME)
  public @interface Tests {
    Test[] value();
  }

  @Retention(RetentionPolicy.RUNTIME)
  @Repeatable(Froms.class)
  public @interface From {

    Class<?> value();
  }
  @Retention(RetentionPolicy.RUNTIME)
  public @interface Froms {
    From[] value();
  }

  @Retention(RetentionPolicy.RUNTIME)
  @Repeatable(Ignores.class)
  public @interface Ignore {
    Class<?> value();

    String method() default "";
  }

  @Retention(RetentionPolicy.RUNTIME)
  public @interface Ignores {
    Ignore[] value();
  }
}
