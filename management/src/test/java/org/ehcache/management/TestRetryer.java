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

import org.junit.AssumptionViolatedException;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.util.EnumSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;
import static org.junit.Assert.assertTrue;

public class TestRetryer<T, R> implements TestRule {

  private final Function<T, R> mapper;
  private final Set<OutputIs> outputIs;
  private final Stream<T> inputs;

  private final AtomicReference<T> inputRef = new AtomicReference<>();
  private final AtomicReference<R> outputRef = new AtomicReference<>();
  private volatile boolean terminalAttempt = false;
  private volatile boolean failedAttempt = false;

  private final Map<Description, Throwable> failures = new ConcurrentHashMap<>();


  @SafeVarargs @SuppressWarnings("varargs") // Creating a stream from an array is safe
  public static <T> TestRetryer<T, T> tryValues(T... values) {
    return tryValues(Stream.of(values));
  }

  public static <T> TestRetryer<T, T> tryValues(Stream<T> values) {
    return tryValues(values, Function.identity());
  }

  public static <T, R> TestRetryer<T, R> tryValues(Stream<T> values, Function<T, R> mapper) {
    return tryValues(values, mapper, EnumSet.noneOf(OutputIs.class));
  }

  public static <T, R> TestRetryer<T, R> tryValues(Stream<T> values, Function<T, R> mapper, Set<OutputIs> outputIs) {
    return new TestRetryer<>(values, mapper, outputIs);
  }

  private TestRetryer(Stream<T> values, Function<T, R> mapper, Set<OutputIs> outputIs) {
    this.mapper = mapper;
    this.outputIs = outputIs;
    this.inputs = values;
  }

  @Override
  public Statement apply(Statement base, Description description) {
    if (description.isTest()) {
      Statement target;
      R output = get();
      if (output instanceof TestRule && outputIs.contains(OutputIs.RULE)) {
        target = ((TestRule) output).apply(base, description);
      } else {
        target = base;
      }

      return new Statement() {
        @Override
        public void evaluate() throws Throwable {
          try {
            target.evaluate();
          } catch (Throwable t) {
            throw handleTestException(description, t);
          }
        }
      };
    } else {
      return new Statement() {
        @Override
        public void evaluate() throws Throwable {
          Iterator<T> iterator = inputs.iterator();
          while (iterator.hasNext()) {
            T input = iterator.next();
            terminalAttempt = !iterator.hasNext();
            failedAttempt = false;
            assertTrue(inputRef.compareAndSet(null, input));
            try {
              R output = mapper.apply(input);
              assertTrue(outputRef.compareAndSet(null, output));
              try {
                if (output instanceof TestRule && outputIs.contains(OutputIs.CLASS_RULE)) {
                  ((TestRule) output).apply(base, description).evaluate();
                } else {
                  base.evaluate();
                }
              } catch (Throwable t) {
                throw handleTestException(description, t);
              } finally {
                assertTrue(outputRef.compareAndSet(output, null));
              }
            } finally {
              assertTrue(inputRef.compareAndSet(input, null));
            }

            if (!failedAttempt) {
              return;
            }
          }
        }
      };
    }
  }

  private Throwable handleTestException(Description description, Throwable t)  {
    failedAttempt = true;
    Throwable merged = failures.merge(description, t, (a, b) -> {
      b.addSuppressed(a);
      return b;
    });
    if (isTerminalAttempt()) {
      return merged;
    } else {
      return new AssumptionViolatedException("Failure for input parameter: " + input(), merged);
    }
  }

  public T input() {
    return requireNonNull(inputRef.get());
  }

  public R get() {
    return requireNonNull(outputRef.get());
  }

  private boolean isTerminalAttempt() {
    return terminalAttempt;
  }

  public enum OutputIs {
    RULE, CLASS_RULE;
  }
}
