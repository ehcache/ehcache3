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

import org.junit.AssumptionViolatedException;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.Collections.max;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static org.junit.Assert.assertTrue;

public class TestRetryer<T, R> implements TestRule, Supplier<R> {

  private static final Logger LOGGER = LoggerFactory.getLogger(TestRetryer.class);

  private final Function<T, R> mapper;
  private final Set<OutputIs> outputIs;
  private final Stream<T> inputs;

  private final AtomicReference<T> inputRef = new AtomicReference<>();
  private final AtomicReference<R> outputRef = new AtomicReference<>();

  private volatile boolean isClassRule = false;
  private volatile boolean isRule = false;

  private volatile boolean terminalAttempt = false;
  private volatile Map<Description, Object> attemptResults = Collections.emptyMap();

  private final Map<Description, Throwable> accumulatedFailures = new ConcurrentHashMap<>();

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
    this.inputs = values.map(Objects::requireNonNull);
    this.mapper = requireNonNull(mapper);
    this.outputIs = requireNonNull(outputIs);
  }

  @Override
  public Statement apply(Statement base, Description description) {
    if (description.isTest()) {
      isRule = true;
      if (!isClassRule) {
        throw new AssertionError(getClass().getSimpleName() + " must be annotated with both @ClassRule and @Rule");
      }
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
          } finally {
            attemptResults.putIfAbsent(description, "PASSED");
          }
        }
      };
    } else {
      isClassRule = true;
      return new Statement() {
        @Override
        public void evaluate() throws Throwable {
          Iterator<T> iterator = inputs.iterator();
          while (iterator.hasNext()) {
            T input = iterator.next();
            terminalAttempt = !iterator.hasNext();
            attemptResults = new ConcurrentHashMap<>();
            LOGGER.debug("{}: attempting with input value {}", description, input);
            assertTrue(inputRef.compareAndSet(null, input));
            try {
              R output = mapper.apply(input);
              LOGGER.debug("{}: input {} maps to {}", description, input, output);
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
            if (!isRule) {
              throw new AssertionError(TestRetryer.this.getClass().getSimpleName() + " must be annotated with both @ClassRule and @Rule");
            } else if (attemptResults.values().stream().noneMatch(Throwable.class::isInstance)) {
              LOGGER.debug("{}: successful with input value {}", description, input);
              return;
            } else {
              LOGGER.info("{}: failed with input value {}\n{}", description, input,
                attemptResults.entrySet().stream().map(e -> {
                  String testMethodHeader = e.getKey().getMethodName() + ": ";
                  return indent(testMethodHeader + e.getValue().toString(), 4, 4 + testMethodHeader.length());
                }).collect(joining("\n"))
              );
            }
          }
        }
      };
    }
  }

  private Throwable handleTestException(Description description, Throwable t)  {
    Throwable failure = (Throwable) attemptResults.merge(description, t, (a, b) -> {
      if (a instanceof Throwable) {
        ((Throwable) a).addSuppressed((Throwable) b);
        return a;
      } else {
        return b;
      }
    });
    Throwable merged = accumulatedFailures.merge(description, t, (a, b) -> {
      b.addSuppressed(a);
      return b;
    });
    if (isTerminalAttempt()) {
      return merged;
    } else {
      return new AssumptionViolatedException("Failure for input parameter: " + input(), failure);
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

  private static CharSequence indent(String string, Integer ... indent) {
    char[] chars = new char[max(asList(indent))];
    Arrays.fill(chars, ' ');
    String indentStrings = new String(chars);
    String[] strings = string.split("(?m)^");
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < strings.length; i++) {
      if (i < indent.length) {
        sb.append(indentStrings, 0, indent[i]);
      } else {
        sb.append(indentStrings, 0, indent[indent.length - 1]);
      }
      sb.append(strings[i]);
    }
    return sb;
  }
}
