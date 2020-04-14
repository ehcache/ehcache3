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
package org.ehcache.clustered;

import org.hamcrest.Matchers;
import org.junit.AssumptionViolatedException;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.MultipleFailureException;
import org.junit.runners.model.Statement;

import java.util.Arrays;
import java.util.Deque;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;
import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.toList;
import static org.ehcache.clustered.TestRetryer.OutputIs.CLASS_RULE;
import static org.ehcache.clustered.TestRetryer.OutputIs.RULE;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertThat;
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


  @SafeVarargs @SuppressWarnings("varargs")
  public TestRetryer(Function<T, R> mapper, Set<OutputIs> isRule, T... values) {
    this(mapper, isRule, Arrays.stream(values));
  }

  public TestRetryer(Function<T, R> mapper, Set<OutputIs> outputIs, Stream<T> values) {
    this.mapper = mapper;
    this.outputIs = outputIs;
    this.inputs = values;
  }

  @Override
  public Statement apply(Statement base, Description description) {
    if (description.isTest()) {
      Statement target;
      R output = getOutput();
      if (output instanceof TestRule && outputIs.contains(RULE)) {
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
                if (output instanceof TestRule && outputIs.contains(CLASS_RULE)) {
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
      return new AssumptionViolatedException("Failure for input parameter: " + getInput(), merged);
    }
  }

  public T getInput() {
    return requireNonNull(inputRef.get());
  }

  public R getOutput() {
    return requireNonNull(outputRef.get());
  }

  private boolean isTerminalAttempt() {
    return terminalAttempt;
  }

  public enum OutputIs {
    RULE, CLASS_RULE;
  }
}
