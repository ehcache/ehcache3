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
package org.ehcache.core.internal.util;

import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.*;

public class FunctionsTest {

  @Test
  public void memoizeFunction() {
    AtomicInteger i = new AtomicInteger();
    Function<String, Integer> function = Functions.memoize(a -> i.incrementAndGet());
    assertThat(function.apply("a").intValue()).isEqualTo(1);
    assertThat(function.apply("b").intValue()).isEqualTo(1); // note that the parameter changes, the result should still be the same
  }

  @Test
  public void memoizeFunction_shouldNotMemoizeOnException() {
    AtomicBoolean firstTime = new AtomicBoolean(true);
    Function<String, Integer> function = Functions.memoize(a -> {
        if (firstTime.get()) {
          firstTime.set(false);
          throw new RuntimeException("Failed");
        }
        return 2;
      });

    assertThatExceptionOfType(RuntimeException.class).isThrownBy(() -> function.apply("a")).withMessage("Failed");
    assertThat(function.apply("a").intValue()).isEqualTo(2);
  }

  @Test
  public void memoizeBiFunction() {
    AtomicInteger i = new AtomicInteger();
    BiFunction<String, String, Integer> function = Functions.memoize((a, b) -> i.incrementAndGet());
    assertThat(function.apply("a", "a").intValue()).isEqualTo(1);
    assertThat(function.apply("b", "b").intValue()).isEqualTo(1); // note that the parameter changes, the result should still be the same
  }

  @Test
  public void memoizeBiFunction_shouldNotMemoizeOnException() {
    AtomicBoolean firstTime = new AtomicBoolean(true);
    BiFunction<String, String, Integer> function = Functions.memoize((a, b) -> {
      if (firstTime.get()) {
        firstTime.set(false);
        throw new RuntimeException("Failed");
      }
      return 2;
    });

    assertThatExceptionOfType(RuntimeException.class).isThrownBy(() -> function.apply("a", "b")).withMessage("Failed");
    assertThat(function.apply("a", "b").intValue()).isEqualTo(2);
  }
}
