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

import org.ehcache.core.spi.function.BiFunction;
import org.ehcache.core.spi.function.Function;

/**
 * A set of utilities methods and Classes around Functions
 *
 * @author Alex Snaps
 */
public class Functions {

  /**
   * Will transform the passed in {@link Function} in to an apply once and only once Function.
   * Irrespectively of the argument passed in! And isn't thread safe. Basically acts as a dumb cache.
   *
   * @param f the function to memoize
   * @param <A> the function's input param type
   * @param <T> the function's output type
   * @return the memoized function
   */
  public static <A, T> Function<A, T> memoize(Function<A, T> f) {
    return new MemoizingFunction<A, T>(f);
  }

  /**
   * Will transform the passed in {@link BiFunction} in to an apply once and only once BiFunction.
   * Irrespectively of the arguments passed in! And isn't thread safe. Basically acts as a dumb cache.
   *
   * @param f the bifunction to memoize
   * @param <A> the bifunction's first input param type
   * @param <B> the bifunction's second input param type
   * @param <T> the function's output type
   * @return the memoized bifunction
   */
  public static <A, B, T> BiFunction<A, B, T> memoize(BiFunction<A, B, T> f) {
    return new MemoizingBiFunction<A, B, T>(f);
  }

  private static final class MemoizingFunction<A, T> implements Function<A, T> {

    private final Function<A, T> function;
    private boolean computed;
    private T value;

    private MemoizingFunction(final Function<A, T> function) {
      this.function = function;
    }

    @Override
    public T apply(final A a) {
      if (computed) {
        return value;
      }
      value = function.apply(a);
      computed = true;
      return value;
    }
  }

  private static final class MemoizingBiFunction<A, B, T> implements BiFunction<A, B, T> {

    private final BiFunction<A, B, T> function;
    private boolean computed;
    private T value;

    private MemoizingBiFunction(final BiFunction<A, B, T> function) {
      this.function = function;
    }

    @Override
    public T apply(final A a, final B b) {
      if (computed) {
        return value;
      }
      computed = true;
      value = function.apply(a, b);
      return value;
    }
  }
}
