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

package org.ehcache.spi.resilience;

import java.util.concurrent.CompletionException;

/**
 * A wrapper Runtime exception used when don't want to handle the checkedException an internal operation fails on a {@link org.ehcache.Cache}.
 *
 * @author nnares
 */
public class StoreAccessRuntimeException extends RuntimeException {

  private static final long serialVersionUID = 6249505400891654776L;

  /**
   * Creates a new exception wrapping the {@link Throwable cause} passed in.
   *
   * @param cause the cause of this exception
   */
  public StoreAccessRuntimeException(Throwable cause) {
    super(cause);
  }

  /**
   * Creates a new exception wrapping the {@link Throwable cause} passed in and with the provided message.
   *
   * @param message information about the exception
   * @param cause the cause of this exception
   */
  public StoreAccessRuntimeException(String message, Throwable cause) {
    super(message, cause);
  }

  /**
   * Creates a new exception with the provided message.
   *
   * @param message information about the exception
   */
  public StoreAccessRuntimeException(String message) {
    super(message);
  }

  /**
   * Wrapped the received {@link java.lang.RuntimeException} to {@link org.ehcache.spi.resilience.StoreAccessException},
   * so that received {@link java.lang.RuntimeException} can reach {@link org.ehcache.spi.resilience.ResilienceStrategy}
   *
   * @param re a {@link java.lang.RuntimeException} that is being handled
   * @return {@link org.ehcache.spi.resilience.StoreAccessException} a type in which wrapping the received {@link java.lang.RuntimeException}
   */
  public static StoreAccessException handleRuntimeException(RuntimeException re) {

    if (re instanceof StoreAccessRuntimeException || re instanceof CompletionException) {
      Throwable cause = re.getCause();
      if (cause instanceof RuntimeException) {
        return new StoreAccessException(cause);
      }
    }
    return new StoreAccessException(re);
  }

}
