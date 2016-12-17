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

package org.ehcache.core.spi.store;

/**
 * Thrown when a cache {@link Store} operation exceeds a configured time limit.
 * This exception is <b>not</b> handled by the
 * {@link org.ehcache.core.internal.resilience.ResilienceStrategy ResilienceStrategy}.
 */
public class StoreAccessTimeoutException extends RuntimeException {
  private static final long serialVersionUID = 7824475930240423944L;

  /**
   * Creates a new exception wrapping the {@link Throwable cause} passed in.
   *
   * @param cause the cause of this exception
   */
  public StoreAccessTimeoutException(Throwable cause) {
    super(cause);
  }

  /**
   * Creates a new exception wrapping the {@link Throwable cause} passed in and with the provided message.
   *
   * @param message information about the exception
   * @param cause the cause of this exception
   */
  public StoreAccessTimeoutException(String message, Throwable cause) {
    super(message, cause);
  }
}
