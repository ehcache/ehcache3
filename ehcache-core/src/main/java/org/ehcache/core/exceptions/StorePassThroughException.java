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

package org.ehcache.core.exceptions;

import org.ehcache.spi.resilience.StoreAccessException;

/**
 * A generic wrapper runtime exception that will not be caught and
 * handled at the store level.
 * <p>
 * This wrapper can be used to enable any runtime exceptions that you don't want to be caught at the store level to
 * be propagated.
 */
public class StorePassThroughException extends RuntimeException {

  private static final long serialVersionUID = -2018452326214235671L;

  /**
   * Creates an exception with the provided message and cause.
   *
   * @param message information about the exception
   * @param cause the cause of this exception
   */
  public StorePassThroughException(final String message, final Throwable cause) {
    super(message, cause);
  }

  /**
   * Creates an exception with the provided cause.
   *
   * @param cause the cause of this exception
   */
  public StorePassThroughException(final Throwable cause) {
    super(cause);
  }

  @Override
  public synchronized Throwable fillInStackTrace() {
    // skip the stack trace filling because this exception is just a placeholder and won't ever be caught outside of
    // a store
    return this;
  }

  /**
   * Helper method for handling runtime exceptions.
   * <p>
   * Returns most as {@link StoreAccessException} except for {@code StorePassThroughException}.
   * In which case if its cause is a {@link RuntimeException} it is thrown and if not it is returned
   * wrapped in a {@code StoreAccessException}.
   *
   * @param re the exception to handler
   * @return StoreAccessException to be thrown
   * @throws RuntimeException if {@code re} is a {@code StorePassThroughException} containing a {@code RuntimeException}
   */
  public static StoreAccessException handleException(Exception re) {
    if(re instanceof StorePassThroughException) {
      Throwable cause = re.getCause();
      if(cause instanceof RuntimeException) {
        throw (RuntimeException) cause;
      } else {
        return new StoreAccessException(cause);
      }
    } else {
      return new StoreAccessException(re);
    }
  }
}
