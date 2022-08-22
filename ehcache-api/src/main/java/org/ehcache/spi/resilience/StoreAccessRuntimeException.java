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

/**
 * A wrapper Runtime exception used when don't want to handle the checkedException an internal operation fails on a {@link org.ehcache.Cache}.
 *
 * @author nnares
 */
public class StoreAccessRuntimeException extends RuntimeException {

  private static final long serialVersionUID = 1L;

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

  public static StoreAccessException handleRuntimeException(Exception re) {
    if(re instanceof StoreAccessRuntimeException) {
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
