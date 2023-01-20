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

package org.ehcache;

/**
 * Indicates an unrecoverable failure occurred during iteration.
 */
public class CacheIterationException extends RuntimeException {

  private static final long serialVersionUID = -2008756317259206440L;

  /**
   * Creates a {@code CacheIterationException}.
   */
  public CacheIterationException() {
  }

  /**
   * Creates a {@code CacheIterationException} with the provided message.
   *
   * @param message information about the exception
   */
  public CacheIterationException(final String message) {
    super(message);
  }

  /**
   * Creates a {@code CacheIterationException} with the provided message and cause.
   *
   * @param message information about the exception
   * @param cause the cause of this exception
   */
  public CacheIterationException(final String message, final Throwable cause) {
    super(message, cause);
  }

  /**
   * Creates a {@code CacheIterationException} with the provided cause.
   *
   * @param cause the cause of this exception
   */
  public CacheIterationException(final Throwable cause) {
    super(cause);
  }
}
