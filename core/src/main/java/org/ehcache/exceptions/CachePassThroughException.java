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

package org.ehcache.exceptions;

/**
 * A generic wrapper runtime exception that will not be caught and
 * handled at the store level. This wrapper can be used to leak any
 * runtime exceptions that you don't want to be caught at the store level.
 * 
 * @author Albin Suresh
 */
public class CachePassThroughException extends RuntimeException {

  private static final long serialVersionUID = -2018452326214235671L;

  /**
   * Creates an exception with the provided message and cause.
   *
   * @param message information about the exception
   * @param cause the cause of this exception
   */
  public CachePassThroughException(final String message, final Throwable cause) {
    super(message, cause);
  }

  /**
   * Creates an exception with the provided cause.
   *
   * @param cause the cause of this exception
   */
  public CachePassThroughException(final Throwable cause) {
    super(cause);
  }
}
