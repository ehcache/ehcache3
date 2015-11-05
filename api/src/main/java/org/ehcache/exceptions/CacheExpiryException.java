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
 * Exception thrown by a {@link org.ehcache.Cache} when the {@link org.ehcache.expiry.Expiry} it uses threw an
 * {@link java.lang.Exception} while fetching expiry for a given key-value mapping
 * 
 * @author rism
 */
public class CacheExpiryException extends RuntimeException {
  
  private static final long serialVersionUID = -1917750364991417531L;

  /**
   * Constructs a CacheExpiryException 
   *
   */
  public CacheExpiryException() {
    super();
  }

  /**
   * Constructs a CacheExpiryException with the provided message.
   *
   * @param message the detail message.
   */
  public CacheExpiryException(final String message) {
    super(message);
  }

  /**
   * Constructs a CacheExpiryException wrapping the {@link Throwable cause} passed in 
   * and with the provided message.
   *
   * @param message the detail message.
   * @param cause   the detail message.
   */
  public CacheExpiryException(final String message, final Throwable cause) {
    super(message, cause);
  }

  /**
   * Constructs a CacheExpiryException wrapping the {@link Throwable cause} passed in.
   *
   * @param cause the detail message.
   */
  public CacheExpiryException(final Throwable cause) {
    super(cause);
  }
}
