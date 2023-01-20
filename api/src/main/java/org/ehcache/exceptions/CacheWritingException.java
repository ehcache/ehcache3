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

import org.ehcache.spi.loaderwriter.CacheLoaderWriter;

/**
 * Exception thrown by a {@link org.ehcache.Cache} when the {@link CacheLoaderWriter} it uses threw an
 * {@link java.lang.Exception} while writing a value for a given key
 *
 * @author Alex Snaps
 */
public class CacheWritingException extends RuntimeException {

  private static final long serialVersionUID = -3547750364991417531L;

  /**
   * Constructs a CacheWritingException
   *
   */
  public CacheWritingException() {
    super();
  }

  /**
   * Constructs a CacheWritingException with the provided message.
   *
   * @param message the detail message
   */
  public CacheWritingException(final String message) {
    super(message);
  }

  /**
   * Constructs a CacheWritingException wrapping the {@link Throwable cause} passed in
   * and with the provided message.
   *
   * @param message the detail message
   * @param cause   the root cause
   */
  public CacheWritingException(final String message, final Throwable cause) {
    super(message, cause);
  }

  /**
   * Constructs a CacheWritingException wrapping the {@link Throwable cause} passed in.
   *
   * @param cause the root cause
   */
  public CacheWritingException(final Throwable cause) {
    super(cause);
  }
}
