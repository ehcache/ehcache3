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
 * {@link java.lang.Exception} while loading a value for a given key.
 *
 * @author Alex Snaps
 */
public class CacheLoadingException extends RuntimeException {

  private static final long serialVersionUID = 4794738044299044587L;

  /**
   * Constructs a CacheLoadingException
   */
  public CacheLoadingException() {
    super();
  }

  /**
   * Constructs a CacheLoadingException with the provided message.
   *
   * @param message the detail message
   */
  public CacheLoadingException(final String message) {
    super(message);
  }

  /**
   * Constructs a CacheLoadingException wrapping the {@link Throwable cause} passed in
   * and with the provided message.
   *
   * @param message the detail message
   * @param cause   the root cause
   */
  public CacheLoadingException(final String message, final Throwable cause) {
    super(message, cause);
  }

  /**
   * Constructs a CacheLoadingException wrapping the {@link Throwable cause} passed in.
   *
   * @param cause the root cause
   */
  public CacheLoadingException(final Throwable cause) {
    super(cause);
  }

}
