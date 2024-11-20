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

import java.lang.Exception;

/**
 * Thrown when failures occur during operations on {@link org.ehcache.PersistentCacheManager}.
 *
 * @see org.ehcache.PersistentCacheManager#destroyCache(String)
 */
public class CachePersistenceException extends Exception {

  private static final long serialVersionUID = -5858875151420107040L;

  /**
   * Creates a {@code CachePersistenceException} with the provided message.
   *
   * @param message information about the exception
   */
  public CachePersistenceException(String message) {
    super(message);
  }

  /**
   * Creates a {@code CachePersistenceException} with the provided message and cause.
   *
   * @param message information about the exception
   * @param cause the cause of this exception
   */
  public CachePersistenceException(String message, Throwable cause) {
    super(message, cause);
  }
}
