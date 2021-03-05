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

package org.ehcache.clustered.client.internal;

import org.ehcache.CachePersistenceException;

/**
 * Thrown to indicate a perpetual (non-transient) failure in a persistent cache manager.
 * <p>
 * Receiving this exception indicates that future interactions with the throwing entity will continue to fail without
 * corrective action.
 *
 * @see CachePersistenceException
 */
public class PerpetualCachePersistenceException extends CachePersistenceException {

  private static final long serialVersionUID = -5858875151420107041L;

  /**
   * Creates a {@code PerpetualCachePersistenceException} with the provided message.
   *
   * @param message information about the exception
   */
  public PerpetualCachePersistenceException(String message) {
    super(message);
  }

  /**
   * Creates a {@code PerpetualCachePersistenceException} with the provided message and cause.
   *
   * @param message information about the exception
   * @param cause the cause of this exception
   */
  public PerpetualCachePersistenceException(String message, Throwable cause) {
    super(message, cause);
  }
}
