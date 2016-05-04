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

package org.ehcache.core.internal.resilience;

import org.ehcache.core.spi.store.StoreAccessException;

/**
 * Generic exception used when an internal operation fails on a {@link org.ehcache.Cache} but shouldn't be
 * handled by a resilience strategy but rather rethrown to the caller.
 *
 * @deprecated This mechanism is a stop-gap solution until {@link ResilienceStrategy}
 * instances can be plugged-in.
 *
 * @author Ludovic Orban
 */
@Deprecated
public class RethrowingStoreAccessException extends StoreAccessException {

  /**
   * Create an instance of RethrowingStoreAccessException.
   * @param cause the cause RuntimeException that will be rethrown.
   */
  public RethrowingStoreAccessException(RuntimeException cause) {
    super(cause);
  }

  @Override
  public synchronized RuntimeException getCause() {
    return (RuntimeException) super.getCause();
  }
}
