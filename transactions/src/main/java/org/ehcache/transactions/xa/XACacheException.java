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
package org.ehcache.transactions.xa;

import org.ehcache.transactions.xa.internal.XAStore;

/**
 * The payload exception thrown by the cache when an {@link XAStore} has issues retrieving the transaction context.
 *
 * @author Ludovic Orban
 */
public class XACacheException extends RuntimeException {
  private static final long serialVersionUID = -6691335026252002011L;

  public XACacheException(String message) {
    super(message);
  }

  public XACacheException(String message, Throwable cause) {
    super(message, cause);
  }
}
