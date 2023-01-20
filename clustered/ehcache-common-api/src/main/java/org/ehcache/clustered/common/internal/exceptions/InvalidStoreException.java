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

package org.ehcache.clustered.common.internal.exceptions;

/**
 * Thrown to indicate an operation cannot be performed on a certain clustered store.
 */
public class InvalidStoreException extends ClusterException {
  private static final long serialVersionUID = 7717112794546075917L;

  public InvalidStoreException(String message) {
    super(message);
  }

  public InvalidStoreException(Throwable cause) {
    super(cause);
  }

  private InvalidStoreException(InvalidStoreException cause) {
    super(cause.getMessage(), cause);
  }

  @Override
  public InvalidStoreException withClientStackTrace() {
    return new InvalidStoreException(this);
  }
}
