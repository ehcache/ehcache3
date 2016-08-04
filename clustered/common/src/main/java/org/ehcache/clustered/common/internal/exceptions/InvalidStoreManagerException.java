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
 * Thrown to indicate an operation cannot be performed on a certain clustered store manager.
 */
public class InvalidStoreManagerException extends ClusterException {
  private static final long serialVersionUID = 8706722895064395931L;

  public InvalidStoreManagerException(String message) {
    super(message);
  }

  public InvalidStoreManagerException(Throwable cause) {
    super(cause);
  }

  private InvalidStoreManagerException(InvalidStoreManagerException cause) {
    super(cause.getMessage(), cause);
  }

  @Override
  public InvalidStoreManagerException withClientStackTrace() {
    return new InvalidStoreManagerException(this);
  }
}
