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

package org.ehcache.clustered.common;

/**
 * Thrown to indicate a failure when creating a {@code ClusteredStore}.
 */
public class ClusteredStoreCreationException extends RuntimeException {
  private static final long serialVersionUID = 8161642579437544726L;

  public ClusteredStoreCreationException(String message) {
    super(message);
  }

  public ClusteredStoreCreationException(String message, Throwable cause) {
    super(message, cause);
  }

  public ClusteredStoreCreationException(Throwable cause) {
    super(cause);
  }
}
