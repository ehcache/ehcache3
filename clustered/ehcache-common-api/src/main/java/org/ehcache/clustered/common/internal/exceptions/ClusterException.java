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

public abstract class ClusterException extends Exception {
  private static final long serialVersionUID = 38615046229882871L;

  public ClusterException(String message) {
    super(message);
  }

  public ClusterException(String message, Throwable cause) {
    super(message, cause);
  }

  public ClusterException(Throwable cause) {
    super(cause);
  }

  /**
   * Makes a copy of this {@code ClusterException}, retaining the type, in the current
   * client context.  This method is used to prepare exceptions originating in the server to be
   * re-thrown in the client.
   *
   * @return a new {@code ClusterException} of {@code this} subtype with {@code this}
   *      instance as the cause
   */
  public abstract ClusterException withClientStackTrace();
}
