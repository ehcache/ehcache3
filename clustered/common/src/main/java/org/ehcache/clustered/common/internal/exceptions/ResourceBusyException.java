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
 * Thrown to indicate a clustered entity is busy.
 */
public class ResourceBusyException extends ClusterException {
  private static final long serialVersionUID = 3830614247618106338L;

  public ResourceBusyException(String message) {
    super(message);
  }

  public ResourceBusyException(Throwable cause) {
    super(cause);
  }

  public ResourceBusyException(String message, Throwable cause) {
    super(message, cause);
  }

  private ResourceBusyException(ResourceBusyException cause) {
    super(cause.getMessage(), cause);
  }

  @Override
  public ResourceBusyException withClientStackTrace() {
    return new ResourceBusyException(this);
  }
}
