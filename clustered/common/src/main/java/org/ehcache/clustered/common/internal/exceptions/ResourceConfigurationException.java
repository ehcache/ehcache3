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
 * Thrown to indicate some clustered resource being mis-configured.
 */
public class ResourceConfigurationException extends ClusterException {
  private static final long serialVersionUID = -8886869788915700432L;

  public ResourceConfigurationException(String message) {
    super(message);
  }

  public ResourceConfigurationException(Throwable cause) {
    super(cause);
  }

  public ResourceConfigurationException(String message, Throwable cause) {
    super(message, cause);
  }

  private ResourceConfigurationException(ResourceConfigurationException cause) {
    super(cause.getMessage(), cause);
  }

  @Override
  public ResourceConfigurationException withClientStackTrace() {
    return new ResourceConfigurationException(this);
  }
}
