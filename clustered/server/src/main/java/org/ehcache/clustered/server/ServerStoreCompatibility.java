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

package org.ehcache.clustered.server;

import org.ehcache.clustered.common.internal.ServerStoreConfiguration;
import org.ehcache.clustered.common.internal.exceptions.InvalidServerStoreConfigurationException;

/**
 * Provides configuration compatibility checks for {@link ServerStoreConfiguration}
 * between client and server specifications.
 */
public class ServerStoreCompatibility {

  /**
   * Ensure compatibility of a client {@link ServerStoreConfiguration} with an existing
   * server-side {@code ServerStoreConfiguration}.
   *
   * @param serverConfiguration the existing server-side {@code ServerStoreConfiguration}
   * @param clientConfiguration the desired client-side {@code ServerStoreConfiguration}
   *
   * @throws InvalidServerStoreConfigurationException if {@code clientConfiguration} is not compatible with
   *          {@code serverConfiguration}
   */
  public void verify(ServerStoreConfiguration serverConfiguration, ServerStoreConfiguration clientConfiguration)
      throws InvalidServerStoreConfigurationException {
    StringBuilder sb = new StringBuilder("Existing ServerStore configuration is not compatible with the desired configuration: ");

    if (!serverConfiguration.isCompatible(clientConfiguration, sb)) {
      throw new InvalidServerStoreConfigurationException(sb.toString());
    }
  }
}
