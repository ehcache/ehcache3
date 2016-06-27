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

import org.ehcache.clustered.common.Consistency;
import org.ehcache.clustered.common.ServerStoreConfiguration;
import org.ehcache.clustered.common.ServerStoreConfiguration.PoolAllocation;
import org.ehcache.clustered.common.exceptions.InvalidServerStoreConfigurationException;

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

    boolean isCompatible;

    PoolAllocation serverPoolAllocation = serverConfiguration.getPoolAllocation();
    PoolAllocation clientPoolAllocation = clientConfiguration.getPoolAllocation();
    isCompatible = compareField(sb, "resourcePoolType",
        serverPoolAllocation.getClass().getName(),
        clientPoolAllocation.getClass().getName());
    if(isCompatible) {
      if (serverPoolAllocation instanceof PoolAllocation.Fixed) {
        PoolAllocation.Fixed serverFixedAllocation = (PoolAllocation.Fixed)serverPoolAllocation;
        PoolAllocation.Fixed clientFixedAllocation = (PoolAllocation.Fixed)clientPoolAllocation;
        if (compareField(sb, "resourcePoolFixedResourceName",
            serverFixedAllocation.getResourceName(),
            clientFixedAllocation.getResourceName())) {
          if (clientFixedAllocation.getSize() != serverFixedAllocation.getSize()) {
            appendFault(sb, "resourcePoolFixedSize", serverFixedAllocation.getSize(), clientFixedAllocation.getSize());
            isCompatible &= false;
          }
        } else {
          isCompatible &= false;
        }
      } else if (serverPoolAllocation instanceof PoolAllocation.Shared) {
        isCompatible &= compareField(sb, "resourcePoolSharedPoolName",
            ((PoolAllocation.Shared)serverPoolAllocation).getResourcePoolName(),
            ((PoolAllocation.Shared)clientPoolAllocation).getResourcePoolName());
      }
    }
    isCompatible &= compareField(sb, "storedKeyType", serverConfiguration.getStoredKeyType(), clientConfiguration.getStoredKeyType());
    isCompatible &= compareField(sb, "storedValueType", serverConfiguration.getStoredValueType(), clientConfiguration.getStoredValueType());
    isCompatible &= compareField(sb, "actualKeyType", serverConfiguration.getActualKeyType(), clientConfiguration.getActualKeyType());
    isCompatible &= compareField(sb, "actualValueType", serverConfiguration.getActualValueType(), clientConfiguration.getActualValueType());
    isCompatible &= compareField(sb, "keySerializerType", serverConfiguration.getKeySerializerType(), clientConfiguration.getKeySerializerType());
    isCompatible &= compareField(sb, "valueSerializerType", serverConfiguration.getValueSerializerType(), clientConfiguration.getValueSerializerType());
    isCompatible &= compareConsistencyField(sb, serverConfiguration.getConsistency(), clientConfiguration.getConsistency());

    if (!isCompatible) {
      throw new InvalidServerStoreConfigurationException(sb.toString());
    }
  }

  private static boolean compareConsistencyField(StringBuilder sb, Consistency serverConsistencyValue, Consistency clientConsistencyValue) {
    if((serverConsistencyValue == null && clientConsistencyValue == null)
        || (serverConsistencyValue != null && serverConsistencyValue.equals(clientConsistencyValue))) {
      return true;
    }

    appendFault(sb, "consistencyType", serverConsistencyValue, clientConsistencyValue);
    return false;
  }

  private static boolean compareField(StringBuilder sb, String fieldName, String serverConfigValue, String clientConfigValue) {
    if ((serverConfigValue == null && clientConfigValue == null)
        || (serverConfigValue != null && serverConfigValue.equals(clientConfigValue))) {
      return true;
    }

    appendFault(sb, fieldName, serverConfigValue, clientConfigValue);
    return false;
  }

  private static void appendFault(StringBuilder sb, String fieldName, Object serverConfigValue, Object clientConfigValue) {
    sb.append("\n\t").append(fieldName)
        .append(" existing: ").append(serverConfigValue)
        .append(", desired: ").append(clientConfigValue);
  }
}
