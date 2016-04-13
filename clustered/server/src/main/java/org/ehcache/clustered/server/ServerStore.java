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

import org.ehcache.clustered.common.ClusteredStoreValidationException;
import org.ehcache.clustered.common.ServerStoreConfiguration;

class ServerStore {

  private final ServerStoreConfiguration storeConfiguration;

  public ServerStore(ServerStoreConfiguration storeConfiguration) {
    this.storeConfiguration = storeConfiguration;
  }

  public void compatible(ServerStoreConfiguration desiredStoreConfiguration) {
    StringBuilder sb = new StringBuilder("Existing ServerStore configuration is not compatible with the desired configuration: ");

    boolean isCompatible;
    isCompatible = compareField(sb, "storedKeyType", storeConfiguration.getStoredKeyType(), desiredStoreConfiguration.getStoredKeyType());
    isCompatible &= compareField(sb, "storedValueType", storeConfiguration.getStoredValueType(), desiredStoreConfiguration.getStoredValueType());
    isCompatible &= compareField(sb, "actualKeyType", storeConfiguration.getActualKeyType(), desiredStoreConfiguration.getActualKeyType());
    isCompatible &= compareField(sb, "actualValueType", storeConfiguration.getActualValueType(), desiredStoreConfiguration.getActualValueType());
    isCompatible &= compareField(sb, "keySerializerType", storeConfiguration.getKeySerializerType(), desiredStoreConfiguration.getKeySerializerType());
    isCompatible &= compareField(sb, "valueSerializerType", storeConfiguration.getValueSerializerType(), desiredStoreConfiguration.getValueSerializerType());

    if (!isCompatible) {
      throw new ClusteredStoreValidationException(sb.toString());
    }
  }

  private boolean compareField(StringBuilder sb, String fieldName, String existingStoredKeyType, String desiredStoreKeyType) {
    if ((existingStoredKeyType == null && desiredStoreKeyType == null)
        || (existingStoredKeyType != null && existingStoredKeyType.equals(desiredStoreKeyType))) {
      return true;
    }

    sb.append("\n\t").append(fieldName)
        .append(" existing: ").append(existingStoredKeyType)
        .append(" desired: ").append(desiredStoreKeyType);

    return false;
  }
}
