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

package org.ehcache.clustered;

/**
 * Configuration properties for {@code ServerStore}.
 */
public class ServerStoreConfiguration {
  private final String storedKeyType;
  private final String storedValueType;
  private final String actualKeyType;
  private final String actualValueType;
  private final String keySerializerType;
  private final String valueSerializerType;

  public ServerStoreConfiguration(String storedKeyType, String storedValueType, String actualKeyType, String actualValueType, String keySerializerType, String valueSerializerType) {
    this.storedKeyType = storedKeyType;
    this.storedValueType = storedValueType;
    this.actualKeyType = actualKeyType;
    this.actualValueType = actualValueType;
    this.keySerializerType = keySerializerType;
    this.valueSerializerType = valueSerializerType;
  }

  public String getStoredKeyType() {
    return storedKeyType;
  }

  public String getStoredValueType() {
    return storedValueType;
  }

  public String getActualKeyType() {
    return actualKeyType;
  }

  public String getActualValueType() {
    return actualValueType;
  }

  public String getKeySerializerType() {
    return keySerializerType;
  }

  public String getValueSerializerType() {
    return valueSerializerType;
  }
}
