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
package org.ehcache.config;

/**
 * @author Ludovic Orban
 */
class ResourcePoolImpl implements ResourcePool {

  private final String type;
  private final String unit;
  private final String value;

  public ResourcePoolImpl(String type, String unit, String value) {
    this.type = type;
    this.unit = unit;
    this.value = value;
  }

  public String getType() {
    return type;
  }

  public String getUnit() {
    return unit;
  }

  public String getValue() {
    return value;
  }

}
