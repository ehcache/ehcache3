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
public class ResourcePoolImpl implements ResourcePool {

  private final ResourceType type;
  private final long size;
  private final ResourceUnit unit;
  private final boolean persistent;

  public ResourcePoolImpl(ResourceType type, long size, ResourceUnit unit, boolean persistent) {
    if (!type.isPersistable() && persistent) {
      throw new IllegalStateException("Non-persistable resource cannot be configured persistent");
    }
    this.type = type;
    this.size = size;
    this.unit = unit;
    this.persistent = persistent;
  }

  public ResourceType getType() {
    return type;
  }

  public long getSize() {
    return size;
  }

  public ResourceUnit getUnit() {
    return unit;
  }

  @Override
  public boolean isPersistent() {
    return persistent;
  }

  @Override
  public String toString() {
    return "Pool {" + getSize() + " " + getUnit() + " " + getType() + (isPersistent() ? "(persistent)}" : "}");
  }

  
}
