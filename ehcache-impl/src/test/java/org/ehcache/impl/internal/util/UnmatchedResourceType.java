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

package org.ehcache.impl.internal.util;

import org.ehcache.config.ResourcePool;
import org.ehcache.config.ResourceType;

/**
 * UnmatchedResourceType
 */
public class UnmatchedResourceType implements ResourceType<ResourcePool> {
  @Override
  public Class<ResourcePool> getResourcePoolClass() {
    return ResourcePool.class;
  }

  @Override
  public boolean isPersistable() {
    return true;
  }

  @Override
  public boolean requiresSerialization() {
    return true;
  }

  @Override
  public int getTierHeight() {
    return 10;
  }
}
