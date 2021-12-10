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

package org.ehcache.xml;

import org.ehcache.config.ResourcePool;
import org.ehcache.config.ResourceType;

public class BazResource implements ResourcePool {

  @Override
  public ResourceType<?> getType() {
    return Type.BAZ_TYPE;
  }

  @Override
  public boolean isPersistent() {
    return false;
  }

  @Override
  public void validateUpdate(ResourcePool newPool) {

  }

  public static class Type implements ResourceType<BazResource> {

    public static final Type BAZ_TYPE = new Type();

    @Override
    public Class<BazResource> getResourcePoolClass() {
      return BazResource.class;
    }

    @Override
    public boolean isPersistable() {
      return false;
    }

    @Override
    public boolean requiresSerialization() {
      return false;
    }

    @Override
    public int getTierHeight() {
      return 0;
    }
  }
}
