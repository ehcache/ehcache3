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

import java.util.HashMap;
import java.util.Map;

/**
 * @author Ludovic Orban
 */
public class ResourcePoolsBuilder {

  private final Map<String, ResourcePool> resourcePools = new HashMap<String, ResourcePool>();

  private ResourcePoolsBuilder() {
  }

  public static ResourcePoolsBuilder newResourcePoolsBuilder() {
    return new ResourcePoolsBuilder();
  }

  public ResourcePoolsBuilder with(String type) {
    resourcePools.put(type, new ResourcePoolImpl(type, null, null));
    return this;
  }

  public ResourcePoolsBuilder with(String type, String unit, String value) {
    resourcePools.put(type, new ResourcePoolImpl(type, unit, value));
    return this;
  }

  public ResourcePools build() {
    return new ResourcePoolsImpl(resourcePools);
  }

}
