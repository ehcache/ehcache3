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

package org.ehcache.impl.config;

import org.ehcache.config.ResourcePool;
import org.ehcache.config.ResourceType;

public class SharedResourcePool<T extends ResourceType<?>> extends AbstractResourcePool<ResourcePool, ResourceType.SharedResource<T>> {

  /**
   * Creates a {@code AbstractResourcePool} instance.
   *
   * @param type       the non-{@code null} {@code ResourceType}
   * @param persistent whether or not this {@code ResourcePool} is persistent
   */
  public SharedResourcePool(T type, boolean persistent) {
    super(new ResourceType.SharedResource<>(type), persistent);
  }
}
