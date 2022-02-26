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
 * A {@link ResourcePool} that is explicitly sized.
 */
public interface SizedResourcePool extends ResourcePool {

  /**
   * Gets the size of this pool.
   *
   * @return the size
   */
  long getSize();

  /**
   * Gets the unit in which the resource is sized.
   *
   * @return the size unit
   */
  ResourceUnit getUnit();
}
