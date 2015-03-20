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
public interface ResourcePool {

  /**
   * Get the type of the tracked resource.
   *
   * @return the type.
   */
  public String getType();

  /**
   * Get the unit in which the resource is measured.
   *
   * @return the unit.
   */
  public String getUnit();

  /**
   * Get the value measuring the pool size.
   *
   * @return the value.
   */
  public String getValue();

}
