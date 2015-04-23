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

package org.ehcache.spi;

/**
 * Internal interface to control the persistence lifecycle of
 * persistence-capable entities.
 *
 * @author Ludovic Orban
 */
public interface Persistable {

  /**
   * Create the persistent representation of the entity.
   *
   * @throws Exception
   */
  void create() throws Exception;

  /**
   * Destroy the persistent representation of the entity.
   *
   * @throws Exception
   */
  void destroy() throws Exception;

  /**
   * Check if the entity wants to survive the lifecycle of the JVM.
   *
   * @return true if it wants to, false otherwise.
   */
  boolean isPersistent();

}
