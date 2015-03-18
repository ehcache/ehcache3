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

package org.ehcache;

/**
 * @author Alex Snaps
 */
public interface Maintainable {

  /**
   * Creates all persistent data structures required to function
   *
   * @throws java.lang.IllegalStateException if data structures exist already
   */
  void create();

  /**
   * Destroys all persistent data structures
   *
   * @throws java.lang.IllegalStateException if data structures don't exist
   */
  void destroy();

  void exit();
}
