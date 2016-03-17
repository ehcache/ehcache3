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

package org.ehcache.core.statistics;

/**
 * Enumeration listing the bulk operations available on a {@link org.ehcache.Cache}.
 */
public enum BulkOps {

  /**
   * The "get all" bulk operation
   */
  GET_ALL_HITS,

  /**
   * The "get all" missed bulk operation
   */
  GET_ALL_MISS,

  /**
   * The "put all" bulk operation
   */
  PUT_ALL,

  /**
   * The "remove all" bulk operation
   */
  REMOVE_ALL;

}
