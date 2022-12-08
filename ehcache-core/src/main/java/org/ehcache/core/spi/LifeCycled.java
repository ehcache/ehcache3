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

package org.ehcache.core.spi;

/**
 * Internal interface to register hooks with the life cycle of {@link org.ehcache.Cache} or
 * {@link org.ehcache.CacheManager} instances.
 */
public interface LifeCycled {

  /**
   * Callback used by internal life cycling infrastructure when transitioning from
   * {@link org.ehcache.Status#UNINITIALIZED} to {@link org.ehcache.Status#AVAILABLE}
   * <br />
   * Throwing an Exception here, will fail the transition
   *
   * @throws Exception to veto transition
   */
  void init() throws Exception;

  /**
   * Callback used by internal life cycling infrastructure when transitioning from
   * {@link org.ehcache.Status#AVAILABLE} to {@link org.ehcache.Status#UNINITIALIZED}
   * <br />
   * Throwing an Exception here, will fail the transition
   *
   * @throws Exception to veto transition
   */
  void close() throws Exception;
}
