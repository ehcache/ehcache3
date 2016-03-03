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

package org.ehcache.spi.loaderwriter;

import java.util.concurrent.TimeUnit;
import org.ehcache.spi.service.ServiceConfiguration;

/**
 * WriteBehindConfiguration
 */
public interface WriteBehindConfiguration extends ServiceConfiguration<WriteBehindProvider> {
  /**
   * A number of bucket/thread pairs configured for this cache's write behind.
   *
   * @return Retrieves the amount of bucket/thread pairs configured for this cache's write behind
   */
  int getConcurrency();

  /**
   * The maximum number of operations allowed on the write behind queue.
   *
   * Only positive values are legal.
   *
   * @return Retrieves the maximum amount of operations allowed on the write behind queue
   */
  int getMaxQueueSize();

  /**
   * Returns the batching configuration or {@code null} if batching is not enabled.
   *
   * @return the batching configuration
   */
  BatchingConfiguration getBatchingConfiguration();

  /**
   * Returns the alias of the thread resource pool to use for write behind task execution.
   *
   * @return the thread pool alias
   */
  String getThreadPoolAlias();

  /**
   * BatchingConfiguration
   */
  interface BatchingConfiguration {
    /**
     * The recommended size of a batch of operations.
     *
     * Only positive values are legal. A value of 1 indicates that no batching should happen.
     *
     * Real batch size will be influenced by arrival frequency of operations and max write delay.
     *
     * @return Retrieves the size of the batch operation.
     */
    int getBatchSize();

    /**
     * The maximum time to wait before writing behind.
     *
     * @return Retrieves the maximum time to wait before writing behind
     */
    long getMaxDelay();

    /**
     * The time unit for the maximum delay.
     *
     * @return Retrieves the unit for the maximum delay
     */
    TimeUnit getMaxDelayUnit();

    /**
     * Whether write operations can be coalesced.
     *
     * @return Retrieves the write coalescing behavior is enabled or not
     */
    boolean isCoalescing();
  }
}
