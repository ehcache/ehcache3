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
 * {@link ServiceConfiguration} for the {@link WriteBehindProvider}.
 * <p>
 * The {@code WriteBehindProvider} provides write-behind services to a
 * {@link org.ehcache.Cache Cache}.
 *
 * @param <R> representation type
 */
public interface WriteBehindConfiguration<R> extends ServiceConfiguration<WriteBehindProvider, R> {

  /**
   * The concurrency of the write behind engines queues.
   *
   * @return the write behind concurrency
   */
  int getConcurrency();

  /**
   * The maximum number of operations allowed on each write behind queue.
   * <p>
   * Only positive values are legal.
   *
   * @return the maximum queue size
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
   * The batching specific part of {@link WriteBehindConfiguration}.
   */
  interface BatchingConfiguration {

    /**
     * The recommended size of a batch of operations.
     * <p>
     * Only positive values are legal. A value of 1 indicates that no batching
     * should happen. Real batch size will be influenced by the write rate and
     * the max write delay.
     *
     * @return the batch size
     */
    int getBatchSize();

    /**
     * The maximum time to wait before writing behind.
     *
     * @return the maximum write delay
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
     * <p>
     * Write coalescing ensure that operations within a batch for the same key
     * will be coalesced in to a single write operation.
     *
     * @return {@code true} if write coalescing enabled
     */
    boolean isCoalescing();
  }
}
