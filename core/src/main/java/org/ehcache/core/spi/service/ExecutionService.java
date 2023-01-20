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

package org.ehcache.core.spi.service;

import org.ehcache.spi.service.Service;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Configuration of ExecutionService defines named pools of threads.  Consumers
 * reference a specific pool of threads from which their "executor" is derived.
 * <p>
 * Shutdown of these derived executors shuts down the derived executors but does
 * nothing to the underlying thread pool.
 */
public interface ExecutionService extends Service {

  /**
   * Get a pre-configured {@link ScheduledExecutorService} instance.
   *
   * @param poolAlias the requested pool alias.
   * @return the {@link ScheduledExecutorService} instance.
   *
   * @throws IllegalArgumentException if the requested pool alias does not exist.
   */
  ScheduledExecutorService getScheduledExecutor(String poolAlias) throws IllegalArgumentException;

  /**
   * Get a pre-configured {@link ExecutorService} instance that guarantees execution in submission order.
   *
   * @param poolAlias the requested pool alias.
   * @param queue the queue in which pending tasks are to be queued.
   *
   * @return the {@link ExecutorService} instance.
   *
   * @throws IllegalArgumentException if the requested pool alias does not exist.
   */
  ExecutorService getOrderedExecutor(String poolAlias, BlockingQueue<Runnable> queue) throws IllegalArgumentException;

  /**
   * Get a pre-configured {@link ExecutorService} instance.
   *
   * @param poolAlias the requested pool alias.
   * @param queue the queue in which pending tasks are to be queued.
   *
   * @return the {@link ExecutorService} instance.
   *
   * @throws IllegalArgumentException if the requested pool alias does not exist.
   */
  ExecutorService getUnorderedExecutor(String poolAlias, BlockingQueue<Runnable> queue) throws IllegalArgumentException;
}
