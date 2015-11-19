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

package org.ehcache.spi.service;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Configuration of ExecutionService defines named pools of threads.  Consumers
 * reference a specific pool of threads from which their "executor" is derived.
 * <p>
 * Shutdown of these derived executors shuts down the derived executors but does
 * nothing to the underlying thread pool.
 * 
 * @author Ludovic Orban
 */
public interface ExecutionService extends Service {

  ScheduledExecutorService getScheduledExecutor(String poolAlias);

  ExecutorService getOrderedExecutor(String poolAlias, BlockingQueue<Runnable> queue);

  ExecutorService getUnorderedExecutor(String poolAlias, BlockingQueue<Runnable> queue);
}
