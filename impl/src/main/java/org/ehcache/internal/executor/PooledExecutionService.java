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
package org.ehcache.internal.executor;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.ehcache.config.executor.PooledExecutionServiceConfiguration;
import org.ehcache.config.executor.PooledExecutionServiceConfiguration.PoolConfiguration;
import org.ehcache.spi.ServiceProvider;
import org.ehcache.spi.service.ExecutionService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.concurrent.TimeUnit.SECONDS;

/**
 *
 * @author cdennis
 */
public class PooledExecutionService implements ExecutionService {

  private static final Logger LOGGER = LoggerFactory.getLogger(PooledExecutionService.class);
  
  private final String defaultPoolAlias;
  private final Map<String, PoolConfiguration> poolConfigurations;
  private final Map<String, ThreadPoolExecutor> pools = new HashMap<String, ThreadPoolExecutor>();

  private volatile boolean running = false;
  private volatile OutOfBandScheduledExecutor scheduledExecutor;
  
  PooledExecutionService(PooledExecutionServiceConfiguration configuration) {
    this.defaultPoolAlias = configuration.getDefaultPoolAlias();
    this.poolConfigurations = configuration.getPoolConfigurations();
  }

  @Override
  public ScheduledExecutorService getScheduledExecutor(String poolAlias) {
    if (running) {
      ThreadPoolExecutor executor = pools.get(poolAlias);
      if (executor == null) {
        throw new IllegalStateException("Pool '" + poolAlias + "' is not in the set of available pools " + pools.keySet());
      } else {
        return new PartitionedScheduledExecutor(scheduledExecutor, pools.get(poolAlias));
      }
    } else {
      throw new IllegalStateException("Service cannot be used, it isn't running");
    }
  }

  @Override
  public ExecutorService getOrderedExecutor(String poolAlias, BlockingQueue<Runnable> queue) {
    if (running) {
      ThreadPoolExecutor executor = pools.get(poolAlias);
      if (executor == null) {
        throw new IllegalStateException("Pool '" + poolAlias + "' is not in the set of available pools " + pools.keySet());
      } else {
        return new PartitionedOrderedExecutor(queue, pools.get(poolAlias));
      }
    } else {
      throw new IllegalStateException("Service cannot be used, it isn't running");
    }
  }

  @Override
  public ExecutorService getUnorderedExecutor(String poolAlias, BlockingQueue<Runnable> queue) {
    if (running) {
      return getOrderedExecutor(poolAlias, queue);
    } else {
      throw new IllegalStateException("Service cannot be used, it isn't running");
    }
  }

  @Override
  public void start(ServiceProvider serviceProvider) {
    for (Entry<String, PoolConfiguration> e : poolConfigurations.entrySet()) {
      pools.put(e.getKey(), createPool(e.getKey(), e.getValue()));
    }
    if (defaultPoolAlias != null) {
      ThreadPoolExecutor defaultPool = pools.get(defaultPoolAlias);
      if (defaultPool == null) {
        throw new IllegalStateException("Pool for default pool alias is null");
      } else {
        pools.put(null, defaultPool);
      }
    }
    scheduledExecutor = new OutOfBandScheduledExecutor();
    running = true;
  }

  @Override
  public void stop() {
    LOGGER.info("Shutting down PooledExecutionService");
    running = false;
    //scheduledExecutor.shutdown();
    for (Iterator<Entry<String, ThreadPoolExecutor>> it = pools.entrySet().iterator(); it.hasNext(); ) {
      Entry<String, ThreadPoolExecutor> e = it.next();
      try {
        if (e.getKey() != null) {
          destroyPool(e.getKey(), e.getValue());
        }
      } finally {
        it.remove();
      }
    }
  }

  private static ThreadPoolExecutor createPool(String alias, PoolConfiguration config) {
    return new ThreadPoolExecutor(config.minSize(), config.maxSize(), 10, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(), threadFactory(alias));
  }

  private static void destroyPool(String alias, ThreadPoolExecutor executor) {
    List<Runnable> tasks = executor.shutdownNow();
    if (!tasks.isEmpty()) {
      LOGGER.warn("Tasks remaining in pool '{}' at shutdown: {}", alias, tasks);
    }
    boolean interrupted = false;
    try {
      while (true) {
        try {
          if (executor.awaitTermination(30, SECONDS)) {
            return;
          } else {
            LOGGER.warn("Still waiting for termination of pool '{}'", alias);
          }
        } catch (InterruptedException e) {
          interrupted = true;
        }
      }
    } finally {
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
  }
  
  private static ThreadFactory threadFactory(final String alias) {
    return new ThreadFactory() {

      private final AtomicInteger threadCount = new AtomicInteger();
      
      @Override
      public Thread newThread(Runnable r) {
        return new Thread(r, "[" + alias + "]-" + threadCount.getAndIncrement());
      }
    };
  }

}
