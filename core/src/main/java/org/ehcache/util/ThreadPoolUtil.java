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

package org.ehcache.util;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Hung Huynh
 *
 */
public class ThreadPoolUtil {
  private static final String ORG_EHCACHE_STATISTICS_EXECUTOR_POOL_SIZE = "org.ehcache.statisticsExecutor.poolSize";

  public static ScheduledExecutorService createStatisticsExecutor() {
    return Executors.newScheduledThreadPool(
        Integer.getInteger(ORG_EHCACHE_STATISTICS_EXECUTOR_POOL_SIZE, 0), new ThreadFactory() {
          private AtomicInteger cnt = new AtomicInteger(0);

          @Override
          public Thread newThread(Runnable r) {
            Thread t = new Thread(r, "Statistics Thread-" + cnt.incrementAndGet());
            t.setDaemon(true);
            return t;
          }
        });
  }

  public static ExecutorService createEventsOrderedDeliveryExecutor() {
    return Executors.newSingleThreadExecutor(new ThreadFactory() {
      private AtomicInteger cnt = new AtomicInteger(0);

      @Override
      public Thread newThread(Runnable r) {
        Thread t = new Thread(r, "Ordered Events Thread-" + cnt.incrementAndGet());
        t.setDaemon(true);
        // XXX perhaps set uncaught exception handler here according to resilience strategy
        return t;
      }
    });
  }

  public static ExecutorService createEventsUnorderedDeliveryExecutor() {
    return Executors.newCachedThreadPool(new ThreadFactory() {
      private AtomicInteger cnt = new AtomicInteger(0);

      @Override
      public Thread newThread(Runnable r) {
        Thread t = new Thread(r, "Unordered Events Thread-" + cnt.incrementAndGet());
        t.setDaemon(true);
        // XXX perhaps set uncaught exception handler here according to resilience strategy
        return t;
      }
    });
  }
}
