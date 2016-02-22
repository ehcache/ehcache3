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
package org.ehcache.impl.internal.executor;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import static java.util.concurrent.TimeUnit.SECONDS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author cdennis
 */
public class ExecutorUtil {

  private static final Logger LOGGER = LoggerFactory.getLogger(ExecutorUtil.class);

  public static void shutdown(ExecutorService executor) {
    executor.shutdown();
    terminate(executor);
  }

  public static void shutdownNow(ExecutorService executor) {
    for (Runnable r : executor.shutdownNow()) {
      if (!(r instanceof FutureTask) || !((FutureTask<?>) r).isCancelled()) {
        try {
          r.run();
        } catch (Throwable t) {
          LOGGER.warn("Exception executing task left in {}: {}", executor, t);
        }
      }
    }
    terminate(executor);
  }

  private static void terminate(ExecutorService executor) {
    boolean interrupted = false;
    try {
      while (true) {
        try {
          if (executor.awaitTermination(30, SECONDS)) {
            return;
          } else {
            LOGGER.warn("Still waiting for termination of {}", executor);
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

  public static <T> T waitFor(Future<T> future) throws ExecutionException {
    boolean interrupted = false;
    try {
      while (true) {
        try {
          return future.get();
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
}
