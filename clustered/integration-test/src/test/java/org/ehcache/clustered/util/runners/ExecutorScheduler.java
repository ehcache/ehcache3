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
package org.ehcache.clustered.util.runners;

import org.junit.runners.model.RunnerScheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class ExecutorScheduler implements RunnerScheduler {

  private static final Logger LOGGER = LoggerFactory.getLogger(ExecutorScheduler.class);

  public final ExecutorService executor;

  public ExecutorScheduler(ExecutorService executor) {
    this.executor = executor;
  }

  @Override
  public void schedule(Runnable childStatement) {
    executor.execute(childStatement);
  }

  @Override
  public void finished() {
    executor.shutdown();
    try {
      if (!executor.awaitTermination(1, TimeUnit.DAYS)) {
        throw new AssertionError(new TimeoutException());
      }
    } catch (InterruptedException e) {
      List<Runnable> runnables = executor.shutdownNow();
      LOGGER.warn("Forcibly terminating execution of scheduled test tasks due to interrupt (" + runnables.size() + " tasks remain unscheduled)");
    }
  }
}
