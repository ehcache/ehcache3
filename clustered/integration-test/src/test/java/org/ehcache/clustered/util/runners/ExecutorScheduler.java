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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

public class ExecutorScheduler implements RunnerScheduler {

  public final Supplier<ExecutorService> executorSupplier;
  public final AtomicReference<ExecutorService> executor = new AtomicReference<>();

  public ExecutorScheduler(Supplier<ExecutorService> executorSupplier) {
    this.executorSupplier = executorSupplier;
  }

  @Override
  public void schedule(Runnable childStatement) {
    ExecutorService executorService;
    while ((executorService = executor.get()) == null && !executor.compareAndSet(null, (executorService = executorSupplier.get()))) {
      executorService.shutdown();
    }
    executorService.execute(childStatement);
  }

  @Override
  public void finished() {
    ExecutorService departing = executor.getAndSet(null);
    departing.shutdown();
    try {
      if (!departing.awaitTermination(1, TimeUnit.DAYS)) {
        throw new AssertionError(new TimeoutException());
      }
    } catch (InterruptedException e) {
      throw new AssertionError(e);
    }
  }
}
