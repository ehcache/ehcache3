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

package org.ehcache.impl.internal.store.disk;

import org.terracotta.offheapstore.util.Factory;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;

import org.ehcache.core.spi.service.ExecutionService;

public class DiskWriteThreadPool implements Factory<ExecutorService> {

  private final List<ExecutorService> writers = new CopyOnWriteArrayList<ExecutorService>();
  private final ExecutionService executionService;
  private final String poolAlias;
  private final int threads;

  private int index   = 0;

  public DiskWriteThreadPool(ExecutionService executionService, String poolAlias, int threads) {
    this.executionService = executionService;
    this.poolAlias = poolAlias;
    this.threads = threads;
  }

  @Override
  public ExecutorService newInstance() {
    ExecutorService writer;
    if (writers.size() < threads) {
      writer = executionService.getOrderedExecutor(poolAlias, new LinkedBlockingQueue<Runnable>());
      writers.add(writer);
    } else {
      writer = writers.get(index++);
      if (index == writers.size()) {
        index = 0;
      }
    }
    return writer;
  }
}
