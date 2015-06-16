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

package org.ehcache.internal.store.disk;

import org.terracotta.offheapstore.util.Factory;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class DiskWriteThreadPool implements Factory<ThreadPoolExecutor> {

  private final List<ThreadPoolExecutor> writers = new CopyOnWriteArrayList<ThreadPoolExecutor>();
  private final String                   name;
  private final int                      threads;

  private int                            index   = 0;

  public DiskWriteThreadPool(String name, int threads) {
    this.name = name;
    this.threads = threads;
  }

  public ThreadPoolExecutor newInstance() {
    ThreadPoolExecutor writer;
    if (writers.size() < threads) {
      final int threadIndex = writers.size();
      writer = new ThreadPoolExecutor(1, 1, 0, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(),
                                      new ThreadFactory() {
                                        public Thread newThread(Runnable r) {
                                          return new Thread(r, "Ehcache Disk Write Thread [" + name + "] - " + threadIndex);
                                        }
                                      });
      writers.add(writer);
    } else {
      writer = writers.get(index++);
      if (index == writers.size()) {
        index = 0;
      }
    }
    return writer;
  }

  public long getTotalQueueSize() {
    long size = 0;
    for (ThreadPoolExecutor e : writers) {
      size += e.getQueue().size();
    }
    return size;
  }
}
