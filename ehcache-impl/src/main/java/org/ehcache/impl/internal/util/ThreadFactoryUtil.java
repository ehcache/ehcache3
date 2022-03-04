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
package org.ehcache.impl.internal.util;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Ludovic Orban
 */
public final class ThreadFactoryUtil {

  /** Turn it on to activate thread creation tracking */
  private static final boolean DEBUG = false;

  /** Stack traces (wrapped in exceptions) of all created threads */
  private static final Map<Integer, Exception> threads = (DEBUG ? new HashMap<>() : null);

  private ThreadFactoryUtil() {}

  /**
   * Return a {@code ThreadFactory} that will generate threads named "Ehcache [alias]-incrementingNumber"
   *
   * @param alias the alias to use in the name. If null, the alias used will be "_default_"
   * @return the new thread factory
   */
  public static ThreadFactory threadFactory(final String alias) {
    return new ThreadFactory() {
      private final ThreadGroup threadGroup = Thread.currentThread().getThreadGroup();
      private final AtomicInteger threadCount = new AtomicInteger();
      private final String poolAlias = (alias == null ? "_default_" : alias);

      @Override
      public Thread newThread(Runnable r) {
        Thread t = new Thread(threadGroup, r, "Ehcache [" + poolAlias + "]-" + threadCount.getAndIncrement());
        if(DEBUG) {
          threads.put(System.identityHashCode(t), new Exception(t.getName()));
        }
        return t;
      }
    };
  }

  /**
   * Will return all the created threads stack traces produce by the thread factories created if the {@link #DEBUG} flag
   * is true.
   *
   * @return All the created thread stack traces if {@link #DEBUG} is on, or null otherwise
   */
  public static Map<Integer, Exception> getCreatedThreads() {
    return threads;
  }
}
