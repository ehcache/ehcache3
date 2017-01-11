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

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Ludovic Orban
 */
public class ThreadFactoryUtil {

  public static ThreadFactory threadFactory(final String alias) {
    return new ThreadFactory() {
      private final ThreadGroup threadGroup = Thread.currentThread().getThreadGroup();
      private final AtomicInteger threadCount = new AtomicInteger();
      private final String poolAlias = (alias == null ? "_default_" : alias);

      @Override
      public Thread newThread(Runnable r) {
        return new Thread(threadGroup, r, "Ehcache [" + poolAlias + "]-" + threadCount.getAndIncrement());
      }
    };
  }

}
