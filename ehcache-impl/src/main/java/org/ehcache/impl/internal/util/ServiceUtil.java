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

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 *
 * @author cdennis
 */
public final class ServiceUtil {

  private ServiceUtil() {
    //static only
  }

  private static final Future<?> COMPLETE_FUTURE = new Future<Void>() {

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
      return false;
    }

    @Override
    public boolean isCancelled() {
      return false;
    }

    @Override
    public boolean isDone() {
      return true;
    }

    @Override
    public Void get() {
      return null;
    }

    @Override
    public Void get(long timeout, TimeUnit unit) {
      return null;
    }
  };

  public static Future<?> completeFuture() {
    return COMPLETE_FUTURE;
  }
}
