/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 */
package org.ehcache.internal.util;

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
