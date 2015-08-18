package org.ehcache.events;

import org.ehcache.event.CacheEvent;

import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * @author rism
 */
public class EventThreadLocal {
//  private static final ThreadLocal<ConcurrentLinkedQueue<CacheEvent>> threadLocal = new ThreadLocal<ConcurrentLinkedQueue<CacheEvent>>() {
//    @Override
//    protected ConcurrentLinkedQueue<CacheEvent> initialValue() {
//      return new ConcurrentLinkedQueue<CacheEvent>();
//    }
//  };

  private static final ThreadLocal<ConcurrentLinkedQueue<CacheEventWrapper>> threadLocal = new ThreadLocal<ConcurrentLinkedQueue<CacheEventWrapper>>() {
    @Override
    protected ConcurrentLinkedQueue<CacheEventWrapper> initialValue() {
      return new ConcurrentLinkedQueue<CacheEventWrapper>();
    }
  };

//  public static ConcurrentLinkedQueue<CacheEvent> get() {
//    return threadLocal.get();
//  }
  
  public static ConcurrentLinkedQueue<CacheEventWrapper> get() {
    return threadLocal.get();
  }
}
