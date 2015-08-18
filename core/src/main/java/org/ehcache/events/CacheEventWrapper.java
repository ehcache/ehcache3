package org.ehcache.events;

import org.ehcache.event.CacheEvent;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author rism
 */
public class CacheEventWrapper<K, V> {

  CacheEvent<K, V> cacheEvent;
  
  Lock lock = new ReentrantLock();
  Condition fireableCondition;
  Condition processedCondition;
  AtomicBoolean fireable;
  AtomicBoolean failed;
  AtomicBoolean processed;
  
  public CacheEventWrapper(CacheEvent<K, V> cacheEvent) {
    this.cacheEvent = cacheEvent;
    fireableCondition  = lock.newCondition();
    processedCondition = lock.newCondition();
    fireable = new AtomicBoolean(false);
    failed = new AtomicBoolean(false);
    processed = new AtomicBoolean(false);
  }
  
  /**
   * This marks events fireable atomically
   */
  void markFireable() {
    lockEvent();
    try {
      while (!isProcessed()) {
        fireable.set(true);
        signalFireableCondition();
        awaitProcessedCondition();
      }
    } catch (InterruptedException e) {
      e.printStackTrace();
    } finally {
      unlockEvent();
    }
  }

  /**
   * Check if the event is marked fireable
   *
   * @return {@code true} if marked fireable
   */
  Boolean isFireable() {
    return this.fireable.get();
  }

  /**
   * This marks events as failed atomically
   */
  void markFailed() {
    this.failed.set(true);
  }

  /**
   * Check if the event is marked failed
   *
   * @return {@code true} if marked failed
   */
  Boolean hasFailed() {
    return this.failed.get();
  }

  /**
   * This marks events as processed atomically
   */
  void markProcessed() {
    this.processed.set(true);
  }

  /**
   * This marks events as processed atomically
   */
  Boolean isProcessed() {
    return processed.get();
  }

  /**
   * Locks the event so it can be marked
   */
  void lockEvent() {
    lock.lock();
  }

  /**
   * Unlocks locked Events
   */
  void unlockEvent() {
    lock.unlock();
  }

  /**
   * Wait for the event to be marked fireable
   *
   * @throws InterruptedException
   */
  void awaitFireableCondition() throws InterruptedException {
    fireableCondition.await();
  }

  /**
   * Signal that event is now marked fireable
   */
  void signalFireableCondition() {
    fireableCondition.signal();
  }

  /**
   * Wait for the event to be marked processed
   *
   * @throws InterruptedException
   */
  void awaitProcessedCondition() throws InterruptedException {
    processedCondition.await();
  }

  /**
   * Signal that event is now marked processed
   */
  void signalProcessedCondition() {
    processedCondition.signal();
  }
}
