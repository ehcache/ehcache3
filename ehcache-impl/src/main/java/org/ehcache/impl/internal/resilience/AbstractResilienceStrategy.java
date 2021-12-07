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
package org.ehcache.impl.internal.resilience;

import org.ehcache.Cache;
import org.ehcache.CacheIterationException;
import org.ehcache.impl.internal.util.Pacer;
import org.ehcache.core.spi.time.SystemTimeSource;
import org.ehcache.spi.resilience.RecoveryStore;
import org.ehcache.spi.resilience.ResilienceStrategy;
import org.ehcache.spi.resilience.StoreAccessException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class that can be used for resilience strategies. It provides helper methods for the resilience strategy to use
 * for cleanup in case or error. It then notifies if the cache was able to recover from the error or is now in an
 * inconsistent state. By default, the notification is to log the error.
 */
public abstract class AbstractResilienceStrategy<K, V> implements ResilienceStrategy<K, V> {

  private final Logger LOGGER = LoggerFactory.getLogger(getClass());

  private final RecoveryStore<K> store;

  /**
   * Used to prevent logging the same error continuously. Instead, we will log in error every 30 seconds.
   */
  private final Pacer pacer = new Pacer(SystemTimeSource.INSTANCE, 30_000);

  /**
   * Unique constructor. It takes a {@link RecoveryStore} that will be used for cleanup operations.
   *
   * @param store store to clean
   */
  protected AbstractResilienceStrategy(RecoveryStore<K> store) {
    this.store = store;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Cache.Entry<K, V> iteratorFailure(StoreAccessException e) {
    LOGGER.error("Ehcache iterator terminated early due to exception", e);
    throw new CacheIterationException(e);
  }

  /**
   * Clear all entries from the store.
   *
   * @param from original failure causing the cleanup
   */
  protected void cleanup(StoreAccessException from) {
    try {
      store.obliterate();
    } catch (StoreAccessException e) {
      inconsistent(from, e);
      return;
    }
    recovered(from);
  }

  /**
   * Clean all keys from the store.
   *
   * @param keys keys to clean
   * @param from original failure causing the cleanup
   */
  protected void cleanup(Iterable<? extends K> keys, StoreAccessException from) {
    try {
      store.obliterate(keys);
    } catch (StoreAccessException e) {
      inconsistent(keys, from, e);
      return;
    }
    recovered(keys, from);
  }

  /**
   * Clean the key from the store.
   *
   * @param key key to clean
   * @param from original failure causing the cleanup
   */
  protected void cleanup(K key, StoreAccessException from) {
    try {
      store.obliterate(key);
    } catch (StoreAccessException e) {
      inconsistent(key, from, e);
      return;
    }
    recovered(key, from);
  }

  /**
   * Called when the cache recovered from a failing store operation on the key.
   *
   * @param key key that failed
   * @param from exception thrown by the failing operation
   */
  protected void recovered(K key, StoreAccessException from) {
    LOGGER.info("Ehcache key {} recovered from", key, from);
  }

  /**
   * Called when the cache recovered from a failing store operation on a list of keys.
   *
   * @param keys keys that failed
   * @param from exception thrown by the failing operation
   */
  protected void recovered(Iterable<? extends K> keys, StoreAccessException from) {
    LOGGER.info("Ehcache keys {} recovered from", keys, from);
  }

  /**
   * Called when the cache recovered from a failing store global operation (no specific key involved e.g.
   * {@code clear()}.
   *
   * @param from exception thrown by the failing operation
   */
  protected void recovered(StoreAccessException from) {
    LOGGER.info("Ehcache recovered from", from);
  }

  /**
   * Called when the cache failed to recover from a failing store operation on a key.
   *
   * @param key key now inconsistent
   * @param because exception thrown by the failing operation
   * @param cleanup all the exceptions that occurred during cleanup
   */
  protected void inconsistent(K key, StoreAccessException because, StoreAccessException... cleanup) {
    pacedErrorLog("Ehcache key {} in possible inconsistent state", key, because);
  }

  /**
   * Called when the cache failed to recover from a failing store operation on a list of keys.
   *
   * @param keys
   * @param because exception thrown by the failing operation
   * @param cleanup all the exceptions that occurred during cleanup
   */
  protected void inconsistent(Iterable<? extends K> keys, StoreAccessException because, StoreAccessException... cleanup) {
    pacedErrorLog("Ehcache keys {} in possible inconsistent state", keys, because);
  }

  /**
   * Called when the cache failed to recover from a failing store global operation (no specific key involved e.g.
   * {@code clear()}.
   *
   * @param because exception thrown by the failing operation
   * @param cleanup all the exceptions that occurred during cleanup
   */
  protected void inconsistent(StoreAccessException because, StoreAccessException... cleanup) {
    pacedErrorLog("Ehcache in possible inconsistent state", because);
  }

  /**
   * Log messages in error at worst every 30 seconds. Log everything at debug level.
   *
   * @param message message to log
   * @param e exception to log
   */
  protected void pacedErrorLog(String message, StoreAccessException e) {
    pacer.pacedCall(() -> LOGGER.error(message + " - Similar messages will be suppressed for 30 seconds", e), () -> LOGGER.debug(message, e));
  }

  /**
   * Log messages in error at worst every 30 seconds. Log everything at debug level.
   *
   * @param message message to log
   * @param arg1 first log param
   * @param arg2 second log param
   */
  protected void pacedErrorLog(String message, Object arg1, Object arg2) {
    pacer.pacedCall(() -> LOGGER.error(message + " - Similar messages will be suppressed for 30 seconds", arg1, arg2), () -> LOGGER.debug(message, arg1, arg2));
  }

}
