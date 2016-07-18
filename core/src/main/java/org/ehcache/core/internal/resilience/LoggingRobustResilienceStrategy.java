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

package org.ehcache.core.internal.resilience;

import org.ehcache.Cache;
import org.ehcache.core.spi.store.StoreAccessException;
import org.ehcache.CacheIterationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author cdennis
 */
public class LoggingRobustResilienceStrategy<K, V> extends RobustResilienceStrategy<K, V> {

  private static final Logger LOGGER = LoggerFactory.getLogger(LoggingRobustResilienceStrategy.class);

  public LoggingRobustResilienceStrategy(RecoveryCache<K> store) {
    super(store);
  }

  @Override
  public Cache.Entry<K, V> iteratorFailure(StoreAccessException e) {
    LOGGER.error("Ehcache iterator terminated early due to exception", e);
    throw new CacheIterationException(e);
  }

  @Override
  protected void recovered(K key, StoreAccessException from) {
    LOGGER.info("Ehcache key {} recovered from", key, from);
  }

  @Override
  protected void recovered(Iterable<? extends K> keys, StoreAccessException from) {
    LOGGER.info("Ehcache keys {} recovered from", keys, from);
  }

  @Override
  protected void recovered(StoreAccessException from) {
    LOGGER.info("Ehcache recovered from", from);
  }

  @Override
  protected void inconsistent(K key, StoreAccessException because, StoreAccessException... cleanup) {
    LOGGER.error("Ehcache key {} in possible inconsistent state due to ", key, because);
  }

  @Override
  protected void inconsistent(Iterable<? extends K> keys, StoreAccessException because, StoreAccessException... cleanup) {
    LOGGER.error("Ehcache keys {} in possible inconsistent state due to ", keys, because);
  }

  @Override
  protected void inconsistent(StoreAccessException because, StoreAccessException... cleanup) {
    LOGGER.error("Ehcache in possible inconsistent state due to ", because);
  }
}
