/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.ehcache.resilience;

import org.ehcache.exceptions.CacheAccessException;
import org.ehcache.spi.cache.Store;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author cdennis
 */
public class LoggingRobustResilienceStrategy<K, V> extends RobustResilienceStrategy<K, V> {

  private static final Logger LOGGER = LoggerFactory.getLogger(LoggingRobustResilienceStrategy.class);

  public LoggingRobustResilienceStrategy(Store<K, V> store) {
    super(store);
  }
  
  @Override
  public void iteratorFailure(CacheAccessException e) {
    LOGGER.error("Ehcache iterator terminated early due to exception", e);
  }

  @Override
  protected void recovered(K key, CacheAccessException from) {
    LOGGER.info("Ehcache key {} recovered from {}", key, from);
  }

  @Override
  protected void recovered(Iterable<? extends K> keys, CacheAccessException from) {
    LOGGER.info("Ehcache keys {} recovered from {}", keys, from);
  }

  @Override
  protected void recovered(CacheAccessException from) {
    LOGGER.info("Ehcache recovered from {}", from);
  }

  @Override
  protected void inconsistent(K key, CacheAccessException because, CacheAccessException... cleanup) {
    LOGGER.error("Ehcache key {} in possible inconsistent state due to ", key, because);
  }

  @Override
  protected void inconsistent(CacheAccessException because, CacheAccessException... cleanup) {
    LOGGER.error("Ehcache in possible inconsistent state due to ", because);
  }
}
