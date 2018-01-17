package org.ehcache.core.resilience;

import org.ehcache.core.spi.store.Store;
import org.ehcache.resilience.StoreAccessException;

/**
 * @author Henri Tremblay
 */
public class DefaultRecoveryStore<K> implements RecoveryStore<K> {

  private final Store<K, ?> store;

  public DefaultRecoveryStore(Store<K, ?> store) {
    this.store = store;
  }

  @Override
  public void obliterate() throws StoreAccessException {
    store.clear();
  }

  @Override
  public void obliterate(K key) throws StoreAccessException {
    store.remove(key);
  }

}
