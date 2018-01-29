package org.ehcache.core.resilience;

import org.ehcache.core.spi.store.Store;
import org.ehcache.resilience.StoreAccessException;

/**
 * Default implementation of the {@link RecoveryStore} as used by the {@link RobustResilienceStrategy} and
 * {@link RobustLoaderWriterResilienceStrategy}. It simply remove the required keys from the store.
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
