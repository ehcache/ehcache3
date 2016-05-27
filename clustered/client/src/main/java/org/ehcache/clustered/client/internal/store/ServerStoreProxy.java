package org.ehcache.clustered.client.internal.store;

import org.ehcache.clustered.common.store.ServerStore;

/**
 * @author Ludovic Orban
 */
public interface ServerStoreProxy extends ServerStore {

  interface InvalidationListener {
    void onInvalidationRequest(long hash);
  }

  String getCacheId();

  void addInvalidationListener(InvalidationListener listener);

}
