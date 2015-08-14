package org.ehcache.transactions;

import org.ehcache.spi.service.Service;

/**
 * @author Ludovic Orban
 */
public interface XaTransactionStateStoreProvider extends Service {

  XaTransactionStateStore createStore();

}
