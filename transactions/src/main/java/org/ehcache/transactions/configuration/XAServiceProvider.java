package org.ehcache.transactions.configuration;

import org.ehcache.spi.service.Service;

import javax.transaction.TransactionManager;
import javax.transaction.xa.XAResource;

/**
 * @author Ludovic Orban
 */
public interface XAServiceProvider extends Service {

  TransactionManager getTransactionManager();

  void registerXAResource(XAResource xaResource);

  void unregisterXAResource(XAResource xaResource);

}
