package org.ehcache.transactions.configuration;

import org.ehcache.spi.service.Service;
import org.ehcache.spi.service.ServiceConfiguration;
import org.ehcache.transactions.txmgrs.XAResourceRegistry;

import javax.transaction.TransactionManager;
import javax.transaction.xa.XAResource;

/**
 * @author Ludovic Orban
 */
public interface XAServiceProvider extends Service {

  TransactionManagerWrapper getTransactionManagerWrapper(ServiceConfiguration<?>... configs);


  class TransactionManagerWrapper {
    private final TransactionManager transactionManager;
    private final XAResourceRegistry xaResourceRegistry;

    public TransactionManagerWrapper(TransactionManager transactionManager, XAResourceRegistry xaResourceRegistry) {
      this.transactionManager = transactionManager;
      this.xaResourceRegistry = xaResourceRegistry;
    }

    public TransactionManager getTransactionManager() {
      return this.transactionManager;
    }

    public void registerXAResource(String uniqueXAResourceId, XAResource xaResource) {
      xaResourceRegistry.registerXAResource(uniqueXAResourceId, xaResource);
    }

    public void unregisterXAResource(String uniqueXAResourceId, XAResource xaResource) {
      xaResourceRegistry.unregisterXAResource(uniqueXAResourceId, xaResource);
    }

    @Override
    public String toString() {
      return transactionManager.toString();
    }
  }

}
