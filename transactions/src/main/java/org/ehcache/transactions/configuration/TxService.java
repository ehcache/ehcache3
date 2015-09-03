package org.ehcache.transactions.configuration;

import org.ehcache.spi.service.Service;

import javax.transaction.TransactionManager;

/**
 * @author Ludovic Orban
 */
public interface TxService extends Service {

  TransactionManager getTransactionManager();

}
