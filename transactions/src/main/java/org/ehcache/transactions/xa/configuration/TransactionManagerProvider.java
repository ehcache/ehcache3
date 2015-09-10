package org.ehcache.transactions.xa.configuration;

import org.ehcache.spi.service.Service;
import org.ehcache.transactions.xa.txmgrs.TransactionManagerWrapper;

/**
 * @author Ludovic Orban
 */
public interface TransactionManagerProvider extends Service {

  TransactionManagerWrapper getTransactionManagerWrapper();

}
