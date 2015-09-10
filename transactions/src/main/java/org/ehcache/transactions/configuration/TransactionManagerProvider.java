package org.ehcache.transactions.configuration;

import org.ehcache.spi.service.Service;
import org.ehcache.transactions.txmgrs.TransactionManagerWrapper;

/**
 * @author Ludovic Orban
 */
public interface TransactionManagerProvider extends Service {

  TransactionManagerWrapper getTransactionManagerWrapper();

}
