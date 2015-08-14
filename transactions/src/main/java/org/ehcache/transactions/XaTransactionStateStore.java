package org.ehcache.transactions;

/**
 * @author Ludovic Orban
 */
public interface XaTransactionStateStore {

  void save(TransactionId transactionId, XAState xaState);

  XAState getState(TransactionId transactionId);

}
