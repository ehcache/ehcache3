package org.ehcache.transactions;

import java.util.Map;

/**
 * @author Ludovic Orban
 */
public interface XaTransactionStateStore {

  void save(TransactionId transactionId, XAState xaState);

  XAState getState(TransactionId transactionId);

  Map<TransactionId, XAState> recover();


  void saveHeuristicDecision(TransactionId transactionId, XAState xaState);

  void forgetHeuristicDecision(TransactionId transactionId);

  Map<TransactionId, XAState> heuristicDecisions();

}
