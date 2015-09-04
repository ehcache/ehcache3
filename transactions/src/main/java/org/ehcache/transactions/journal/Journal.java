package org.ehcache.transactions.journal;

import org.ehcache.transactions.TransactionId;
import org.ehcache.transactions.XAState;

import java.util.Map;

/**
 * @author Ludovic Orban
 */
public interface Journal {

  void save(TransactionId transactionId, XAState xaState, boolean heuristicDecision);

  XAState getState(TransactionId transactionId);

  Map<TransactionId, XAState> recover();


  void forget(TransactionId transactionId);

  Map<TransactionId, XAState> heuristicDecisions();

}
