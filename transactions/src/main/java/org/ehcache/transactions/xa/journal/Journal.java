package org.ehcache.transactions.xa.journal;

import org.ehcache.transactions.xa.TransactionId;
import org.ehcache.transactions.xa.XAState;

import java.io.IOException;
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

  void open() throws IOException;

  void close() throws IOException;

}
