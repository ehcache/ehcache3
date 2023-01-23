/*
 * Copyright Terracotta, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.ehcache.transactions.xa.internal.journal;

import org.ehcache.transactions.xa.internal.TransactionId;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

/**
 * XA transactions journal used to record the state of in-flight transactions.
 *
 * @author Ludovic Orban
 */
public interface Journal<K> {

  /**
   * Save that a transaction has committed.
   *
   * @param transactionId the ID of the transaction.
   * @param heuristicDecision true if the state change is being done upon a heuristic decision.
   */
  void saveCommitted(TransactionId transactionId, boolean heuristicDecision);

  /**
   * Save that a transaction has rolled back.
   *
   * @param transactionId the ID of the transaction.
   * @param heuristicDecision true if the state change is being done upon a heuristic decision.
   */
  void saveRolledBack(TransactionId transactionId, boolean heuristicDecision);

  /**
   * Save that a transaction is in-doubt.
   *
   * @param transactionId the ID of the transaction.
   * @param inDoubtKeys a {@link Collection} of keys modified by the transaction.
   */
  void saveInDoubt(TransactionId transactionId, Collection<K> inDoubtKeys);

  /**
   * Check if a transaction has been saved as in-doubt.
   *
   * @param transactionId the ID of the transaction.
   * @return true if the transaction is in-doubt.
   */
  boolean isInDoubt(TransactionId transactionId);

  /**
   * Get a {@link Collection} of keys modified by a transaction still in-doubt.
   *
   * @param transactionId the ID of the transaction.
   * @return a {@link Collection} of keys modified by the transaction.
   */
  Collection<K> getInDoubtKeys(TransactionId transactionId);

  /**
   * Recover the state of all in-doubt transactions.
   *
   * @return a map using the transaction ID as the key and the state as value.
   */
  Map<TransactionId, Collection<K>> recover();

  /**
   * Check if a transaction has been terminated by a heuristic decision.
   *
   * @param transactionId the ID of the transaction.
   * @return true if the transaction has been terminated by a heuristic decision.
   */
  boolean isHeuristicallyTerminated(TransactionId transactionId);

  /**
   * Recover the state of all transactions that were terminated upon a heuristic decision.
   *
   * @return a map using the transaction ID as the key and the value true if the state is committed,
   * false if it is rolled back.
   */
  Map<TransactionId, Boolean> heuristicDecisions();

  /**
   * Forget a transaction that was terminated upon a heuristic decision.
   *
   * @param transactionId the Id of the transaction.
   */
  void forget(TransactionId transactionId);

  /**
   * Open the journal.
   * @throws IOException if there was an error opening the journal.
   */
  void open() throws IOException;

  /**
   * Close the journal.
   *
   * @throws IOException if there was an error closing the journal.
   */
  void close() throws IOException;

}
