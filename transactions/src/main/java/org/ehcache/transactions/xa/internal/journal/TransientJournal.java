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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * An in-memory only  {@link Journal} implementation.
 *
 * @author Ludovic Orban
 */
public class TransientJournal<K> implements Journal<K> {

  enum XAState {
    /**
     * In-doubt (aka prepared) state.
     */
    IN_DOUBT,

    /**
     * Committed state.
     */
    COMMITTED,

    /**
     * Rolled back state.
     */
    ROLLED_BACK,
  }

  protected static class Entry<K> {
    final XAState state;
    final boolean heuristic;
    final Collection<K> keys;
    public Entry(XAState state, boolean heuristic, Collection<K> keys) {
      this.state = state;
      this.heuristic = heuristic;
      this.keys = new ArrayList<>(keys); // make a copy of the collection
    }
  }

  protected final ConcurrentHashMap<TransactionId, Entry<K>> states = new ConcurrentHashMap<>();

  @Override
  public void saveCommitted(TransactionId transactionId, boolean heuristicDecision) {
    save(transactionId, XAState.COMMITTED, heuristicDecision, Collections.emptySet());
  }

  @Override
  public void saveRolledBack(TransactionId transactionId, boolean heuristicDecision) {
    save(transactionId, XAState.ROLLED_BACK, heuristicDecision, Collections.emptySet());
  }

  @Override
  public void saveInDoubt(TransactionId transactionId, Collection<K> inDoubtKeys) {
    save(transactionId, XAState.IN_DOUBT, false, inDoubtKeys);
  }

  private void save(TransactionId transactionId, XAState xaState, boolean heuristicDecision, Collection<K> inDoubtKeys) {
    if (!heuristicDecision) {
      // check for heuristics
      if (xaState == XAState.IN_DOUBT) {
        Entry<K> existing = states.putIfAbsent(transactionId, new Entry<>(xaState, false, inDoubtKeys));
        if (existing != null) {
          throw new IllegalStateException("A transaction cannot go back to in-doubt state");
        }
      } else {
        Entry<K> entry = states.get(transactionId);
        if (entry != null && entry.heuristic) {
          throw new IllegalStateException("A heuristically terminated transaction cannot be normally terminated, it must be forgotten");
        }
        states.remove(transactionId);
      }
    } else {
      if (xaState == XAState.IN_DOUBT) {
        throw new IllegalStateException("A transaction cannot enter in-doubt state heuristically");
      } else {
        Entry<K> replaced = states.replace(transactionId, new Entry<>(xaState, true, Collections.emptySet()));
        if (replaced == null) {
          throw new IllegalStateException("Only in-doubt transactions can be heuristically terminated");
        }
      }
    }
  }

  @Override
  public boolean isInDoubt(TransactionId transactionId) {
    Entry<K> entry = states.get(transactionId);
    return entry != null && entry.state == XAState.IN_DOUBT;
  }

  @Override
  public Collection<K> getInDoubtKeys(TransactionId transactionId) {
    Entry<K> entry = states.get(transactionId);
    if (entry == null || entry.heuristic) {
      return null;
    }
    return new ArrayList<>(entry.keys);
  }

  @Override
  public Map<TransactionId, Collection<K>> recover() {
    HashMap<TransactionId, Collection<K>> result = new HashMap<>();
    for (Map.Entry<TransactionId, Entry<K>> entry : states.entrySet()) {
      if (!entry.getValue().heuristic) {
        result.put(entry.getKey(), new ArrayList<>(entry.getValue().keys));
      }
    }
    return result;
  }

  @Override
  public boolean isHeuristicallyTerminated(TransactionId transactionId) {
    Entry<K> entry = states.get(transactionId);
    return entry != null && entry.heuristic;
  }

  @Override
  public void forget(TransactionId transactionId) {
    Entry<K> entry = states.get(transactionId);
    if (entry != null) {
      if (entry.heuristic) {
        states.remove(transactionId);
      } else {
        throw new IllegalStateException("Cannot forget non-heuristically terminated transaction");
      }
    } else {
      throw new IllegalStateException("Cannot forget unknown transaction");
    }
  }

  @Override
  public Map<TransactionId, Boolean> heuristicDecisions() {
    HashMap<TransactionId, Boolean> result = new HashMap<>();
    for (Map.Entry<TransactionId, Entry<K>> entry : states.entrySet()) {
      if (entry.getValue().heuristic) {
        result.put(entry.getKey(), entry.getValue().state == XAState.COMMITTED);
      }
    }
    return result;
  }

  @Override
  public void open() throws IOException {
  }

  @Override
  public void close() throws IOException {
  }
}
