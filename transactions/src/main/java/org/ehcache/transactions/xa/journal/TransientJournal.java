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
package org.ehcache.transactions.xa.journal;

import org.ehcache.internal.concurrent.ConcurrentHashMap;
import org.ehcache.transactions.xa.TransactionId;
import org.ehcache.transactions.xa.XAState;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Ludovic Orban
 */
public class TransientJournal implements Journal {

  protected static class Entry {
    final XAState state;
    final boolean heuristic;
    public Entry(XAState state, boolean heuristic) {
      this.state = state;
      this.heuristic = heuristic;
    }
  }

  protected final ConcurrentHashMap<TransactionId, Entry> states = new ConcurrentHashMap<TransactionId, Entry>();

  @Override
  public void save(TransactionId transactionId, XAState xaState, boolean heuristicDecision) {
    if (!heuristicDecision) {
      // check for heuristics
      if (xaState == XAState.IN_DOUBT) {
        Entry existing = states.putIfAbsent(transactionId, new Entry(xaState, false));
        if (existing != null) {
          throw new RuntimeException("A transaction cannot go back to in-doubt state");
        }
      } else {
        Entry entry = states.get(transactionId);
        if (entry != null && entry.heuristic) {
          throw new RuntimeException("A heuristically terminated transaction cannot be normally terminated, it must be forgotten");
        }
        states.remove(transactionId);
      }
    } else {
      if (xaState == XAState.IN_DOUBT) {
        throw new RuntimeException("A transaction cannot enter in-doubt state heuristically");
      } else {
        Entry replaced = states.replace(transactionId, new Entry(xaState, true));
        if (replaced == null) {
          throw new RuntimeException("Only in-doubt transactions can be heuristically terminated");
        }
      }
    }
  }

  @Override
  public XAState getState(TransactionId transactionId) {
    Entry entry = states.get(transactionId);
    if (entry == null || entry.heuristic) {
      return null;
    }
    return entry.state;
  }

  @Override
  public Map<TransactionId, XAState> recover() {
    HashMap<TransactionId, XAState> result = new HashMap<TransactionId, XAState>();
    for (Map.Entry<TransactionId, Entry> entry : states.entrySet()) {
      if (!entry.getValue().heuristic) {
        result.put(entry.getKey(), entry.getValue().state);
      }
    }
    return result;
  }

  @Override
  public void forget(TransactionId transactionId) {
    Entry entry = states.get(transactionId);
    if (entry != null && entry.heuristic) {
      states.remove(transactionId);
    }
  }

  @Override
  public Map<TransactionId, XAState> heuristicDecisions() {
    HashMap<TransactionId, XAState> result = new HashMap<TransactionId, XAState>();
    for (Map.Entry<TransactionId, Entry> entry : states.entrySet()) {
      if (entry.getValue().heuristic) {
        result.put(entry.getKey(), entry.getValue().state);
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
