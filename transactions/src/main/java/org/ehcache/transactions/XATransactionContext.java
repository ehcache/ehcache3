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
package org.ehcache.transactions;

import org.ehcache.exceptions.CacheAccessException;
import org.ehcache.internal.concurrent.ConcurrentHashMap;
import org.ehcache.spi.cache.Store;
import org.ehcache.transactions.commands.Command;
import org.ehcache.transactions.commands.StoreEvictCommand;
import org.ehcache.transactions.commands.StorePutCommand;
import org.ehcache.transactions.journal.Journal;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Ludovic Orban
 */
public class XATransactionContext<K, V> {

  private final ConcurrentHashMap<K, Command<V>> commands = new ConcurrentHashMap<K, Command<V>>();
  private final TransactionId transactionId;
  private final Store<K, SoftLock<V>> underlyingStore;
  private final Journal journal;

  public XATransactionContext(TransactionId transactionId, Store<K, SoftLock<V>> underlyingStore, Journal journal) {
    this.transactionId = transactionId;
    this.underlyingStore = underlyingStore;
    this.journal = journal;
  }

  public TransactionId getTransactionId() {
    return transactionId;
  }

  public void addCommand(K key, Command<V> command) {
    if (commands.get(key) instanceof StoreEvictCommand) {
      return;
    }
    commands.put(key, command);
  }


  public Map<K, XAValueHolder<V>> listPutNewValueHolders() {
    Map<K, XAValueHolder<V>> puts = new HashMap<K, XAValueHolder<V>>();

    for (Map.Entry<K, Command<V>> entry : commands.entrySet()) {
      Command<V> command = entry.getValue();
      if (command instanceof StorePutCommand) {
        puts.put(entry.getKey(), entry.getValue().getNewValueHolder());
      }
    }

    return puts;
  }


  public XAValueHolder<V> getNewValueHolder(K key) {
    Command<V> command = commands.get(key);
    return command != null ? command.getNewValueHolder() : null;
  }

  public Store.ValueHolder<V> getOldValueHolder(K key) {
    Command<V> command = commands.get(key);
    return command != null ? command.getOldValueHolder() : null;
  }

  public boolean containsCommandFor(K key) {
    return commands.containsKey(key);
  }

  public V latestValueFor(K key) {
    Command<V> command = commands.get(key);
    XAValueHolder<V> valueHolder = command == null ? null : command.getNewValueHolder();
    return valueHolder == null ? null : valueHolder.value();
  }

  public int prepare() throws CacheAccessException, IllegalStateException {
    if (journal.getState(transactionId) != null) {
      throw new IllegalStateException("Cannot prepare transaction that is not in-flight : " + transactionId);
    }

    journal.save(transactionId, XAState.IN_DOUBT, false);
    for (Map.Entry<K, Command<V>> entry : commands.entrySet()) {
      if (entry.getValue() instanceof StoreEvictCommand) {
        continue;
      }
      Store.ValueHolder<SoftLock<V>> softLockValueHolder = underlyingStore.get(entry.getKey());
      SoftLock<V> oldSoftLock = softLockValueHolder == null ? null : softLockValueHolder.value();
      SoftLock<V> newSoftLock = new SoftLock<V>(transactionId, entry.getValue().getOldValueHolder(), entry.getValue().getNewValueHolder());
      if (oldSoftLock != null) {
        boolean replaced = underlyingStore.replace(entry.getKey(), oldSoftLock, newSoftLock);
        if (!replaced) {
          throw new AssertionError("TODO: handle this case");
        }
      } else {
        Store.ValueHolder<SoftLock<V>> existing = underlyingStore.putIfAbsent(entry.getKey(), newSoftLock);
        if (existing != null) {
          throw new AssertionError("TODO: handle this case");
        }
      }
    }
    return commands.size();
  }

  public void commit() throws CacheAccessException {
    if (journal.getState(transactionId) == null) {
      throw new IllegalStateException("Cannot commit transaction that has not been prepared : " + transactionId);
    } else if (journal.getState(transactionId) != XAState.IN_DOUBT) {
      throw new IllegalStateException("Cannot commit done transaction : " + transactionId);
    }

    for (Map.Entry<K, Command<V>> entry : commands.entrySet()) {
      if (entry.getValue() instanceof StoreEvictCommand) {
        underlyingStore.remove(entry.getKey());
        continue;
      }

      SoftLock<V> preparedSoftLock = new SoftLock<V>(transactionId, entry.getValue().getOldValueHolder(), entry.getValue().getNewValueHolder());
      SoftLock<V> definitiveSoftLock = entry.getValue().getNewValueHolder() == null ? null : new SoftLock<V>(null, entry.getValue().getNewValueHolder(), null);

      if (definitiveSoftLock != null) {
        boolean replaced = underlyingStore.replace(entry.getKey(), preparedSoftLock, definitiveSoftLock);
        if (!replaced) {
          throw new AssertionError("TODO: handle this case");
        }
      } else {
        boolean removed = underlyingStore.remove(entry.getKey(), preparedSoftLock);
        if (!removed) {
          throw new AssertionError("TODO: handle this case");
        }
      }
    }
    journal.save(transactionId, XAState.COMMITTED, false);
  }

  public void commitInOnePhase() throws CacheAccessException {
    if (journal.getState(transactionId) != null) {
      throw new IllegalStateException("Cannot commit-one-phase transaction that is not in-flight : " + transactionId);
    }

    for (Map.Entry<K, Command<V>> entry : commands.entrySet()) {
      if (entry.getValue() instanceof StoreEvictCommand) {
        underlyingStore.remove(entry.getKey());
        continue;
      }
      Store.ValueHolder<SoftLock<V>> softLockValueHolder = underlyingStore.get(entry.getKey());
      SoftLock<V> oldSoftLock = softLockValueHolder == null ? null : softLockValueHolder.value();
      SoftLock<V> newSoftLock = entry.getValue().getNewValueHolder() == null ? null : new SoftLock<V>(null, entry.getValue().getNewValueHolder(), null);
      if (oldSoftLock != null) {
        if (newSoftLock != null) {
          boolean replaced = underlyingStore.replace(entry.getKey(), oldSoftLock, newSoftLock);
          if (!replaced) {
            throw new AssertionError("TODO: handle this case");
          }
        } else {
          boolean removed = underlyingStore.remove(entry.getKey(), oldSoftLock);
          if (!removed) {
            throw new AssertionError("TODO: handle this case");
          }
        }
      } else {
        if (newSoftLock != null) {
          Store.ValueHolder<SoftLock<V>> existing = underlyingStore.putIfAbsent(entry.getKey(), newSoftLock);
          if (existing != null) {
            throw new AssertionError("TODO: handle this case");
          }
        } else {
          // replace null with null
          Store.ValueHolder<SoftLock<V>> existing = underlyingStore.get(entry.getKey());
          if (existing != null) {
            throw new AssertionError("TODO: handle this case");
          }
        }
      }
    }
    journal.save(transactionId, XAState.COMMITTED, false);
  }

  public void rollback() throws CacheAccessException {
    if (journal.getState(transactionId) == null) {
      // phase 1 rollback


    } else if (journal.getState(transactionId) == XAState.IN_DOUBT) {
      // phase 2 rollback

      for (Map.Entry<K, Command<V>> entry : commands.entrySet()) {
        SoftLock<V> preparedSoftLock = new SoftLock<V>(transactionId, entry.getValue().getOldValueHolder(), entry.getValue().getNewValueHolder());
        SoftLock<V> definitiveSoftLock = entry.getValue().getOldValueHolder() == null ? null : new SoftLock<V>(null, entry.getValue().getOldValueHolder(), null);
        if (definitiveSoftLock != null) {
          boolean replaced = underlyingStore.replace(entry.getKey(), preparedSoftLock, definitiveSoftLock);
          if (!replaced) {
            throw new AssertionError("TODO: handle this case");
          }
        } else {
          boolean removed = underlyingStore.remove(entry.getKey(), preparedSoftLock);
          if (!removed) {
            throw new AssertionError("TODO: handle this case");
          }
        }
      }

    } else {
      throw new IllegalStateException("Cannot rollback done transaction : " + transactionId);
    }

    journal.save(transactionId, XAState.ROLLED_BACK, false);
  }
}
