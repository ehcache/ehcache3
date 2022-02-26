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

package org.ehcache.transactions.xa.internal;

import org.ehcache.spi.resilience.StoreAccessException;
import org.ehcache.core.spi.time.TimeSource;
import org.ehcache.core.spi.store.Store;
import org.ehcache.core.spi.store.Store.RemoveStatus;
import org.ehcache.core.spi.store.Store.ReplaceStatus;
import org.ehcache.transactions.xa.internal.commands.Command;
import org.ehcache.transactions.xa.internal.commands.StoreEvictCommand;
import org.ehcache.transactions.xa.internal.commands.StorePutCommand;
import org.ehcache.transactions.xa.internal.commands.StoreRemoveCommand;
import org.ehcache.transactions.xa.internal.journal.Journal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Context holder of an in-flight XA transaction. Modifications to the {@link XAStore} are registered in an instance
 * of this class in the form of {@link Command}s and can then be applied to the {@link Store} backing the {@link XAStore}
 * in the form of {@link SoftLock}s.
 *
 * @author Ludovic Orban
 */
public class XATransactionContext<K, V> {

  private static final Logger LOGGER = LoggerFactory.getLogger(XATransactionContext.class);

  private final ConcurrentHashMap<K, Command<V>> commands = new ConcurrentHashMap<>();
  private final TransactionId transactionId;
  private final Store<K, SoftLock<V>> underlyingStore;
  private final Journal<K> journal;
  private final TimeSource timeSource;
  private final long timeoutTimestamp;

  XATransactionContext(TransactionId transactionId, Store<K, SoftLock<V>> underlyingStore, Journal<K> journal, TimeSource timeSource, long timeoutTimestamp) {
    this.transactionId = transactionId;
    this.underlyingStore = underlyingStore;
    this.journal = journal;
    this.timeSource = timeSource;
    this.timeoutTimestamp = timeoutTimestamp;
  }

  public boolean hasTimedOut() {
    return timeSource.getTimeMillis() >= timeoutTimestamp;
  }

  public TransactionId getTransactionId() {
    return transactionId;
  }

  public boolean addCommand(K key, Command<V> command) {
    if (commands.get(key) instanceof StoreEvictCommand) {
      // once a mapping is marked as evict, that's the only thing that can happen
      return false;
    }
    commands.put(key, command);
    return true;
  }

  public void removeCommand(K key) {
    commands.remove(key);
  }

  public Map<K, XAValueHolder<V>> newValueHolders() {
    Map<K, XAValueHolder<V>> puts = new HashMap<>();

    for (Map.Entry<K, Command<V>> entry : commands.entrySet()) {
      Command<V> command = entry.getValue();
      if (command instanceof StorePutCommand) {
        puts.put(entry.getKey(), entry.getValue().getNewValueHolder());
      }
    }

    return puts;
  }

  public boolean touched(K key) {
    return commands.containsKey(key);
  }

  public boolean removed(K key) {
    return commands.get(key) instanceof StoreRemoveCommand;
  }

  public boolean updated(K key) {
    return commands.get(key) instanceof StorePutCommand;
  }

  public boolean evicted(K key) {
    return commands.get(key) instanceof StoreEvictCommand;
  }

  public V oldValueOf(K key) {
    Command<V> command = commands.get(key);
    return command != null ? command.getOldValue() : null;
  }

  public XAValueHolder<V> newValueHolderOf(K key) {
    Command<V> command = commands.get(key);
    return command != null ? command.getNewValueHolder() : null;
  }

  public V newValueOf(K key) {
    Command<V> command = commands.get(key);
    XAValueHolder<V> valueHolder = command == null ? null : command.getNewValueHolder();
    return valueHolder == null ? null : valueHolder.get();
  }

  public int prepare() throws StoreAccessException, IllegalStateException, TransactionTimeoutException {
    try {
      if (hasTimedOut()) {
        throw new TransactionTimeoutException();
      }
      if (journal.isInDoubt(transactionId)) {
        throw new IllegalStateException("Cannot prepare transaction that is not in-flight : " + transactionId);
      }

      journal.saveInDoubt(transactionId, commands.keySet());
      for (Map.Entry<K, Command<V>> entry : commands.entrySet()) {
        if (entry.getValue() instanceof StoreEvictCommand) {
          evictFromUnderlyingStore(entry.getKey());
          continue;
        }
        V oldValue = entry.getValue().getOldValue();
        SoftLock<V> oldSoftLock = oldValue == null ? null : new SoftLock<>(null, oldValue, null);
        SoftLock<V> newSoftLock = new SoftLock<>(transactionId, oldValue, entry.getValue().getNewValueHolder());
        if (oldSoftLock != null) {
          boolean replaced = replaceInUnderlyingStore(entry.getKey(), oldSoftLock, newSoftLock);
          if (!replaced) {
            LOGGER.debug("prepare failed replace of softlock (concurrent modification?)");
            evictFromUnderlyingStore(entry.getKey());
          }
        } else {
          Store.ValueHolder<SoftLock<V>> existing = putIfAbsentInUnderlyingStore(entry, newSoftLock);
          if (existing != null) {
            LOGGER.debug("prepare failed putIfAbsent of softlock (concurrent modification?)");
            evictFromUnderlyingStore(entry.getKey());
          }
        }
      }

      if (commands.isEmpty()) {
        journal.saveRolledBack(transactionId, false);
      }

      return commands.size();
    } finally {
      commands.clear();
    }
  }

  /**
   * @throws IllegalStateException if the transaction ID is unknown
   * @throws IllegalArgumentException if the transaction ID has not been prepared
   */
  public void commit(boolean recovering) throws StoreAccessException, IllegalStateException, IllegalArgumentException {
    if (!journal.isInDoubt(transactionId)) {
      if (recovering) {
        throw new IllegalStateException("Cannot commit unknown transaction : " + transactionId);
      } else {
        throw new IllegalArgumentException("Cannot commit transaction that has not been prepared : " + transactionId);
      }
    }

    Collection<K> keys = journal.getInDoubtKeys(transactionId);
    for (K key : keys) {
      SoftLock<V> preparedSoftLock = getFromUnderlyingStore(key);
      XAValueHolder<V> newValueHolder = preparedSoftLock == null ? null : preparedSoftLock.getNewValueHolder();
      SoftLock<V> definitiveSoftLock = newValueHolder == null ? null : new SoftLock<>(null, newValueHolder.get(), null);

      if (preparedSoftLock != null) {
        if (preparedSoftLock.getTransactionId() != null && !preparedSoftLock.getTransactionId().equals(transactionId)) {
          LOGGER.debug("commit skipping prepared softlock with non-matching TX ID (concurrent modification?)");
          evictFromUnderlyingStore(key);
          continue;
        }

        if (definitiveSoftLock != null) {
          boolean replaced = replaceInUnderlyingStore(key, preparedSoftLock, definitiveSoftLock);
          if (!replaced) {
            LOGGER.debug("commit failed replace of softlock (concurrent modification?)");
            evictFromUnderlyingStore(key);
          }
        } else {
          boolean removed = removeFromUnderlyingStore(key, preparedSoftLock);
          if (!removed) {
            LOGGER.debug("commit failed remove of softlock (concurrent modification?)");
            evictFromUnderlyingStore(key);
          }
        }
      } else {
        LOGGER.debug("commit skipping evicted prepared softlock");
      }

    }

    journal.saveCommitted(transactionId, false);
  }

  public void commitInOnePhase() throws StoreAccessException, IllegalStateException, TransactionTimeoutException {
    if (journal.isInDoubt(transactionId)) {
      throw new IllegalStateException("Cannot commit-one-phase transaction that has been prepared : " + transactionId);
    }

    int prepared = prepare();
    if (prepared > 0) {
      commit(false);
    }
  }

  /**
   * @throws IllegalStateException if the transaction ID is unknown
   */
  public void rollback(boolean recovering) throws StoreAccessException, IllegalStateException {
    boolean inDoubt = journal.isInDoubt(transactionId);

    if (inDoubt) {
      // phase 2 rollback

      Collection<K> keys = journal.getInDoubtKeys(transactionId);
      for (K key : keys) {
        SoftLock<V> preparedSoftLock = getFromUnderlyingStore(key);
        V oldValue = preparedSoftLock == null ? null : preparedSoftLock.getOldValue();
        SoftLock<V> definitiveSoftLock = oldValue == null ? null : new SoftLock<>(null, oldValue, null);

        if (preparedSoftLock != null) {
          if (preparedSoftLock.getTransactionId() != null && !preparedSoftLock.getTransactionId().equals(transactionId)) {
            LOGGER.debug("rollback skipping prepared softlock with non-matching TX ID (concurrent modification?)");
            evictFromUnderlyingStore(key);
            continue;
          }

          if (definitiveSoftLock != null) {
            boolean replaced = replaceInUnderlyingStore(key, preparedSoftLock, definitiveSoftLock);
            if (!replaced) {
              LOGGER.debug("rollback failed replace of softlock (concurrent modification?)");
              evictFromUnderlyingStore(key);
            }
          } else {
            boolean removed = removeFromUnderlyingStore(key, preparedSoftLock);
            if (!removed) {
              LOGGER.debug("rollback failed remove of softlock (concurrent modification?)");
              evictFromUnderlyingStore(key);
            }
          }
        } else {
          LOGGER.debug("rollback skipping evicted prepared softlock");
        }
      }

      journal.saveRolledBack(transactionId, false);
    } else if (recovering) {
      throw new IllegalStateException("Cannot rollback unknown transaction : " + transactionId);
    } else {
      // phase 1 rollback
      commands.clear();
    }
  }


  private boolean removeFromUnderlyingStore(K key, SoftLock<V> preparedSoftLock) throws StoreAccessException {
    if (underlyingStore.remove(key, preparedSoftLock).equals(RemoveStatus.REMOVED)) {
      return true;
    }
    return false;
  }

  private boolean replaceInUnderlyingStore(K key, SoftLock<V> preparedSoftLock, SoftLock<V> definitiveSoftLock) throws StoreAccessException {
    if (underlyingStore.replace(key, preparedSoftLock, definitiveSoftLock).equals(ReplaceStatus.HIT)) {
      return true;
    }
    return false;
  }

  private Store.ValueHolder<SoftLock<V>> putIfAbsentInUnderlyingStore(Map.Entry<K, Command<V>> entry, SoftLock<V> newSoftLock) throws StoreAccessException {
    return underlyingStore.putIfAbsent(entry.getKey(), newSoftLock, b -> {});
  }

  private SoftLock<V> getFromUnderlyingStore(K key) throws StoreAccessException {
    Store.ValueHolder<SoftLock<V>> softLockValueHolder = underlyingStore.get(key);
    return softLockValueHolder == null ? null : softLockValueHolder.get();
  }

  private void evictFromUnderlyingStore(K key) throws StoreAccessException {
    underlyingStore.remove(key);
  }

  static class TransactionTimeoutException extends RuntimeException {
    private static final long serialVersionUID = -4629992436523905812L;
  }

}
