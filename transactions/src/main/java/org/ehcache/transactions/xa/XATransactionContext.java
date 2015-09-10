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
package org.ehcache.transactions.xa;

import org.ehcache.exceptions.CacheAccessException;
import org.ehcache.internal.TimeSource;
import org.ehcache.internal.concurrent.ConcurrentHashMap;
import org.ehcache.spi.cache.Store;
import org.ehcache.transactions.xa.commands.Command;
import org.ehcache.transactions.xa.commands.StoreEvictCommand;
import org.ehcache.transactions.xa.commands.StorePutCommand;
import org.ehcache.transactions.xa.commands.StoreRemoveCommand;
import org.ehcache.transactions.xa.journal.Journal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Ludovic Orban
 */
public class XATransactionContext<K, V> {

  private static final Logger LOGGER = LoggerFactory.getLogger(XATransactionContext.class);

  private final ConcurrentHashMap<K, Command<V>> commands = new ConcurrentHashMap<K, Command<V>>();
  private final TransactionId transactionId;
  private final Store<K, SoftLock<V>> underlyingStore;
  private final Journal journal;
  private final TimeSource timeSource;
  private final long timeoutTimestamp;

  public XATransactionContext(TransactionId transactionId, Store<K, SoftLock<V>> underlyingStore, Journal journal, TimeSource timeSource, long timeoutTimestamp) {
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

  public void addCommand(K key, Command<V> command) {
    if (commands.get(key) instanceof StoreEvictCommand) {
      // once a mapping is marked as evict, that's the only thing that can happen
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

  public V getOldValue(K key) {
    Command<V> command = commands.get(key);
    return command != null ? command.getOldValue() : null;
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

  public V getEvictedValue(K key) {
    Command<V> command = commands.get(key);
    if (command instanceof StoreEvictCommand) {
      return command.getOldValue();
    }
    return null;
  }

  public V latestValueFor(K key) {
    Command<V> command = commands.get(key);
    XAValueHolder<V> valueHolder = command == null ? null : command.getNewValueHolder();
    return valueHolder == null ? null : valueHolder.value();
  }

  public int prepare() throws CacheAccessException, IllegalStateException, TransactionTimeoutException {
    if (hasTimedOut()) {
      throw new TransactionTimeoutException();
    }
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
      V oldValue = entry.getValue().getOldValue();
      SoftLock<V> newSoftLock = new SoftLock<V>(transactionId, oldValue, entry.getValue().getNewValueHolder());
      if (oldSoftLock != null) {
        boolean replaced = underlyingStore.replace(entry.getKey(), oldSoftLock, newSoftLock);
        if (!replaced) {
          LOGGER.debug("prepare failed replace of softlock (concurrent modification?)");
          evictFromUnderlyingStore(entry.getKey());
        }
      } else {
        Store.ValueHolder<SoftLock<V>> existing = underlyingStore.putIfAbsent(entry.getKey(), newSoftLock);
        if (existing != null) {
          LOGGER.debug("prepare failed putIfAbsent of softlock (concurrent modification?)");
          evictFromUnderlyingStore(entry.getKey());
        }
      }
    }

    if (commands.isEmpty()) {
      journal.save(transactionId, XAState.COMMITTED, false);
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
        evictFromUnderlyingStore(entry.getKey());
        continue;
      }

      V oldValue = entry.getValue().getOldValue();
      SoftLock<V> preparedSoftLock = new SoftLock<V>(transactionId, oldValue, entry.getValue().getNewValueHolder());
      SoftLock<V> definitiveSoftLock = entry.getValue().getNewValueHolder() == null ? null : new SoftLock<V>(null, entry.getValue().getNewValueHolder().value(), null);

      if (definitiveSoftLock != null) {
        boolean replaced = underlyingStore.replace(entry.getKey(), preparedSoftLock, definitiveSoftLock);
        if (!replaced) {
          LOGGER.debug("commit failed replace of softlock (concurrent modification?)");
          evictFromUnderlyingStore(entry.getKey());
        }
      } else {
        boolean removed = underlyingStore.remove(entry.getKey(), preparedSoftLock);
        if (!removed) {
          LOGGER.debug("commit failed remove of softlock (concurrent modification?)");
          evictFromUnderlyingStore(entry.getKey());
        }
      }
    }
    journal.save(transactionId, XAState.COMMITTED, false);
  }

  public void commitInOnePhase() throws CacheAccessException, TransactionTimeoutException {
    if (hasTimedOut()) {
      throw new TransactionTimeoutException();
    }
    if (journal.getState(transactionId) != null) {
      throw new IllegalStateException("Cannot commit-one-phase transaction that has been prepared : " + transactionId);
    }

    for (Map.Entry<K, Command<V>> entry : commands.entrySet()) {
      if (entry.getValue() instanceof StoreEvictCommand) {
        evictFromUnderlyingStore(entry.getKey());
        continue;
      }
      Store.ValueHolder<SoftLock<V>> softLockValueHolder = underlyingStore.get(entry.getKey());
      SoftLock<V> oldSoftLock = softLockValueHolder == null ? null : softLockValueHolder.value();
      SoftLock<V> newSoftLock = entry.getValue().getNewValueHolder() == null ? null : new SoftLock<V>(null, entry.getValue().getNewValueHolder().value(), null);
      if (oldSoftLock != null) {
        if (newSoftLock != null) {
          boolean replaced = underlyingStore.replace(entry.getKey(), oldSoftLock, newSoftLock);
          if (!replaced) {
            LOGGER.debug("commitInOnePhase failed replace of softlock (concurrent modification?)");
            evictFromUnderlyingStore(entry.getKey());
          }
        } else {
          boolean removed = underlyingStore.remove(entry.getKey(), oldSoftLock);
          if (!removed) {
            LOGGER.debug("commitInOnePhase failed remove of softlock (concurrent modification?)");
            evictFromUnderlyingStore(entry.getKey());
          }
        }
      } else {
        if (newSoftLock != null) {
          Store.ValueHolder<SoftLock<V>> existing = underlyingStore.putIfAbsent(entry.getKey(), newSoftLock);
          if (existing != null) {
            LOGGER.debug("commitInOnePhase failed putIfAbsent of softlock (concurrent modification?)");
            evictFromUnderlyingStore(entry.getKey());
          }
        } else {
          // replace null with null
          Store.ValueHolder<SoftLock<V>> existing = underlyingStore.get(entry.getKey());
          if (existing != null) {
            LOGGER.debug("commitInOnePhase failed null check of softlock (concurrent modification?)");
            evictFromUnderlyingStore(entry.getKey());
          }
        }
      }
    }
    journal.save(transactionId, XAState.COMMITTED, false);
  }

  public void rollback() throws CacheAccessException {
    if (journal.getState(transactionId) == null) {
      // phase 1 rollback

      // no-op
    } else if (journal.getState(transactionId) == XAState.IN_DOUBT) {
      // phase 2 rollback

      for (Map.Entry<K, Command<V>> entry : commands.entrySet()) {
        V oldValue = entry.getValue().getOldValue();
        SoftLock<V> preparedSoftLock = new SoftLock<V>(transactionId, oldValue, entry.getValue().getNewValueHolder());
        SoftLock<V> definitiveSoftLock = entry.getValue().getOldValue() == null ? null : new SoftLock<V>(null, oldValue, null);
        if (definitiveSoftLock != null) {
          boolean replaced = underlyingStore.replace(entry.getKey(), preparedSoftLock, definitiveSoftLock);
          if (!replaced) {
            LOGGER.debug("rollback failed replace of softlock (concurrent modification?)");
          }
        } else {
          boolean removed = underlyingStore.remove(entry.getKey(), preparedSoftLock);
          if (!removed) {
            LOGGER.debug("rollback failed remove of softlock (concurrent modification?)");
          }
        }
      }

    } else {
      throw new IllegalStateException("Cannot rollback done transaction : " + transactionId);
    }

    journal.save(transactionId, XAState.ROLLED_BACK, false);
  }

  private void evictFromUnderlyingStore(K key) throws CacheAccessException {
    underlyingStore.remove(key);
  }

}
