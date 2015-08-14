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
import org.ehcache.transactions.commands.StorePutCommand;
import org.ehcache.transactions.commands.StoreRemoveCommand;

import java.util.Map;

/**
 * @author Ludovic Orban
 */
public class XATransactionContext<K, V> {

  private final ConcurrentHashMap<K, Command<V>> commands = new ConcurrentHashMap<K, Command<V>>();
  private final TransactionId transactionId;
  private final Store<K, SoftLock<V>> underlyingStore;
  private final XaTransactionStateStore stateStore;

  public XATransactionContext(TransactionId transactionId, Store<K, SoftLock<V>> underlyingStore, XaTransactionStateStore stateStore) {
    this.transactionId = transactionId;
    this.underlyingStore = underlyingStore;
    this.stateStore = stateStore;
  }

  public void addCommand(K key, StorePutCommand<V> command) {
    commands.put(key, command);
  }

  public void addCommand(K key, StoreRemoveCommand<V> command) {
    commands.put(key, command);
  }

  public Store.ValueHolder<V> getNewValueHolder(K key) {
    Command<V> command = commands.get(key);
    return command != null ? command.getNewValueHolder() : null;
  }

  public Store.ValueHolder<V> getOldValueHolder(K key) {
    Command<V> command = commands.get(key);
    return command != null ? command.getOldValueHolder() : null;
  }

  public boolean isRemoved(K key) {
    Command command = commands.get(key);
    return command != null && command instanceof StoreRemoveCommand;
  }

  public boolean containsCommandFor(K key) {
    return commands.containsKey(key);
  }

  public int prepare() throws CacheAccessException {
    stateStore.save(transactionId, XAState.IN_DOUBT);
    for (Map.Entry<K, Command<V>> entry : commands.entrySet()) {
      if (entry.getValue().getOldValueHolder() != null) {
        SoftLock<V> oldSoftLock = new SoftLock<V>(transactionId, entry.getValue().getOldValueHolder(), null);
        SoftLock<V> newSoftLock = new SoftLock<V>(transactionId, entry.getValue().getOldValueHolder(), entry.getValue().getNewValueHolder());
        boolean replaced = underlyingStore.replace(entry.getKey(), oldSoftLock, newSoftLock);
        if (!replaced) {
          throw new AssertionError("TODO: handle this case");
        }
      } else {
        SoftLock<V> newSoftLock = new SoftLock<V>(transactionId, entry.getValue().getOldValueHolder(), entry.getValue().getNewValueHolder());
        Store.ValueHolder<SoftLock<V>> existing = underlyingStore.putIfAbsent(entry.getKey(), newSoftLock);
        if (existing != null) {
          throw new AssertionError("TODO: handle this case");
        }
      }
    }
    return commands.size();
  }

  public void commit() throws CacheAccessException {
    stateStore.save(transactionId, XAState.COMMITTED);
    for (Map.Entry<K, Command<V>> entry : commands.entrySet()) {
      SoftLock<V> preparedSoftLock = new SoftLock<V>(transactionId, entry.getValue().getOldValueHolder(), entry.getValue().getNewValueHolder());
      SoftLock<V> definitiveSoftLock = new SoftLock<V>(transactionId, entry.getValue().getNewValueHolder(), null);
      boolean replaced = underlyingStore.replace(entry.getKey(), preparedSoftLock, definitiveSoftLock);
      if (!replaced) {
        throw new AssertionError("TODO: handle this case");
      }
    }
  }
}
