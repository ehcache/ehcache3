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

import javax.transaction.xa.XAException;
import java.util.Map;

/**
 * @author Ludovic Orban
 */
public class XATransactionContext<K, V> {

  private final ConcurrentHashMap<K, Command<V>> commands = new ConcurrentHashMap<K, Command<V>>();
  private final Store<K, SoftLock<K, V>> underlyingStore;

  public XATransactionContext(Store<K, SoftLock<K, V>> underlyingStore) {
    this.underlyingStore = underlyingStore;
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
    for (Map.Entry<K, Command<V>> entry : commands.entrySet()) {
      if (entry.getValue().getOldValueHolder() != null) {
        SoftLock<K, V> oldSoftLock = new SoftLock<K, V>(entry.getValue().getOldValueHolder(), null, false);
        SoftLock<K, V> newSoftLock = new SoftLock<K, V>(entry.getValue().getOldValueHolder(), entry.getValue().getNewValueHolder(), true);
        boolean replaced = underlyingStore.replace(entry.getKey(), oldSoftLock, newSoftLock);
        if (!replaced) {
          throw new AssertionError("TODO: handle this case");
        }
      } else {
        SoftLock<K, V> newSoftLock = new SoftLock<K, V>(entry.getValue().getOldValueHolder(), entry.getValue().getNewValueHolder(), true);
        Store.ValueHolder<SoftLock<K, V>> existing = underlyingStore.putIfAbsent(entry.getKey(), newSoftLock);
        if (existing != null) {
          throw new AssertionError("TODO: handle this case");
        }
      }
    }
    return commands.size();
  }

  public void commit() throws CacheAccessException {
    for (Map.Entry<K, Command<V>> entry : commands.entrySet()) {
      SoftLock<K, V> preparedSoftLock = new SoftLock<K, V>(entry.getValue().getOldValueHolder(), entry.getValue().getNewValueHolder(), true);
      SoftLock<K, V> definitiveSoftLock = new SoftLock<K, V>(entry.getValue().getNewValueHolder(), null, false);
      boolean replaced = underlyingStore.replace(entry.getKey(), preparedSoftLock, definitiveSoftLock);
      if (!replaced) {
        throw new AssertionError("TODO: handle this case");
      }
    }
  }
}
