/**
 *  Copyright Terracotta, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.ehcache.transactions.commands;


import org.ehcache.spi.cache.Store;

/**
 * @author Ludovic Orban
 */
public class StorePutCommand<V> implements Command<V> {

  private final Store.ValueHolder<V> newValueHolder;
  private final Store.ValueHolder<V> oldValueHolder;

  public StorePutCommand(Store.ValueHolder<V> newValueHolder, Store.ValueHolder<V> oldValueHolder) {
    this.newValueHolder = newValueHolder;
    this.oldValueHolder = oldValueHolder;
  }

  @Override
  public Store.ValueHolder<V> getNewValueHolder() {
    return newValueHolder;
  }

  @Override
  public Store.ValueHolder<V> getOldValueHolder() {
    return oldValueHolder;
  }
}
