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
package org.ehcache.core.resilience;

import org.ehcache.core.spi.store.Store;
import org.ehcache.spi.resilience.RecoveryStore;
import org.ehcache.spi.resilience.StoreAccessException;

/**
 * Default implementation of the {@link RecoveryStore}.
 *
 * It maps each obliterate operation to the equivalent remove operation.
 */
public class DefaultRecoveryStore<K> implements RecoveryStore<K> {

  private final Store<K, ?> store;

  public DefaultRecoveryStore(Store<K, ?> store) {
    this.store = store;
  }

  @Override
  public void obliterate() throws StoreAccessException {
    store.clear();
  }

  @Override
  public void obliterate(K key) throws StoreAccessException {
    store.remove(key);
  }

}
