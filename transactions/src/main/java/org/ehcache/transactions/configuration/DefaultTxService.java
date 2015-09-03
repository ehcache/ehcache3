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
package org.ehcache.transactions.configuration;

import org.ehcache.spi.ServiceProvider;
import org.ehcache.spi.service.ServiceDependencies;
import org.ehcache.transactions.XAStore;

import javax.transaction.TransactionManager;

/**
 * @author Ludovic Orban
 */
@ServiceDependencies({XAStore.Provider.class})
public class DefaultTxService implements TxService {
  private volatile ServiceProvider serviceProvider;

  private final TransactionManager transactionManager;

  public DefaultTxService(TransactionManager transactionManager) {
    this.transactionManager = transactionManager;
  }

  @Override
  public void start(ServiceProvider serviceProvider) {
    this.serviceProvider = serviceProvider;
  }

  @Override
  public void stop() {
    this.serviceProvider = null;
  }

  @Override
  public TransactionManager getTransactionManager() {
    return transactionManager;
  }
}
