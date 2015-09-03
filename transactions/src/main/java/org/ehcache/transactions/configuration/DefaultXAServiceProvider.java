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
import org.ehcache.transactions.txmgrs.btm.Ehcache3XAResourceProducer;

import javax.transaction.TransactionManager;
import javax.transaction.xa.XAResource;

/**
 * @author Ludovic Orban
 */
@ServiceDependencies({XAStore.Provider.class})
public class DefaultXAServiceProvider implements XAServiceProvider {

  // TODO: find a unique ID per cache
  private static final String SOME_XASTORE_UNIQUE_NAME = "some-xastore-unique-name";

  private final TransactionManager transactionManager;

  public DefaultXAServiceProvider(TransactionManager transactionManager) {
    this.transactionManager = transactionManager;
  }

  @Override
  public void start(ServiceProvider serviceProvider) {
  }

  @Override
  public void stop() {
  }

  @Override
  public TransactionManager getTransactionManager() {
    return transactionManager;
  }

  @Override
  public void registerXAResource(XAResource xaResource) {
    //TODO: abstract that
    Ehcache3XAResourceProducer.registerXAResource(SOME_XASTORE_UNIQUE_NAME, xaResource);
  }

  @Override
  public void unregisterXAResource(XAResource xaResource) {
    //TODO: abstract that
    Ehcache3XAResourceProducer.unregisterXAResource(SOME_XASTORE_UNIQUE_NAME, xaResource);
  }
}
