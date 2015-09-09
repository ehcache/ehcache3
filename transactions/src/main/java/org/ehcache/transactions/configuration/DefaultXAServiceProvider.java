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

import bitronix.tm.TransactionManagerServices;
import org.ehcache.spi.ServiceProvider;
import org.ehcache.spi.service.ServiceConfiguration;
import org.ehcache.spi.service.ServiceDependencies;
import org.ehcache.transactions.XAStore;
import org.ehcache.transactions.txmgrs.btm.Ehcache3XAResourceProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicReference;

import static org.ehcache.spi.ServiceLocator.findSingletonAmongst;

/**
 * @author Ludovic Orban
 */
@ServiceDependencies({XAStore.Provider.class})
public class DefaultXAServiceProvider implements XAServiceProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultXAServiceProvider.class);

  private final AtomicReference<TransactionManagerWrapper> transactionManagerWrapperRef = new AtomicReference<TransactionManagerWrapper>();

  @Override
  public void start(ServiceProvider serviceProvider) {
  }

  @Override
  public void stop() {
    this.transactionManagerWrapperRef.set(null);
  }

  @Override
  public TransactionManagerWrapper getTransactionManagerWrapper(ServiceConfiguration<?>... configs) {
    DefaultXAServiceConfiguration xaServiceConfiguration = findSingletonAmongst(DefaultXAServiceConfiguration.class, configs);

    TransactionManagerWrapper transactionManagerWrapper;
    if (transactionManagerWrapperRef.get() == null) {
      if (xaServiceConfiguration != null && xaServiceConfiguration.getTransactionManager() != null) {
        transactionManagerWrapperRef.compareAndSet(null, new TransactionManagerWrapper(xaServiceConfiguration.getTransactionManager(), xaServiceConfiguration.getXAResourceRegistry()));
        transactionManagerWrapper = transactionManagerWrapperRef.get();
      } else {
        //TODO: lookup other TX managers and XAResourceRegistry impls
        if (!TransactionManagerServices.isTransactionManagerRunning()) {
          throw new IllegalStateException("BTM must be started beforehand");
        }
        transactionManagerWrapperRef.compareAndSet(null, new TransactionManagerWrapper(TransactionManagerServices.getTransactionManager(), new Ehcache3XAResourceProducer()));
        transactionManagerWrapper = transactionManagerWrapperRef.get();
      }
      LOGGER.info("Using JTA transaction manager : " + transactionManagerWrapper);
    } else {
      transactionManagerWrapper = transactionManagerWrapperRef.get();
    }

    return transactionManagerWrapper;
  }


}
