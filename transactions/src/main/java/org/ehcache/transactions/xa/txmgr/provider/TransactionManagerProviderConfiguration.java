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
package org.ehcache.transactions.xa.txmgr.provider;

import bitronix.tm.TransactionManagerServices;

import org.ehcache.spi.service.ServiceCreationConfiguration;
import org.ehcache.transactions.xa.txmgr.TransactionManagerWrapper;
import org.ehcache.transactions.xa.txmgr.btm.BitronixXAResourceRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Ludovic Orban
 */
public class TransactionManagerProviderConfiguration implements ServiceCreationConfiguration<TransactionManagerProvider> {

  private static final Logger LOGGER = LoggerFactory.getLogger(TransactionManagerProviderConfiguration.class);


  private final TransactionManagerWrapper transactionManagerWrapper;

  public TransactionManagerProviderConfiguration() {
    transactionManagerWrapper = null;
  }

  public TransactionManagerProviderConfiguration(TransactionManagerWrapper transactionManagerWrapper) {
    this.transactionManagerWrapper = transactionManagerWrapper;
  }

  public TransactionManagerWrapper getTransactionManagerWrapper() {
    if (transactionManagerWrapper == null) {
      if (!TransactionManagerServices.isTransactionManagerRunning()) {
        throw new IllegalStateException("BTM must be started beforehand");
      }
      TransactionManagerWrapper tmWrapper = new TransactionManagerWrapper(TransactionManagerServices.getTransactionManager(),
          new BitronixXAResourceRegistry());
      LOGGER.info("Using looked up transaction manager : {}", tmWrapper);
      return tmWrapper;
    } else {
      return transactionManagerWrapper;
    }
  }

  @Override
  public Class<TransactionManagerProvider> getServiceType() {
    return TransactionManagerProvider.class;
  }
}
