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

package org.ehcache.transactions.xa.internal.txmgr.provider;

import bitronix.tm.TransactionManagerServices;
import org.ehcache.spi.ServiceProvider;
import org.ehcache.spi.service.Service;
import org.ehcache.transactions.xa.txmgr.TransactionManagerWrapper;
import org.ehcache.transactions.xa.txmgr.btm.BitronixXAResourceRegistry;
import org.ehcache.transactions.xa.txmgr.provider.TransactionManagerProvider;
import org.ehcache.transactions.xa.txmgr.provider.TransactionManagerProviderConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The default {@link TransactionManagerProvider} implementation.
 *
 * @author Ludovic Orban
 */
public class DefaultTransactionManagerProvider implements TransactionManagerProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultTransactionManagerProvider.class);

  private final TransactionManagerProviderConfiguration config;

  public DefaultTransactionManagerProvider(TransactionManagerProviderConfiguration config) {
    this.config = config;
  }

  @Override
  public TransactionManagerWrapper getTransactionManagerWrapper() {
    if (config != null) {
      LOGGER.info("Using configured transaction manager : {}", config.getTransactionManagerWrapper());
      return config.getTransactionManagerWrapper();
    }

    //TODO: lookup other TX managers and XAResourceRegistry impls
    if (!TransactionManagerServices.isTransactionManagerRunning()) {
      throw new IllegalStateException("BTM must be started beforehand");
    }
    TransactionManagerWrapper transactionManagerWrapper = new TransactionManagerWrapper(TransactionManagerServices.getTransactionManager(), new BitronixXAResourceRegistry());
    LOGGER.info("Using looked up transaction manager : {}", transactionManagerWrapper);
    return transactionManagerWrapper;
  }

  @Override
  public void start(ServiceProvider<Service> serviceProvider) {
  }

  @Override
  public void stop() {
  }
}
