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
package com.pany;

import bitronix.tm.TransactionManagerServices;
import org.ehcache.spi.ServiceProvider;
import org.ehcache.transactions.configuration.TransactionManagerProvider;
import org.ehcache.transactions.txmgrs.TransactionManagerWrapper;
import org.ehcache.transactions.txmgrs.btm.BitronixXAResourceRegistry;

/**
 * @author Ludovic Orban
 */
public class BitronixProvider implements TransactionManagerProvider {
  @Override
  public TransactionManagerWrapper getTransactionManagerWrapper() {
    if (!TransactionManagerServices.isTransactionManagerRunning()) {
      throw new RuntimeException("You must start the Bitronix TM before ehcache can use it");
    }
    return new TransactionManagerWrapper(TransactionManagerServices.getTransactionManager(), new BitronixXAResourceRegistry());
  }

  @Override
  public void start(ServiceProvider serviceProvider) {

  }

  @Override
  public void stop() {

  }
}
