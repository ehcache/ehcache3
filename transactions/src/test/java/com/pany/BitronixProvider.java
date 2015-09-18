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
import org.ehcache.transactions.xa.txmgr.provider.TransactionManagerProvider;
import org.ehcache.transactions.xa.txmgr.TransactionManagerWrapper;
import org.ehcache.transactions.xa.txmgr.btm.BitronixXAResourceRegistry;

/**
 * @author Ludovic Orban
 */
// tag::BitronixProvider[]
public class BitronixProvider implements TransactionManagerProvider { // <1>
  @Override
  public TransactionManagerWrapper getTransactionManagerWrapper() { // <2>
    if (!TransactionManagerServices.isTransactionManagerRunning()) { // <3>
      throw new RuntimeException("You must start the Bitronix TM before ehcache can use it");
    }
    return new TransactionManagerWrapper(TransactionManagerServices.getTransactionManager(),
        new BitronixXAResourceRegistry()); // <4>
  }

  @Override
  public void start(ServiceProvider serviceProvider) {
    // no-op
  }

  @Override
  public void stop() {
    // no-op
  }
}
// end::BitronixProvider[]
