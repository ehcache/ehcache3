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

package org.ehcache.transactions.xa.txmgr.btm;

import bitronix.tm.TransactionManagerServices;

import org.ehcache.transactions.xa.txmgr.TransactionManagerWrapper;
import org.ehcache.transactions.xa.txmgr.provider.TransactionManagerLookup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Ludovic Orban
 */
// tag::BitronixLookup[]
public class BitronixTransactionManagerLookup implements TransactionManagerLookup { // <1>

  private static final Logger LOGGER = LoggerFactory.getLogger(BitronixTransactionManagerLookup.class);

  @Override
  public TransactionManagerWrapper lookupTransactionManagerWrapper() { // <2>
    if (!TransactionManagerServices.isTransactionManagerRunning()) { // <3>
      throw new IllegalStateException("BTM must be started beforehand");
    }
    TransactionManagerWrapper tmWrapper = new TransactionManagerWrapper(TransactionManagerServices.getTransactionManager(),
        new BitronixXAResourceRegistry()); // <4>
    LOGGER.info("Using looked up transaction manager : {}", tmWrapper);
    return tmWrapper;
  }
}
// end::BitronixLookup[]
