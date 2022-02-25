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

package org.ehcache.transactions.xa.txmgr.narayana;

import jakarta.transaction.TransactionManager;
import org.ehcache.transactions.xa.txmgr.TransactionManagerWrapper;
import org.ehcache.transactions.xa.txmgr.provider.TransactionManagerLookup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Ludovic Orban
 */
public class NarayanaTransactionManagerLookup implements TransactionManagerLookup<TransactionManager> {

  private static final Logger LOGGER = LoggerFactory.getLogger(NarayanaTransactionManagerLookup.class);

  @Override
  public TransactionManagerWrapper<TransactionManager> lookupTransactionManagerWrapper() {
    TransactionManagerWrapper<TransactionManager> tmWrapper = new TransactionManagerWrapper<>(
      com.arjuna.ats.jta.TransactionManager.transactionManager(), new NarayanaXAResourceRegistry());
    LOGGER.info("Using looked up transaction manager : {}", tmWrapper);
    return tmWrapper;
  }
}
