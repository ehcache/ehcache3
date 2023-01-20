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

import org.ehcache.transactions.xa.txmgr.TransactionManagerWrapper;

/**
 * Interface used by the {@link LookupTransactionManagerProvider} to lookup transaction managers to be used in XA
 * transactional caches.
 * <P>
 *   Implementation are epxected to offer a no-arg constructor that will be used by Ehcache.
 * </P>
 */
public interface TransactionManagerLookup {

  /**
   * Creates a new {@link TransactionManagerWrapper} on each invocation.
   *
   * @return the transaction manager wrapper to use
   */
  TransactionManagerWrapper lookupTransactionManagerWrapper();
}
