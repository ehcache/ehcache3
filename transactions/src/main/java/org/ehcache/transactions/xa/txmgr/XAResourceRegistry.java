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
package org.ehcache.transactions.xa.txmgr;

import javax.transaction.xa.XAResource;

/**
 * Support for {@link XAResource} registration with {@link javax.transaction.TransactionManager} implementations.
 *
 * @author Ludovic Orban
 */
public interface XAResourceRegistry {

  /**
   * Register a {@link XAResource} with a JTA implementation.
   *
   * @param uniqueName the unique name of the {@link XAResource}.
   * @param xaResource the {@link XAResource}.
   */
  void registerXAResource(String uniqueName, XAResource xaResource);

  /**
   * Unregister a {@link XAResource} from a JTA implementation.
   *
   * @param uniqueName the unique name of the {@link XAResource}.
   * @param xaResource the {@link XAResource}.
   */
  void unregisterXAResource(String uniqueName, XAResource xaResource);

}
