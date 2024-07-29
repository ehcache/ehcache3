/*
 * Copyright Terracotta, Inc.
 * Copyright Super iPaaS Integration LLC, an IBM Company 2024
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

import org.ehcache.javadoc.PublicApi;
import org.ehcache.spi.service.ServiceCreationConfiguration;

import javax.transaction.TransactionManager;

import static org.ehcache.transactions.xa.internal.TypeUtil.uncheckedCast;

/**
 * Specialized {@link ServiceCreationConfiguration} for the {@link LookupTransactionManagerProvider}.
 */
@PublicApi
public class LookupTransactionManagerProviderConfiguration implements ServiceCreationConfiguration<TransactionManagerProvider<TransactionManager>, Class<? extends TransactionManagerLookup<TransactionManager>>> {

  private final Class<? extends TransactionManagerLookup<TransactionManager>> lookupClass;

  public LookupTransactionManagerProviderConfiguration(String className) throws ClassNotFoundException {
    this.lookupClass = uncheckedCast(Class.forName(className));
  }

  public LookupTransactionManagerProviderConfiguration(Class<? extends TransactionManagerLookup<TransactionManager>> clazz) {
    this.lookupClass = clazz;
  }

  /**
   * Returns the class to be used for transaction manager lookup.
   *
   * @return the transaction manager lookup class
   */
  public Class<? extends TransactionManagerLookup<TransactionManager>> getTransactionManagerLookup() {
    return lookupClass;
  }

  @Override
  public Class<TransactionManagerProvider<TransactionManager>> getServiceType() {
    return uncheckedCast(TransactionManagerProvider.class);
  }

  @Override
  public Class<? extends TransactionManagerLookup<TransactionManager>> derive() {
    return getTransactionManagerLookup();
  }

  @Override
  public LookupTransactionManagerProviderConfiguration build(Class<? extends TransactionManagerLookup<TransactionManager>> clazz) {
    return new LookupTransactionManagerProviderConfiguration(clazz);
  }
}
