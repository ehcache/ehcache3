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

import org.ehcache.spi.service.ServiceCreationConfiguration;
import org.ehcache.core.spi.service.ServiceFactory;
import org.ehcache.transactions.xa.txmgr.provider.LookupTransactionManagerProvider;
import org.ehcache.transactions.xa.txmgr.provider.LookupTransactionManagerProviderConfiguration;
import org.ehcache.transactions.xa.txmgr.provider.TransactionManagerProvider;
import org.osgi.service.component.annotations.Component;

/**
 * {@link ServiceFactory} for the default {@link TransactionManagerProvider}
 *
 * @see LookupTransactionManagerProvider
 */
@Component
@ServiceFactory.RequiresConfiguration
public class DefaultTransactionManagerProviderFactory implements ServiceFactory<TransactionManagerProvider> {

  /**
   * {@inheritDoc}
   */
  @Override
  public TransactionManagerProvider create(ServiceCreationConfiguration<TransactionManagerProvider> configuration) {
    return new LookupTransactionManagerProvider((LookupTransactionManagerProviderConfiguration) configuration);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Class<TransactionManagerProvider> getServiceType() {
    return TransactionManagerProvider.class;
  }
}
