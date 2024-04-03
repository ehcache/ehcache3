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

import org.ehcache.core.spi.service.InstantiatorService;
import org.ehcache.spi.service.Service;
import org.ehcache.spi.service.ServiceDependencies;
import org.ehcache.spi.service.ServiceProvider;
import org.ehcache.transactions.xa.txmgr.TransactionManagerWrapper;

import javax.transaction.TransactionManager;

/**
 * A {@link TransactionManagerProvider} implementation that resolves the {@link TransactionManagerWrapper} through the
 * {@link TransactionManagerLookup lookup class} provided through its {@link LookupTransactionManagerProviderConfiguration}.
 * <p>
 * The lifecycle this implementation will honour is as follows:
 * <ul>
 *   <li>On construction, instantiate the lookup class indicated by configuration</li>
 *   <li>On {@code start} the service will ask the lookup instance for a {@code TransactionManagerWrapper}
 *   which it will then cache and serve to service users.</li>
 *   <li>On {@code stop} the service will forget about the known {@code TransactionManagerWrapper}</li>
 *   <li>On subsequent {@code start}, the service will ask the lookup instance for a new
 *   {@code TransactionManagerWrapper}</li>
 * </ul>
 * Note that in this scheme, the lookup instance is not expected to cache the {@code TransactionManagerWrapper}
 * unless it can be considered a singleton.
 */
@ServiceDependencies(InstantiatorService.class)
public class LookupTransactionManagerProvider implements TransactionManagerProvider<TransactionManager> {

  private final LookupTransactionManagerProviderConfiguration config;
  private TransactionManagerWrapper<TransactionManager> transactionManagerWrapper;

  /**
   * Creates a new instance with the provided configuration.
   *
   * @param config the service creation configuration
   *
   * @throws NullPointerException if the config is {@code null}
   */
  public LookupTransactionManagerProvider(LookupTransactionManagerProviderConfiguration config) {
    if (config == null) {
      throw new NullPointerException("LookupTransactionManagerProviderConfiguration cannot be null");
    }
    this.config = config;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public TransactionManagerWrapper<TransactionManager> getTransactionManagerWrapper() {
    return transactionManagerWrapper;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void start(ServiceProvider<Service> serviceProvider) {
    InstantiatorService instantiatorService = serviceProvider.getService(InstantiatorService.class);
    this.transactionManagerWrapper = instantiatorService.instantiate(config.getTransactionManagerLookup()).lookupTransactionManagerWrapper();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void stop() {
    this.transactionManagerWrapper = null;
  }
}
