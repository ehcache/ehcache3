package org.ehcache.transactions.configuration;

import bitronix.tm.BitronixTransactionManager;
import bitronix.tm.TransactionManagerServices;
import org.ehcache.spi.service.ServiceCreationConfiguration;
import org.ehcache.spi.service.ServiceFactory;

/**
 * @author Ludovic Orban
 */
public class XAServiceProviderFactory implements ServiceFactory<XAServiceProvider> {
  @Override
  public XAServiceProvider create(ServiceCreationConfiguration<XAServiceProvider> configuration) {
    //TODO: lookup other TX managers
    if (!TransactionManagerServices.isTransactionManagerRunning()) {
      throw new IllegalStateException("BTM must be started beforehand");
    }
    BitronixTransactionManager transactionManager = TransactionManagerServices.getTransactionManager();
    return new DefaultXAServiceProvider(transactionManager);
  }

  @Override
  public Class<XAServiceProvider> getServiceType() {
    return XAServiceProvider.class;
  }
}
