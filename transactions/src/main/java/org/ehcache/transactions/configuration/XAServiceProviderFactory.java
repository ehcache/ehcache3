package org.ehcache.transactions.configuration;

import org.ehcache.spi.service.ServiceCreationConfiguration;
import org.ehcache.spi.service.ServiceFactory;

/**
 * @author Ludovic Orban
 */
public class XAServiceProviderFactory implements ServiceFactory<XAServiceProvider> {
  @Override
  public XAServiceProvider create(ServiceCreationConfiguration<XAServiceProvider> configuration) {
    return new DefaultXAServiceProvider();
  }

  @Override
  public Class<XAServiceProvider> getServiceType() {
    return XAServiceProvider.class;
  }
}
