/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 */
package org.ehcache.internal.serialization;

import org.ehcache.spi.service.ServiceConfiguration;
import org.ehcache.spi.service.ServiceFactory;

/**
 *
 * @author cdennis
 */
public class JavaSerializationProviderFactory implements ServiceFactory<SerializationProvider> {

  @Override
  public SerializationProvider create(ServiceConfiguration<SerializationProvider> serviceConfiguration) {
    return new JavaSerializationProvider();
  }

  @Override
  public Class<SerializationProvider> getServiceType() {
    return SerializationProvider.class;
  }
  
}
