/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 */

package org.ehcache.config.xml;

import org.ehcache.spi.Service;
import org.ehcache.spi.ServiceConfiguration;

/**
 *
 * @author cdennis
 */
class FooConfiguration implements ServiceConfiguration<Service> {

  @Override
  public Class<Service> getServiceType() {
    return Service.class;
  }
  
}
