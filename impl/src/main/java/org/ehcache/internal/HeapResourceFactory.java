/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 */

package org.ehcache.internal;

import org.ehcache.spi.service.ServiceConfiguration;
import org.ehcache.spi.service.ServiceFactory;

/**
 * @author Alex Snaps
 */
public class HeapResourceFactory implements ServiceFactory<HeapResource> {

  @Override
  public HeapResource create(final ServiceConfiguration<HeapResource> serviceConfiguration) {
    return new HeapResource();
  }

  @Override
  public Class<HeapResource> getServiceType() {
    return HeapResource.class;
  }
}
