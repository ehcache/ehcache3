/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 */
package org.ehcache.internal;

import org.ehcache.spi.service.ServiceConfiguration;
import org.ehcache.spi.service.ServiceFactory;

/**
 * @author Alex Snaps
 */
public class HeapCacheFactory implements ServiceFactory<HeapCachingTierResource> {

  @Override
  public HeapCachingTierResource create(final ServiceConfiguration<HeapCachingTierResource> serviceConfiguration) {
    return new HeapCachingTierResource();
  }

  @Override
  public Class<HeapCachingTierResource> getServiceType() {
    return HeapCachingTierResource.class;
  }
}
