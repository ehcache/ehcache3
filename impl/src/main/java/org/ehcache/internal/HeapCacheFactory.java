/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 */
package org.ehcache.internal;

import org.ehcache.spi.ServiceConfiguration;
import org.ehcache.spi.ServiceFactory;

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
