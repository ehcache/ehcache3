/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 */

package org.ehcache.internal;

import org.ehcache.spi.service.ServiceConfiguration;

/**
 * @author Alex Snaps
 */
public class HeapResourceCacheConfiguration implements ServiceConfiguration<HeapResource> {

  private final long maxOnHeapEntryCount;

  public HeapResourceCacheConfiguration(final long maxOnHeapEntryCount) {
    this.maxOnHeapEntryCount = maxOnHeapEntryCount;
  }

  public long getMaxOnHeapEntryCount() {
    return maxOnHeapEntryCount;
  }

  @Override
  public Class<HeapResource> getServiceType() {
    return HeapResource.class;
  }
}
