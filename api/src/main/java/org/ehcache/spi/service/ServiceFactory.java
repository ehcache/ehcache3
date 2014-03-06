/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 */

package org.ehcache.spi.service;

/**
 * @author Alex Snaps
 */
public interface ServiceFactory<T extends Service> {

  T create(final ServiceConfiguration<T> serviceConfiguration);
  
  Class<T> getServiceType();
}
