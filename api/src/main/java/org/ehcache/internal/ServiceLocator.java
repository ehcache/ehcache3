package org.ehcache.internal;

import org.ehcache.spi.service.Service;

/**
 * @author Alex Snaps
 */
public interface ServiceLocator {
  <T extends Service> T findService(Class<T> serviceType);
}
