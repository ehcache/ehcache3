/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 */

package org.ehcache.spi;

/**
 * @author Alex Snaps
 */
public interface ServiceConfiguration<T extends Service> {

  Class<T> getServiceType();
}
