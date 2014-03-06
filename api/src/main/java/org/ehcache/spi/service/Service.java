/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 */

package org.ehcache.spi.service;

import java.util.concurrent.Future;

/**
 * @author Alex Snaps
 */
public interface Service {

  Future<?> start();
  
  Future<?> stop();
}
