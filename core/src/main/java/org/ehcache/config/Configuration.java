/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 */

package org.ehcache.config;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.ehcache.spi.service.ServiceConfiguration;

import static java.util.Collections.*;

/**
 * @author Alex Snaps
 */
public final class Configuration {

  private final Map<String,CacheConfiguration<?, ?>> caches;
  private final Collection<ServiceConfiguration<?>> services;

  public Configuration(Map<String, CacheConfiguration<?, ?>> caches, ServiceConfiguration<?> ... services) {
    this.services = unmodifiableCollection(Arrays.asList(services));
    this.caches = unmodifiableMap(new HashMap<>(caches));
  }

  public Map<String, CacheConfiguration<?, ?>> getCacheConfigurations() {
    return caches;
  }

  public Collection<ServiceConfiguration<?>> getServiceConfigurations() {
    return services;
  }
}
