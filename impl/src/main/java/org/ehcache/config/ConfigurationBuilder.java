/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 */

package org.ehcache.config;

import org.ehcache.spi.ServiceConfiguration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Alex Snaps
 */
public class ConfigurationBuilder {

  private final Map<String, CacheConfiguration<?, ?>> caches = new HashMap<>();
  private final List<ServiceConfiguration<?>> serviceConfigurations = new ArrayList<>();

  public static ConfigurationBuilder newConfigurationBuilder() {
    return new ConfigurationBuilder();
  }

  public ConfigurationBuilder() {
  }

  public Configuration build() {
    return new Configuration(caches, serviceConfigurations.toArray(new ServiceConfiguration<?>[serviceConfigurations.size()]));
  }

  public ConfigurationBuilder addCache(String alias, CacheConfiguration<?, ?> config) {
    caches.put(alias, config);
    return this;
  }

  public ConfigurationBuilder removeCache(String alias) {
    caches.remove(alias);
    return this;
  }

  public ConfigurationBuilder addService(ServiceConfiguration<?> serviceConfiguration) {
    serviceConfigurations.add(serviceConfiguration);
    return this;
  }

  public ConfigurationBuilder removeService(ServiceConfiguration<?> serviceConfiguration) {
    serviceConfigurations.remove(serviceConfiguration);
    return this;
  }
}
