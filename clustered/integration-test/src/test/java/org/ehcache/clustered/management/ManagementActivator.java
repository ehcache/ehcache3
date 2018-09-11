/*
 * Copyright (c) 2011-2018 Software AG, Darmstadt, Germany and/or Software AG USA Inc., Reston, VA, USA, and/or its subsidiaries and/or its affiliates and/or their licensors.
 * Use, reproduction, transfer, publication or disclosure is prohibited except as specifically provided for in your License Agreement with Software AG.
 */
package org.ehcache.clustered.management;

import org.ehcache.management.ManagementRegistryService;
import org.ehcache.management.registry.DefaultManagementRegistryConfiguration;
import org.ehcache.spi.service.ServiceCreationConfiguration;
import org.ehcache.spi.service.ServiceCreationConfigurationProvider;

/**
 * Activates management automatically if no config has been previously set manually or through XML.
 *
 * @author Mathieu Carbou
 */
public class ManagementActivator implements ServiceCreationConfigurationProvider<ManagementRegistryService> {
  @Override
  public Class<ManagementRegistryService> getServiceType() {
    return ManagementRegistryService.class;
  }

  @Override
  public ServiceCreationConfiguration<ManagementRegistryService> get() {
    return new DefaultManagementRegistryConfiguration()
      .addTags("webapp-1", "server-node-1")
      .setCacheManagerAlias("my-super-cache-manager");
  }

}
