/*
 * Copyright Terracotta, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.ehcache.management.cluster;

import org.ehcache.spi.service.Service;
import org.ehcache.spi.service.ServiceProvider;

/**
 * Feature-check class that is there only so that the management module can contain some specific classes
 * depending on clustering, and activated only when clustering is available
 */
public class Clustering {

  private static final String ENTITY_SERVICE_FQCN = "org.ehcache.clustered.client.service.EntityService";
  private static final Class<? extends Service> ENTITY_SERVICE_CLASS;

  static {
    Class<? extends Service> serviceClass = null;
    try {
      serviceClass = Clustering.class.getClassLoader().loadClass(ENTITY_SERVICE_FQCN).asSubclass(Service.class);
    } catch (ClassNotFoundException ignored) {
    }
    ENTITY_SERVICE_CLASS = serviceClass;
  }

  /**
   * Check if clustering is active for this cache manager
   */
  public static boolean isAvailable(ServiceProvider<Service> serviceProvider) {
    return ENTITY_SERVICE_CLASS != null && serviceProvider.getService(ENTITY_SERVICE_CLASS) != null;
  }

  /**
   * Creates a new ${@link ClusteringManagementService} to handle the management integration with the cluster
   */
  public static ClusteringManagementService newClusteringManagementService(ClusteringManagementServiceConfiguration<?> configuration) {
    return new DefaultClusteringManagementService(configuration);
  }

}
