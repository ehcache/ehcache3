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

package org.ehcache.impl.internal.persistence;

import java.io.File;
import java.util.Collection;

import org.ehcache.config.ResourcePool;
import org.ehcache.config.ResourceType;
import org.ehcache.impl.config.persistence.CacheManagerPersistenceConfiguration;
import org.ehcache.CachePersistenceException;
import org.ehcache.impl.persistence.DefaultLocalPersistenceService;
import org.ehcache.spi.service.ServiceProvider;
import org.ehcache.core.spi.service.FileBasedPersistenceContext;
import org.ehcache.core.spi.service.LocalPersistenceService;
import org.ehcache.spi.service.MaintainableService;
import org.ehcache.spi.service.Service;
import org.ehcache.spi.service.ServiceConfiguration;
import org.junit.rules.ExternalResource;
import org.junit.rules.TemporaryFolder;

/**
 *
 * @author cdennis
 */
public class TestLocalPersistenceService extends ExternalResource implements LocalPersistenceService {

  private final TemporaryFolder folder;

  private LocalPersistenceService persistenceService;

  public TestLocalPersistenceService(File folder) {
    this.folder = new TemporaryFolder(folder);
  }

  public TestLocalPersistenceService() {
    this.folder = new TemporaryFolder();
  }

  @Override
  protected void before() throws Throwable {
    folder.create();
    persistenceService = new DefaultLocalPersistenceService(new CacheManagerPersistenceConfiguration(folder.newFolder()));
    persistenceService.start(null);
  }


  @Override
  protected void after() {
    LocalPersistenceService ps = persistenceService;
    persistenceService = null;
    try {
      ps.stop();
    } finally {
      folder.delete();
    }
  }

  @Override
  public boolean handlesResourceType(ResourceType<?> resourceType) {
    return persistenceService.handlesResourceType(resourceType);
  }

  @Override
  public Collection<ServiceConfiguration<?>> additionalConfigurationsForPool(String alias, ResourcePool pool) throws CachePersistenceException {
    return persistenceService.additionalConfigurationsForPool(alias, pool);
  }

  @Override
  public PersistenceSpaceIdentifier getOrCreatePersistenceSpace(String name) throws CachePersistenceException {
    return persistenceService.getOrCreatePersistenceSpace(name);
  }

  @Override
  public void destroy(String name) throws CachePersistenceException {
    persistenceService.destroy(name);
  }

  @Override
  public FileBasedPersistenceContext createPersistenceContextWithin(PersistenceSpaceIdentifier space, String name) throws CachePersistenceException {
    return persistenceService.createPersistenceContextWithin(space, name);
  }

  @Override
  public void create() throws CachePersistenceException {
    persistenceService.create();
  }

  @Override
  public void destroyAll() {
    persistenceService.destroyAll();
  }

  @Override
  public void start(ServiceProvider<Service> serviceProvider) {
    //ignore
  }

  @Override
  public void startForMaintenance(ServiceProvider<MaintainableService> serviceProvider) {
    //ignore
  }

  @Override
  public void stop() {
    //ignore
  }
}
