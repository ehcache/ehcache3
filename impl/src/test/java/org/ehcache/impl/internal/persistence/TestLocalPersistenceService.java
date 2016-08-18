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
import org.ehcache.config.CacheConfiguration;

import org.ehcache.config.ResourcePool;
import org.ehcache.config.ResourceType;
import org.ehcache.impl.config.persistence.CacheManagerPersistenceConfiguration;
import org.ehcache.CachePersistenceException;
import org.ehcache.impl.persistence.DefaultLocalPersistenceService;
import org.ehcache.spi.persistence.PersistableResourceService;
import org.ehcache.spi.persistence.StateRepository;
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
  public PersistenceSpaceIdentifier getPersistenceSpaceIdentifier(String name, CacheConfiguration<?, ?> config) throws CachePersistenceException {
    return persistenceService.getPersistenceSpaceIdentifier(name, config);
  }

  @Override
  public void releasePersistenceSpaceIdentifier(PersistenceSpaceIdentifier<?> identifier) throws CachePersistenceException {
    persistenceService.releasePersistenceSpaceIdentifier(identifier);
  }

  @Override
  public StateRepository getStateRepositoryWithin(PersistenceSpaceIdentifier<?> identifier, String name) throws CachePersistenceException {
    return persistenceService.getStateRepositoryWithin(identifier, name);
  }

  @Override
  public void destroy(String name) throws CachePersistenceException {
    persistenceService.destroy(name);
  }

  @Override
  public FileBasedPersistenceContext createPersistenceContextWithin(PersistenceSpaceIdentifier identifier, String name) throws CachePersistenceException {
    return persistenceService.createPersistenceContextWithin(identifier, name);
  }

  @Override
  public void destroyAll() throws CachePersistenceException {
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
