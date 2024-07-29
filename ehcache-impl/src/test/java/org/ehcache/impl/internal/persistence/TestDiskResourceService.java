/*
 * Copyright Terracotta, Inc.
 * Copyright Super iPaaS Integration LLC, an IBM Company 2024
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

import org.ehcache.CachePersistenceException;
import org.ehcache.config.ResourcePool;
import org.ehcache.config.ResourceType;
import org.ehcache.core.spi.service.DiskResourceService;
import org.ehcache.core.spi.service.FileBasedPersistenceContext;
import org.ehcache.core.spi.service.LocalPersistenceService;
import org.ehcache.impl.config.persistence.CacheManagerPersistenceConfiguration;
import org.ehcache.impl.persistence.DefaultDiskResourceService;
import org.ehcache.impl.persistence.DefaultLocalPersistenceService;
import org.ehcache.spi.persistence.StateRepository;
import org.ehcache.spi.service.MaintainableService;
import org.ehcache.spi.service.Service;
import org.ehcache.spi.service.ServiceProvider;
import org.junit.rules.ExternalResource;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import java.io.File;

import static org.ehcache.test.MockitoUtil.uncheckedGenericMock;

/**
 *
 * @author cdennis
 */
public class TestDiskResourceService extends ExternalResource implements DiskResourceService {

  private final TemporaryFolder folder;

  private LocalPersistenceService fileService;
  private DiskResourceService diskResourceService;

  public TestDiskResourceService(File folder) {
    this.folder = new TemporaryFolder(folder);
  }

  public TestDiskResourceService() {
    this.folder = new TemporaryFolder();
  }

  @Override
  protected void before() throws Throwable {
    folder.create();
    fileService = new DefaultLocalPersistenceService(new CacheManagerPersistenceConfiguration(folder.newFolder()));
    fileService.start(null);
    diskResourceService = new DefaultDiskResourceService();
    ServiceProvider<Service> sp = uncheckedGenericMock(ServiceProvider.class);
    Mockito.when(sp.getService(LocalPersistenceService.class)).thenReturn(fileService);
    diskResourceService.start(sp);
  }


  @Override
  protected void after() {
    DiskResourceService ps = diskResourceService;
    LocalPersistenceService ls = fileService;
    diskResourceService = null;
    fileService = null;
    try {
      ps.stop();
      ls.stop();
    } finally {
      folder.delete();
    }
  }

  @Override
  public boolean handlesResourceType(ResourceType<?> resourceType) {
    return diskResourceService.handlesResourceType(resourceType);
  }

  @Override
  public PersistenceSpaceIdentifier<?> getPersistenceSpaceIdentifier(String name, ResourcePool resourcePool) throws CachePersistenceException {
    return diskResourceService.getPersistenceSpaceIdentifier(name, resourcePool);
  }

  @Override
  public PersistenceSpaceIdentifier<?> getSharedPersistenceSpaceIdentifier(ResourcePool resource) throws CachePersistenceException {
    return diskResourceService.getSharedPersistenceSpaceIdentifier(resource);
  }

  @Override
  public void releasePersistenceSpaceIdentifier(PersistenceSpaceIdentifier<?> identifier) throws CachePersistenceException {
    diskResourceService.releasePersistenceSpaceIdentifier(identifier);
  }

  @Override
  public StateRepository getStateRepositoryWithin(PersistenceSpaceIdentifier<?> identifier, String name) throws CachePersistenceException {
    return diskResourceService.getStateRepositoryWithin(identifier, name);
  }

  @Override
  public void destroy(String name) throws CachePersistenceException {
    diskResourceService.destroy(name);
  }

  @Override
  public FileBasedPersistenceContext createPersistenceContextWithin(PersistenceSpaceIdentifier<?> identifier, String name) throws CachePersistenceException {
    return diskResourceService.createPersistenceContextWithin(identifier, name);
  }

  @Override
  public void destroyAll() throws CachePersistenceException {
    diskResourceService.destroyAll();
  }

  @Override
  public void start(ServiceProvider<Service> serviceProvider) {
    //ignore
  }

  @Override
  public void startForMaintenance(ServiceProvider<? super MaintainableService> serviceProvider, MaintenanceScope maintenanceScope) {
    //ignore
  }

  @Override
  public void stop() {
    //ignore
  }
}
