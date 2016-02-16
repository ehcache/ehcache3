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
import org.ehcache.impl.config.persistence.CacheManagerPersistenceConfiguration;
import org.ehcache.exceptions.CachePersistenceException;
import org.ehcache.spi.ServiceProvider;
import org.ehcache.spi.service.FileBasedPersistenceContext;
import org.ehcache.spi.service.LocalPersistenceService;
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
  public PersistenceSpaceIdentifier getOrCreatePersistenceSpace(String name) throws CachePersistenceException {
    return persistenceService.getOrCreatePersistenceSpace(name);
  }

  @Override
  public void destroyPersistenceSpace(String name) throws CachePersistenceException {
    persistenceService.destroyPersistenceSpace(name);
  }

  @Override
  public FileBasedPersistenceContext createPersistenceContextWithin(PersistenceSpaceIdentifier space, String name) throws CachePersistenceException {
    return persistenceService.createPersistenceContextWithin(space, name);
  }

  @Override
  public void destroyAllPersistenceSpaces() {
    persistenceService.destroyAllPersistenceSpaces();
  }

  @Override
  public void start(ServiceProvider serviceProvider) {
    //ignore
  }

  @Override
  public void stop() {
    //ignore
  }
}
