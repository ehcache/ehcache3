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

package org.ehcache.impl.persistence;

import org.ehcache.CachePersistenceException;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.core.spi.service.LocalPersistenceService;
import org.ehcache.impl.config.persistence.DefaultPersistenceConfiguration;
import org.ehcache.spi.persistence.PersistableResourceService;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;
import static org.mockito.Mockito.never;

public class DefaultLocalPersistenceServiceTest {

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Rule
  public final TemporaryFolder folder = new TemporaryFolder();

  private File testFolder;

  @Before
  public void setup() throws IOException {
    testFolder = folder.newFolder("testFolder");
  }

  @Test
  public void testFailsIfDirectoryExistsButNotWritable() throws IOException {
    assumeTrue(testFolder.setWritable(false));
    try {
      try {
        final DefaultLocalPersistenceService service = new DefaultLocalPersistenceService(new DefaultPersistenceConfiguration(testFolder));
        service.start(null);
        fail("Expected IllegalArgumentException");
      } catch(IllegalArgumentException e) {
        assertThat(e.getMessage(), equalTo("Location isn't writable: " + testFolder.getAbsolutePath()));
      }
    } finally {
      testFolder.setWritable(true);
    }
  }

  @Test
  public void testFailsIfFileExistsButIsNotDirectory() throws IOException {
    File f = folder.newFile("testFailsIfFileExistsButIsNotDirectory");
    try {
      final DefaultLocalPersistenceService service = new DefaultLocalPersistenceService(new DefaultPersistenceConfiguration(f));
      service.start(null);
      fail("Expected IllegalArgumentException");
    } catch(IllegalArgumentException e) {
      assertThat(e.getMessage(), equalTo("Location is not a directory: " + f.getAbsolutePath()));
    }
  }

  @Test
  public void testFailsIfDirectoryDoesNotExistsAndIsNotCreated() throws IOException {
    assumeTrue(testFolder.setWritable(false));
    try {
      File f = new File(testFolder, "notallowed");
      try {
        final DefaultLocalPersistenceService service = new DefaultLocalPersistenceService(new DefaultPersistenceConfiguration(f));
        service.start(null);
        fail("Expected IllegalArgumentException");
      } catch(IllegalArgumentException e) {
        assertThat(e.getMessage(), equalTo("Directory couldn't be created: " + f.getAbsolutePath()));
      }
    } finally {
      testFolder.setWritable(true);
    }
  }

  @Test
  public void testLocksDirectoryAndUnlocks() throws IOException {
    final DefaultLocalPersistenceService service = new DefaultLocalPersistenceService(new DefaultPersistenceConfiguration(testFolder));
    service.start(null);
    assertThat(service.getLockFile().exists(), is(true));
    service.stop();
    assertThat(service.getLockFile().exists(), is(false));
  }

  @Test
  public void testExclusiveLock() throws IOException {
    DefaultLocalPersistenceService service1 = new DefaultLocalPersistenceService(new DefaultPersistenceConfiguration(testFolder));
    DefaultLocalPersistenceService service2 = new DefaultLocalPersistenceService(new DefaultPersistenceConfiguration(testFolder));
    service1.start(null);

    // We should not be able to lock the same directory twice
    // And we should receive a meaningful exception about it
    expectedException.expectMessage("Couldn't lock rootDir: " + testFolder.getAbsolutePath());
    service2.start(null);
  }

  @Test
  public void testCantDestroyAllIfServiceNotStarted() {
    DefaultLocalPersistenceService service = new DefaultLocalPersistenceService(new DefaultPersistenceConfiguration(testFolder));
    expectedException.expect(IllegalStateException.class);
    expectedException.expectMessage("Service must be started");
    service.destroyAll();
  }

  @Test
  public void testDestroyWhenStarted() throws CachePersistenceException {
    DefaultLocalPersistenceService service = new DefaultLocalPersistenceService(new DefaultPersistenceConfiguration(testFolder));
    service.start(null);

    CacheConfiguration<Object, Object> cacheConfiguration = CacheConfigurationBuilder
      .newCacheConfigurationBuilder(Object.class, Object.class,
        ResourcePoolsBuilder.newResourcePoolsBuilder().disk(10, MemoryUnit.MB))
      .build();
    PersistableResourceService.PersistenceSpaceIdentifier<LocalPersistenceService> id =
      service.getPersistenceSpaceIdentifier("test", cacheConfiguration);

    service = Mockito.spy(service);
    service.destroy("test");

    // Make sure we haven't tried to start the service
    Mockito.verify(service, never()).internalStart();

    // Make sure we are still started
    assertThat(service.isStarted(), is(true));

    // Make sure the cache was deleted
    expectedException.expect(CachePersistenceException.class);
    service.getStateRepositoryWithin(id, "test");
  }

  @Test
  public void testDestroyWhenStopped() throws CachePersistenceException {
    DefaultLocalPersistenceService service = new DefaultLocalPersistenceService(new DefaultPersistenceConfiguration(testFolder));
    service.start(null);

    CacheConfiguration<Object, Object> cacheConfiguration = CacheConfigurationBuilder
      .newCacheConfigurationBuilder(Object.class, Object.class,
        ResourcePoolsBuilder.newResourcePoolsBuilder().disk(10, MemoryUnit.MB))
      .build();
    PersistableResourceService.PersistenceSpaceIdentifier<LocalPersistenceService> id =
      service.getPersistenceSpaceIdentifier("test", cacheConfiguration);

    service.stop();

    service = Mockito.spy(service);
    service.destroy("test");

    // Make sure it was started
    Mockito.verify(service).internalStart();

    // Make sure the service is still stopped
    assertThat(service.isStarted(), is(false));

    // Make sure the cache was deleted
    expectedException.expect(CachePersistenceException.class);
    service.getStateRepositoryWithin(id, "test");
  }

  @Test
  public void testIsStarted() {
    DefaultLocalPersistenceService service = new DefaultLocalPersistenceService(new DefaultPersistenceConfiguration(testFolder));
    assertThat(service.isStarted(), is(false));
    service.start(null);
    assertThat(service.isStarted(), is(true));
    service.stop();
    assertThat(service.isStarted(), is(false));
    service.startForMaintenance(null);
    assertThat(service.isStarted(), is(true));
    service.stop();
    assertThat(service.isStarted(), is(false));
  }
}
