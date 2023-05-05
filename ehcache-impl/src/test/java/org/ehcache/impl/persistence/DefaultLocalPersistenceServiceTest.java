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
import org.ehcache.core.spi.service.LocalPersistenceService;
import org.ehcache.impl.config.persistence.DefaultPersistenceConfiguration;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;

import static org.ehcache.impl.internal.util.FileExistenceMatchers.containsCacheDirectory;
import static org.ehcache.impl.internal.util.FileExistenceMatchers.isLocked;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasProperty;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

public class DefaultLocalPersistenceServiceTest {

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
  public void testPhysicalDestroy() throws IOException, CachePersistenceException {
    final File f = folder.newFolder("testPhysicalDestroy");
    final DefaultLocalPersistenceService service = new DefaultLocalPersistenceService(new DefaultPersistenceConfiguration(f));
    service.start(null);

    assertThat(service.getLockFile().exists(), is(true));
    assertThat(f, isLocked());

    LocalPersistenceService.SafeSpaceIdentifier id = service.createSafeSpaceIdentifier("test", "test");
    service.createSafeSpace(id);

    assertThat(f, containsCacheDirectory("test", "test"));

    // try to destroy the physical space without the logical id
    LocalPersistenceService.SafeSpaceIdentifier newId = service.createSafeSpaceIdentifier("test", "test");
    service.destroySafeSpace(newId, false);

    assertThat(f, not(containsCacheDirectory("test", "test")));

    service.stop();

    assertThat(f, not(isLocked()));
  }

  @Test
  public void testExclusiveLock() throws IOException {
    DefaultLocalPersistenceService service1 = new DefaultLocalPersistenceService(new DefaultPersistenceConfiguration(testFolder));
    DefaultLocalPersistenceService service2 = new DefaultLocalPersistenceService(new DefaultPersistenceConfiguration(testFolder));
    service1.start(null);

    // We should not be able to lock the same directory twice
    // And we should receive a meaningful exception about it
    RuntimeException thrown = assertThrows(RuntimeException.class, () -> service2.start(null));
    assertThat(thrown, hasProperty("message", is("Persistence directory already locked by this process: " + testFolder.getAbsolutePath())));
  }

  @Test
  public void testServiceShutdownWithEmptyDirectory() throws IOException {
    File f = folder.newFolder("testServiceShutdownWithEmptyDirectory");
    final DefaultLocalPersistenceService service = new DefaultLocalPersistenceService(new DefaultPersistenceConfiguration(f));
    service.start(null);
    assertTrue(service.isClean());
    service.stop();
    service.start(null);
    assertTrue(service.isClean());
    service.stop();
  }

  @Test
  public void testServiceShutdownWithNonEmptyDirectory() throws IOException {
    File f = folder.newFolder("testServiceShutdownWithNonEmptyDirectory");
    final DefaultLocalPersistenceService service = new DefaultLocalPersistenceService(new DefaultPersistenceConfiguration(f));
    new File(f, "dummy.txt").createNewFile();
    new File(f, ".clean").createNewFile();
    service.start(null);
    assertTrue(service.isClean());
    service.stop();
  }

  @Test
  public void testServiceShutdownUnexpectedly() throws IOException {
    // Service shutdown unexpectedly means directory exists with some data but without .clean file.
    File f = folder.newFolder("testServiceShutdownUnexpectedly");
    final DefaultLocalPersistenceService service = new DefaultLocalPersistenceService(new DefaultPersistenceConfiguration(f));
    new File(f, "dummy.txt").createNewFile();
    service.start(null);
    assertFalse(service.isClean());
    service.stop();
  }

  @Test
  public void testServiceShutdownStatusIfServiceIsNotRunning() throws IOException {
    File f = folder.newFolder("testServiceShutdownStatusIfServiceIsNotRunning");
    final DefaultLocalPersistenceService service = new DefaultLocalPersistenceService(new DefaultPersistenceConfiguration(f));
    try {
      service.isClean();
      fail("Expected IllegalStateException");
    } catch(IllegalStateException e) {
      assertThat(e.getMessage(), equalTo("Service is not running"));
    }
  }
}
