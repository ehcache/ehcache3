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

import org.ehcache.impl.config.persistence.DefaultPersistenceConfiguration;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

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
}
