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

package org.ehcache.internal.persistence;

import org.ehcache.config.persistence.PersistenceConfiguration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;

public class DefaultLocalPersistenceServiceTest {

  @Rule
  public final TemporaryFolder folder = new TemporaryFolder();

  @Test
  public void testFailsIfDirectoryExistsButNotWritable() throws IOException {
    File f = folder.newFolder("testFailsIfDirectoryExistsButNotWritable");
    f.setWritable(false);
    try {
      final DefaultLocalPersistenceService service = new DefaultLocalPersistenceService(new PersistenceConfiguration(f));
      try {
        service.start(null, null);
      } catch(IllegalArgumentException e) {
        assertThat(e.getMessage(), equalTo("Location isn't writable: " + f.getAbsolutePath()));
      }
    } finally {
      f.setWritable(true);
    }
  }

  @Test
  public void testFailsIfFileExistsButIsNotDirectory() throws IOException {
    File f = folder.newFile("testFailsIfFileExistsButIsNotDirectory");
    final DefaultLocalPersistenceService service = new DefaultLocalPersistenceService(new PersistenceConfiguration(f));
    try {
      service.start(null, null);
    } catch(IllegalArgumentException e) {
      assertThat(e.getMessage(), equalTo("Location is not a directory: " + f.getAbsolutePath()));
    }
  }

  @Test
  public void testFailsIfDirectoryDoesNotExistsAndIsNotCreated() throws IOException {
    File fdr = folder.newFolder("testFailsIfDirectoryDoesNotExistsAndIsNotCreated");
    fdr.setWritable(false);
    try {
      File f = new File(fdr, "notallowed");
      final DefaultLocalPersistenceService service = new DefaultLocalPersistenceService(new PersistenceConfiguration(f));
      try {
        service.start(null, null);
      } catch(IllegalArgumentException e) {
        assertThat(e.getMessage(), equalTo("Directory couldn't be created: " + f.getAbsolutePath()));
      }
    } finally {
      fdr.setWritable(true);
    }
  }

  @Test
  public void testLocksDirectoryAndUnlocks() throws IOException {
    final File f = folder.newFolder("testLocksDirectoryAndUnlocks");
    final DefaultLocalPersistenceService service = new DefaultLocalPersistenceService(new PersistenceConfiguration(f));
    service.start(null, null);
    assertThat(service.getLockFile().exists(), is(true));
    service.stop();
    assertThat(service.getLockFile().exists(), is(false));
  }
}