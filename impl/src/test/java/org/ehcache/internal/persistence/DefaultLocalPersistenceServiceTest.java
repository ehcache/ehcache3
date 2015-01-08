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
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DefaultLocalPersistenceServiceTest {

  public static final String PATH = "foo://bar";

  @Test
  public void testFailsIfDirectoryExistsButNotWritable() {
    File f = mock(File.class);
    when(f.getAbsolutePath()).thenReturn(PATH);
    when(f.exists()).thenReturn(true);
    when(f.isDirectory()).thenReturn(true);
    when(f.canWrite()).thenReturn(false);
    final DefaultLocalPersistenceService service = new DefaultLocalPersistenceService(new PersistenceConfiguration(f));
    try {
      service.start(null);
    } catch(IllegalArgumentException e) {
      assertThat(e.getMessage(), equalTo("Location isn't writable: " + PATH));
    }
  }

  @Test
  public void testFailsIfFileExistsButIsNotDirectory() {
    File f = mock(File.class);
    when(f.getAbsolutePath()).thenReturn(PATH);
    when(f.exists()).thenReturn(true);
    when(f.isDirectory()).thenReturn(false);
    when(f.canWrite()).thenReturn(false);
    final DefaultLocalPersistenceService service = new DefaultLocalPersistenceService(new PersistenceConfiguration(f));
    try {
      service.start(null);
    } catch(IllegalArgumentException e) {
      assertThat(e.getMessage(), equalTo("Location is not a directory: " + PATH));
    }
  }

  @Test
  public void testFailsIfDirectoryDoesNotExistsAndIsNotCreated() {
    File f = mock(File.class);
    when(f.getAbsolutePath()).thenReturn(PATH);
    when(f.exists()).thenReturn(false);
    when(f.mkdirs()).thenReturn(false);
    final DefaultLocalPersistenceService service = new DefaultLocalPersistenceService(new PersistenceConfiguration(f));
    try {
      service.start(null);
    } catch(IllegalArgumentException e) {
      assertThat(e.getMessage(), equalTo("Directory couldn't be created: " + PATH));
    }
  }

  @Test
  public void testLocksDirectoryAndUnlocks() throws IOException {
    final File f = File.createTempFile(this.getClass().getSimpleName(), ".tmp");

    if(!(f.delete())) {
      throw new IOException("Could not delete temp file: " + f.getAbsolutePath());
    }

    if(!(f.mkdir())) {
      throw new IOException("Could not create temp directory: " + f.getAbsolutePath());
    }
    f.deleteOnExit();

    final DefaultLocalPersistenceService service = new DefaultLocalPersistenceService(new PersistenceConfiguration(f));
    service.start(null);
    assertThat(service.getLockFile().exists(), is(true));
    service.stop();
    assertThat(service.getLockFile().exists(), is(false));
  }
}