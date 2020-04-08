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

package org.ehcache.impl.internal.util;

import org.junit.Rule;
import org.junit.Test;
import org.terracotta.org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;

import static org.ehcache.impl.internal.util.FileExistenceMatchers.containsCacheDirectory;
import static org.ehcache.impl.internal.util.FileExistenceMatchers.isLocked;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertThat;

/**
 * @author Henri Tremblay
 */
public class FileExistenceMatchersTest {

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  @Test
  public void directoryIsLocked() throws Exception {
    File dir = folder.newFolder();

    assertThat(dir, not(isLocked()));
  }

  @Test
  public void directoryIsNotLocked() throws Exception {
    File dir = folder.newFolder();
    File lock = new File(dir, ".lock");
    lock.createNewFile();

    assertThat(dir, isLocked());
  }

  @Test
  public void containsCacheDirectory_noFileDir() throws IOException {
    File dir = folder.newFolder();

    assertThat(dir, not(containsCacheDirectory("test123")));
  }

  @Test
  public void containsCacheDirectory_noCacheDir() throws IOException {
    File dir = folder.newFolder();
    File file = new File(dir, "file");
    file.mkdir();

    assertThat(dir, not(containsCacheDirectory("test123")));
  }

  @Test
  public void containsCacheDirectory_moreThanOneCacheDir() throws IOException {
    File dir = folder.newFolder();
    File file = new File(dir, "file");
    file.mkdir();
    new File(file, "test123_aaa").mkdir();
    new File(file, "test123_bbb").mkdir();

    assertThat(dir, not(containsCacheDirectory("test123")));
  }

  @Test
  public void containsCacheDirectory_existing() throws IOException {
    File dir = folder.newFolder();
    new File(dir, "file/test123_aaa").mkdirs();

    assertThat(dir, containsCacheDirectory("test123"));
  }

  @Test
  public void containsCacheDirectory_withSafeSpaceExisting() throws IOException {
    File dir = folder.newFolder();
    new File(dir, "safespace/test123_aaa").mkdirs();

    assertThat(dir, containsCacheDirectory("safespace", "test123"));
  }
}
