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

package org.ehcache.config.builders;

import org.ehcache.impl.config.persistence.CacheManagerPersistenceConfiguration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;

import static org.ehcache.config.builders.CacheManagerBuilder.newCacheManagerBuilder;
import static org.junit.Assert.assertTrue;

/**
 * @author Alex Snaps
 */
public class PersistentCacheManagerTest {

  @Rule
  public final TemporaryFolder folder = new TemporaryFolder();

  @Test
  public void testInitializesLocalPersistenceService() throws IOException {
    final File rootDirectory = folder.newFolder("testInitializesLocalPersistenceService");
    assertTrue(rootDirectory.delete());
    newCacheManagerBuilder().with(new CacheManagerPersistenceConfiguration(rootDirectory)).build(true);
    assertTrue(rootDirectory.isDirectory());
  }
}
