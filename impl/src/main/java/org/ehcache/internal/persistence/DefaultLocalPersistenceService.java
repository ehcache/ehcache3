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

import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.persistence.PersistenceConfiguration;
import org.ehcache.spi.ServiceProvider;
import org.ehcache.spi.service.LocalPersistenceService;
import org.ehcache.spi.service.ServiceConfiguration;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileLock;

/**
 * @author Alex Snaps
 */
public class DefaultLocalPersistenceService implements LocalPersistenceService {

  private final File rootDirectory;
  private FileLock lock;
  private File lockFile;
  private RandomAccessFile rw;

  public DefaultLocalPersistenceService(final PersistenceConfiguration persistenceConfiguration) {
    rootDirectory = persistenceConfiguration.getRootDirectory();
  }

  @Override
  public synchronized void start(final ServiceConfiguration<?> config, final ServiceProvider serviceProvider) {
    createLocationIfRequiredAndVerify(rootDirectory);
    try {
      lockFile = new File(rootDirectory + File.separator + ".lock");
      rw = new RandomAccessFile(lockFile, "rw");
      lock = rw.getChannel().lock();
    } catch (IOException e) {
      throw new RuntimeException("Couldn't lock rootDir: " + rootDirectory.getAbsolutePath(), e);
    }
  }

  @Override
  public synchronized void stop() {
    try {
      lock.release();
      // Closing RandomAccessFile so that files gets deleted on windows and
      // org.ehcache.internal.persistence.DefaultLocalPersistenceServiceTest.testLocksDirectoryAndUnlocks()
      // passes on windows
      rw.close();
      if (!lockFile.delete()) {
        // todo log something?;
      }
    } catch (IOException e) {
      throw new RuntimeException("Couldn't unlock rootDir: " + rootDirectory.getAbsolutePath(), e);
    }
  }

  static void createLocationIfRequiredAndVerify(final File rootDirectory) {
    if(!rootDirectory.exists()) {
      if(!rootDirectory.mkdirs()) {
        throw new IllegalArgumentException("Directory couldn't be created: " + rootDirectory.getAbsolutePath());
      }
    } else if(!rootDirectory.isDirectory()) {
      throw new IllegalArgumentException("Location is not a directory: " + rootDirectory.getAbsolutePath());
    }

    if(!rootDirectory.canWrite()) {
      throw new IllegalArgumentException("Location isn't writable: " + rootDirectory.getAbsolutePath());
    }
  }

  @Override
  public Object persistenceContext(final String cacheAlias, final CacheConfiguration<?, ?> cacheConfiguration) {
    throw new UnsupportedOperationException("Implement me!");
  }

  File getLockFile() {
    return lockFile;
  }
}
